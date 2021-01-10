package main

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"database/sql"
	"net/http"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

type urlMap struct {
	ID int64
	orgURL string
	shorURL string
	createtime int32
}
func covertURL(orgUrl string) uint64 {
	var data []byte = []byte(orgUrl)
    return murmur3.Sum64(data)
}

func writeToDB(orgURL,shortURL string)  {
	db,err := sql.Open("mysql","root:@tcp(127.0.0.1:3306)/GOLANG_LEARN_DB?charset=utf8&parseTime=True&net_write_timeout=6000")
	if err != nil {
		fmt.Println("connect to local mysql server error.",err)
		return
	} else {
		fmt.Println("connect to local mysql success.")
	}
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)
	if err := db.Ping(); err != nil{
        fmt.Println("open database fail")
        return
    } else {
		fmt.Println("opon database success")
	}
	defer db.Close()
	fmt.Printf("do exec.org:%s,new:%s\n",orgURL,shortURL)
	result,errx := db.Exec(`INSERT INTO short_url_table(lurl,surl,gmt_create) values(?,?,?)`,orgURL,shortURL,20333)
	if errx != nil {
		fmt.Println("insert url info to DB error.",errx)
		return
	} else {
		fmt.Println("insert data  to local mysql success.")
	}
	newID, _ := result.LastInsertId()
	i, _ := result.RowsAffected()
	fmt.Printf("add data id : %d,affected rows: %d\n",newID,i)
	
	return
}

func getOrgURLFromDB(shortURL string) string{
	db,err := sql.Open("mysql","root:@tcp(127.0.0.1:3306)/GOLANG_LEARN_DB?charset=utf8&parseTime=True")
	if err != nil {
		fmt.Println("connect to local mysql server error.",err)
		return ""
	}
	defer db.Close()
	var urlinfo urlMap
	rows := db.QueryRow("select * from short_url_table where surl=?",shortURL)
	
	switch err := rows.Scan(&urlinfo.ID,&urlinfo.orgURL,&urlinfo.shorURL,&urlinfo.createtime); err {
	case sql.ErrNoRows:
	  fmt.Println("No rows were returned!")
	  return ""
	case nil:
		fmt.Println("get url: ",urlinfo)
	default:
	  panic(err)
	}
	
	return urlinfo.orgURL
}

func transto62(data uint64) string {
	charset := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var shortUrl []byte
	for {
		var result byte
		number := data % 62
		result = charset[number]
		var tmp []byte
		tmp = append(tmp,result)
		//fmt.Println(tmp)
		shortUrl = append(tmp,shortUrl...)
		//fmt.Println(shortUrl)
		data = data / 62
		if data == 0 {
			break
		}
	}

	return string(shortUrl)
}
func handler(w http.ResponseWriter,r *http.Request) {
	//fmt.Fprintf(w,"URL.Path = %q\n",r.URL.Path)
	shortPath := strings.TrimLeft(r.URL.Path, "/")
	if shortPath == "" {
		fmt.Fprintln(w, "short url error")
		return
	}

	// 路径转长链接并重定向
	longUrl := getOrgURLFromDB(shortPath)
	fmt.Printf("getOrgURLFromDB: %s\n",longUrl)
	http.Redirect(w, r, longUrl, 302)
}
func web_server(){
	http.HandleFunc("/",handler)
	log.Fatal(http.ListenAndServe("localhost:8000",nil))
}

func main() {
	//orgUrl := "https://godoc.org/github.com/spaolacci/murmur3"
	orgUrl := "http://www.baidu.com"
	newUrl_hash := covertURL(orgUrl)
	newUrl := transto62(newUrl_hash)
	fmt.Printf("orgUrl: \t%s\nconvertURLHash:\t%d\nnewURL:\t%s\n",orgUrl,newUrl_hash,newUrl)
	writeToDB(orgUrl,newUrl)
	ret := getOrgURLFromDB(newUrl)
	fmt.Printf("getOrgURLFromDB: %s\n",ret)
	web_server()
}

