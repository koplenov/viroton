package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/ompluscator/dynamic-struct"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func init() {
	rand.Seed(time.Now().UnixNano())
}
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// e.POST("/send", save)
func send(c *gin.Context) {
	link := c.PostForm("site")
	c.Header("Content-Type", "application/json")
	var respa = getString(link)
	id := "d" +  RandStringRunes(12)
	tableName := createTable(id)
	
	formatJson(respa, tableName)
	c.String(http.StatusOK, "Id вашей итерации: " + id)
}

func createTable(tableName string) string{
	sql := "CREATE TABLE NAME (id serial PRIMARY KEY,details JSONB);"
	sql = strings.ReplaceAll(sql, "NAME", tableName)
	println(sql)
	_, err := conn.Query(context.Background(), sql)
	if err != nil {
		panic(err)
	}
	return tableName
}
func addItemToTable(tableName string, item string){
	sql := "INSERT INTO PUBLIC.TNAME (details) VALUES ($1);"
	sql = strings.ReplaceAll(sql, "TNAME", tableName)
	_, err := conn.Exec(context.Background(), sql, item)
	if err != nil {
		panic(err)
	}
}

func input() {
	router := gin.Default()
	router.Static("/static", "./static")
	// For each matched request Context will hold the route definition
	router.POST("/send", send)
	// Listen and serve on 0.0.0.0:8080
	router.Run(":8080")

	//defer conn.Close(context.Background())
}

var conn *pgxpool.Pool

func main() {
	//urlExample := "postgres://postgres:2403@localhost:5432/"
	urlExample := "postgres://superset:superset@172.18.0.3:5432/superset"
	var err error
	conn, err = pgxpool.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	input()
}

// getJSON fetches the contents of the given URL
// and decodes it as JSON into the given result,
// which should be a pointer to the expected data.
func getString(url string) []byte {
	resp, err := http.Get(url)
	if err != nil {
		return nil ///fmt.Errorf("cannot fetch URL %q: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil //fmt.Errorf("unexpected http GET status: %s", resp.Status)
	}


	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	return responseData
}

func formatJson(inputJson []byte, tableName string ) {
	var items []interface{}

	if err := json.Unmarshal(inputJson, &items); err != nil {
		panic(err)
	}

	// generate view
	viewName := tableName + "_view"
	sql := `create view VIEWNAME as select id, `
	sql = strings.ReplaceAll(sql, "VIEWNAME", viewName)

	//
	var varables = " "
	b, _ := json.Marshal(items[0])
	currentStrings := getKeys(b)
	for i, s := range currentStrings {
		//varables += "'"+s+"'" + " as " + rarir(s)
		varables += "details->>" + "'"+s+"'" + " as " + rarir(s)
		if i != len(currentStrings)-1 {
			varables += ", "
		}
	}

	//
	sql += varables + " from " + tableName + " ;"
    println(sql)
	// создаем проекцию
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}

	// fill database
	for i := range items {
		b, err := json.Marshal(items[i])
		if err != nil {
			panic(err)
		}
		//fmt.Println(string(b) + "GG")
		addItemToTable(tableName, string(b))
	}
}

func rarir(s string) string {
	return strings.ReplaceAll(s, " ", "_")
}

func getKeys(inputJson []byte) []string {
	// a map container to decode the JSON structure into
	c := make(map[string]json.RawMessage)

	// unmarschal JSON
	e := json.Unmarshal(inputJson, &c)

	// panic on error
	if e != nil {
		panic(e)
	}

	// a string slice to hold the keys
	k := make([]string, len(c))

	// iteration counter
	i := 0

	// copy c's keys into k
	for s, _ := range c {
		k[i] = s
		i++
	}

	// output result to STDOUT
	fmt.Printf("%#v\n", k)

	return k
}