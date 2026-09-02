package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

func main() {
	host := flag.String("host", "localhost", "mysql server address")
	port := flag.Uint("port", 3306, "mysql server port")
	user := flag.String("user", "", "username")
	password := flag.String("password", "", "password")
	database := flag.String("database", "", "database to authenticate against")
	flag.Parse()

	logger := log.New(os.Stderr, "", 0)
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", *user, *password, *host, *port, *database)
	db, err := sql.Open("mysql", dataSource)

	if err != nil {
		logger.Fatal(err)
	}
	defer db.Close()

	runQuery := func(query string, processRows func(*sql.Rows) error) {
		rows, err := db.Query(query)
		if err != nil {
			logger.Fatal(err)
		}

		columns, err := rows.Columns()
		fmt.Println("Columns:")
		for _, name := range columns {
			fmt.Println(name)
		}

		columnsTypes, err := rows.ColumnTypes()
		fmt.Println("Column types:")
		for _, column := range columnsTypes {
			fmt.Printf("%s %s\n", column.Name(), column.DatabaseTypeName())
		}

		fmt.Println("Result:")
		err = processRows(rows)
		if err != nil {
			logger.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			logger.Fatal(err)
		}
		err = rows.Close()
		if err != nil {
			logger.Fatal(err)
		}
	}

	processRows := func(rows *sql.Rows) error {
		var x int
		for rows.Next() {
			err := rows.Scan(&x)
			if err != nil {
				return err
			}
			fmt.Println(x)
		}
		return rows.Err()
	}
	runQuery("select number as a from system.numbers limit 2", processRows)

	processRows = func(rows *sql.Rows) error {
		var name string
		var a int
		for rows.Next() {
			err := rows.Scan(&name, &a)
			if err != nil {
				return err
			}
			fmt.Println(name, a)
		}
		return rows.Err()
	}
	runQuery("select name, 1 as a from system.tables where name == 'tables'", processRows)

	runQuery("select 'тест' as a, 1 as b", processRows)

}
