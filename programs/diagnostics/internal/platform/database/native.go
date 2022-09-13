package database

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"
)

type ClickhouseNativeClient struct {
	host       string
	connection *sql.DB
}

func NewNativeClient(host string, port uint16, username string, password string) (*ClickhouseNativeClient, error) {
	// debug output ?debug=true
	connection, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s:%d/", url.QueryEscape(username), url.QueryEscape(password), host, port))
	if err != nil {
		return &ClickhouseNativeClient{}, err
	}
	if err := connection.Ping(); err != nil {
		return &ClickhouseNativeClient{}, err
	}
	return &ClickhouseNativeClient{
		host:       host,
		connection: connection,
	}, nil
}

func (c *ClickhouseNativeClient) Ping() error {
	return c.connection.Ping()
}

func (c *ClickhouseNativeClient) ReadTable(databaseName string, tableName string, excludeColumns []string, orderBy data.OrderBy, limit int64) (data.Frame, error) {
	exceptClause := ""
	if len(excludeColumns) > 0 {
		exceptClause = fmt.Sprintf("EXCEPT(%s) ", strings.Join(excludeColumns, ","))
	}
	limitClause := ""
	if limit >= 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := c.connection.Query(fmt.Sprintf("SELECT * %sFROM %s.%s%s%s", exceptClause, databaseName, tableName, orderBy.String(), limitClause))
	if err != nil {
		return data.DatabaseFrame{}, err
	}
	return data.NewDatabaseFrame(fmt.Sprintf("%s.%s", databaseName, tableName), rows)
}

func (c *ClickhouseNativeClient) ReadTableNamesForDatabase(databaseName string) ([]string, error) {
	rows, err := c.connection.Query(fmt.Sprintf("SHOW TABLES FROM %s", databaseName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tableNames []string
	var name string
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	return tableNames, nil
}

func (c *ClickhouseNativeClient) ExecuteStatement(id string, statement string) (data.Frame, error) {
	rows, err := c.connection.Query(statement)
	if err != nil {
		return data.DatabaseFrame{}, err
	}
	return data.NewDatabaseFrame(id, rows)
}

func (c *ClickhouseNativeClient) Version() (string, error) {
	frame, err := c.ExecuteStatement("version", "SELECT version() as version")
	if err != nil {
		return "", err
	}
	values, ok, err := frame.Next()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("unable to read ClickHouse version")
	}
	if len(values) != 1 {
		return "", errors.New("unable to read ClickHouse version - no rows returned")
	}
	return values[0].(string), nil
}
