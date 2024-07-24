package data

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type DatabaseFrame struct {
	name        string
	ColumnNames []string
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	vars        []interface{}
}

func NewDatabaseFrame(name string, rows *sql.Rows) (DatabaseFrame, error) {
	databaseFrame := DatabaseFrame{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return DatabaseFrame{}, err
	}
	databaseFrame.columnTypes = columnTypes
	databaseFrame.name = name
	vars := make([]interface{}, len(columnTypes))
	columnNames := make([]string, len(columnTypes))
	for i := range columnTypes {
		value := reflect.Zero(columnTypes[i].ScanType()).Interface()
		vars[i] = &value
		columnNames[i] = columnTypes[i].Name()
	}
	databaseFrame.ColumnNames = columnNames
	databaseFrame.vars = vars
	databaseFrame.rows = rows
	return databaseFrame, nil
}

func (f DatabaseFrame) Next() ([]interface{}, bool, error) {
	values := make([]interface{}, len(f.columnTypes))
	for f.rows.Next() {
		if err := f.rows.Scan(f.vars...); err != nil {
			return nil, false, err
		}
		for i := range f.columnTypes {
			ptr := reflect.ValueOf(f.vars[i])
			values[i] = ptr.Elem().Interface()
		}
		return values, true, nil //nolint
	}
	// TODO: raise issue as this seems to always raise an error
	//err := f.rows.Err()
	f.rows.Close()
	return nil, false, nil
}

func (f DatabaseFrame) Columns() []string {
	return f.ColumnNames
}

func (f DatabaseFrame) Name() string {
	return f.name
}

type Order int

const (
	Asc  Order = 1
	Desc Order = 2
)

type OrderBy struct {
	Column string
	Order  Order
}

func (o OrderBy) String() string {
	if strings.TrimSpace(o.Column) == "" {
		return ""
	}
	switch o.Order {
	case Asc:
		return fmt.Sprintf(" ORDER BY %s ASC", o.Column)
	case Desc:
		return fmt.Sprintf(" ORDER BY %s DESC", o.Column)
	}
	return ""
}
