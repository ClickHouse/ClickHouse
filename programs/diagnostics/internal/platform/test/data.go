package test

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/data"
	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/pkg/errors"
)

type fakeClickhouseClient struct {
	tables         map[string][]string
	QueryResponses map[string]*FakeDataFrame
}

func NewFakeClickhouseClient(tables map[string][]string) fakeClickhouseClient {
	queryResponses := make(map[string]*FakeDataFrame)
	return fakeClickhouseClient{
		tables:         tables,
		QueryResponses: queryResponses,
	}
}

func (f fakeClickhouseClient) ReadTableNamesForDatabase(databaseName string) ([]string, error) {
	if _, ok := f.tables[databaseName]; ok {
		return f.tables[databaseName], nil
	}
	return nil, fmt.Errorf("database %s does not exist", databaseName)
}

func (f fakeClickhouseClient) ReadTable(databaseName string, tableName string, excludeColumns []string, orderBy data.OrderBy, limit int64) (data.Frame, error) {

	exceptClause := ""
	if len(excludeColumns) > 0 {
		exceptClause = fmt.Sprintf("EXCEPT(%s) ", strings.Join(excludeColumns, ","))
	}
	limitClause := ""
	if limit >= 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}
	query := fmt.Sprintf("SELECT * %sFROM %s.%s%s%s", exceptClause, databaseName, tableName, orderBy.String(), limitClause)
	frame, error := f.ExecuteStatement(fmt.Sprintf("read_table_%s.%s", databaseName, tableName), query)
	if error != nil {
		return frame, error
	}
	fFrame := *(frame.(*FakeDataFrame))
	fFrame = fFrame.FilterColumns(excludeColumns)
	fFrame = fFrame.Order(orderBy)
	fFrame = fFrame.Limit(limit)
	return fFrame, nil
}

func (f fakeClickhouseClient) ExecuteStatement(id string, statement string) (data.Frame, error) {
	if frame, ok := f.QueryResponses[statement]; ok {
		return frame, nil
	}
	return FakeDataFrame{}, errors.New(fmt.Sprintf("No recorded response for %s", statement))
}

func (f fakeClickhouseClient) Version() (string, error) {
	return "21.12.3", nil
}

func (f fakeClickhouseClient) Reset() {
	for key, frame := range f.QueryResponses {
		frame.Reset()
		f.QueryResponses[key] = frame
	}
}

type FakeDataFrame struct {
	i           *int
	Rows        [][]interface{}
	ColumnNames []string
	name        string
}

func NewFakeDataFrame(name string, columns []string, rows [][]interface{}) FakeDataFrame {
	i := 0
	return FakeDataFrame{
		i:           &i,
		Rows:        rows,
		ColumnNames: columns,
		name:        name,
	}
}

func (f FakeDataFrame) Next() ([]interface{}, bool, error) {
	if len(f.Rows) == *(f.i) {
		return nil, false, nil
	}
	value := f.Rows[*f.i]
	*f.i++
	return value, true, nil
}

func (f FakeDataFrame) Columns() []string {
	return f.ColumnNames
}

func (f FakeDataFrame) Name() string {
	return f.name
}

func (f *FakeDataFrame) Reset() {
	i := 0
	f.i = &i
}

func (f FakeDataFrame) FilterColumns(excludeColumns []string) FakeDataFrame {
	// get columns we can remove
	rColumns := utils.Intersection(f.ColumnNames, excludeColumns)
	rIndexes := make([]int, len(rColumns))
	// find the indexes of the columns to remove
	for i, column := range rColumns {
		rIndexes[i] = utils.IndexOf(f.ColumnNames, column)
	}
	newRows := make([][]interface{}, len(f.Rows))
	for r, row := range f.Rows {
		newRow := row
		for i, index := range rIndexes {
			newRow = utils.Remove(newRow, index-i)
		}
		newRows[r] = newRow
	}
	f.Rows = newRows
	f.ColumnNames = utils.Distinct(f.ColumnNames, excludeColumns)
	return f
}

func (f FakeDataFrame) Limit(rowLimit int64) FakeDataFrame {
	if rowLimit >= 0 {
		if int64(len(f.Rows)) > rowLimit {
			f.Rows = f.Rows[:rowLimit]
		}
	}
	return f
}

func (f FakeDataFrame) Order(orderBy data.OrderBy) FakeDataFrame {
	if orderBy.Column == "" {
		return f
	}
	cIndex := utils.IndexOf(f.ColumnNames, orderBy.Column)
	sort.Slice(f.Rows, func(i, j int) bool {
		left := f.Rows[i][cIndex]
		right := f.Rows[j][cIndex]
		if iLeft, ok := left.(int); ok {
			if orderBy.Order == data.Asc {
				return iLeft < right.(int)
			}
			return iLeft > right.(int)
		} else {
			// we aren't a full db - revert to string order
			sLeft := left.(string)
			sRight := right.(string)
			if orderBy.Order == data.Asc {
				return sLeft < sRight
			}
			return sLeft > sRight
		}
	})
	return f
}
