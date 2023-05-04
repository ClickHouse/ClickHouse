package data

type MemoryFrame struct {
	i           *int
	ColumnNames []string
	Rows        [][]interface{}
	name        string
}

func NewMemoryFrame(name string, columns []string, rows [][]interface{}) MemoryFrame {
	i := 0
	return MemoryFrame{
		i:           &i,
		Rows:        rows,
		ColumnNames: columns,
		name:        name,
	}
}

func (f MemoryFrame) Next() ([]interface{}, bool, error) {
	if f.Rows == nil || len(f.Rows) == *(f.i) {
		return nil, false, nil
	}
	value := f.Rows[*f.i]
	*f.i++
	return value, true, nil
}

func (f MemoryFrame) Columns() []string {
	return f.ColumnNames
}

func (f MemoryFrame) Name() string {
	return f.name
}
