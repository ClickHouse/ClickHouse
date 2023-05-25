package data

type Field struct {
	// Name of the field
	Name string
	// A list of fields that must implement FieldType interface
	Values []interface{}
}
