package data

func NewHierarchicalFrame(name string, frame Frame, subFrames []HierarchicalFrame) HierarchicalFrame {
	return HierarchicalFrame{
		name:      name,
		DataFrame: frame,
		SubFrames: subFrames,
	}
}

type HierarchicalFrame struct {
	name      string
	DataFrame Frame
	SubFrames []HierarchicalFrame
}

func (hf HierarchicalFrame) Name() string {
	return hf.name
}

func (hf HierarchicalFrame) Columns() []string {
	return hf.DataFrame.Columns()
}

func (hf HierarchicalFrame) Next() ([]interface{}, bool, error) {
	return hf.DataFrame.Next()
}
