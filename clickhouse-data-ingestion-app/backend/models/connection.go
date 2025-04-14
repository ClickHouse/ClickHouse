package models

// ClickHouseConnectionRequest represents the request to connect to a ClickHouse database
type ClickHouseConnectionRequest struct {
	Host      string `json:"host" binding:"required"`
	Port      string `json:"port" binding:"required"`
	Database  string `json:"database" binding:"required"`
	Username  string `json:"username" binding:"required"`
	Password  string `json:"password"`
	JWTToken  string `json:"jwt_token"`
	UseJWT    bool   `json:"use_jwt"`
	Secure    bool   `json:"secure"`
	SkipVerify bool  `json:"skip_verify"`
}

// FileConnectionRequest represents the request to connect to a flat file
type FileConnectionRequest struct {
	Delimiter string `json:"delimiter" binding:"required"`
	HasHeader bool   `json:"has_header"`
}

// ConnectionResponse represents the response after establishing a connection
type ConnectionResponse struct {
	Success      bool   `json:"success"`
	Message      string `json:"message"`
	ConnectionID string `json:"connection_id,omitempty"`
	Error        string `json:"error,omitempty"`
}

// Column represents a database column or file column
type Column struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	IsNullable bool   `json:"is_nullable"`
	Selected  bool   `json:"selected"`
}

// SchemaRequest represents a request to discover schema
type SchemaRequest struct {
	ConnectionID string `json:"connection_id" binding:"required"`
	TableName    string `json:"table_name,omitempty"`
	FilePath     string `json:"file_path,omitempty"`
}

// SchemaResponse represents the response containing the discovered schema
type SchemaResponse struct {
	Success bool     `json:"success"`
	Columns []Column `json:"columns,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// DataTransferRequest represents a request to transfer data
type DataTransferRequest struct {
	ConnectionID    string   `json:"connection_id" binding:"required"`
	SourceType      string   `json:"source_type" binding:"required,oneof=clickhouse file"` 
	DestinationType string   `json:"destination_type" binding:"required,oneof=clickhouse file"`
	SourceTable     string   `json:"source_table,omitempty"`
	SourceFilePath  string   `json:"source_file_path,omitempty"`
	DestTable       string   `json:"dest_table,omitempty"`
	DestFilePath    string   `json:"dest_file_path,omitempty"`
	SelectedColumns []string `json:"selected_columns" binding:"required"`
	BatchSize       int      `json:"batch_size,omitempty"`
}

// DataTransferResponse represents the response after data transfer
type DataTransferResponse struct {
	Success      bool   `json:"success"`
	RecordCount  int64  `json:"record_count,omitempty"`
	Message      string `json:"message,omitempty"`
	Error        string `json:"error,omitempty"`
}

// DataPreviewRequest represents a request to preview data
type DataPreviewRequest struct {
	ConnectionID string   `json:"connection_id" binding:"required"`
	SourceType   string   `json:"source_type" binding:"required,oneof=clickhouse file"`
	SourceTable  string   `json:"source_table,omitempty"`
	FilePath     string   `json:"file_path,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

// DataPreviewResponse represents the response containing preview data
type DataPreviewResponse struct {
	Success bool                     `json:"success"`
	Headers []string                 `json:"headers,omitempty"`
	Data    []map[string]interface{} `json:"data,omitempty"`
	Error   string                   `json:"error,omitempty"`
}

// ProgressUpdate represents a progress update during data transfer
type ProgressUpdate struct {
	ProcessedRecords int64   `json:"processed_records"`
	TotalRecords     int64   `json:"total_records"`
	Percentage       float64 `json:"percentage"`
	Status           string  `json:"status"`
} 