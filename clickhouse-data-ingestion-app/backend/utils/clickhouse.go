package utils

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"clickhouse-ingestion/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseClient is a wrapper around the ClickHouse driver
type ClickHouseClient struct {
	Conn       driver.Conn
	Config     *models.ClickHouseConnectionRequest
	Database   string
	ConnString string
}

// NewClickHouseClient creates a new ClickHouse client
func NewClickHouseClient(config *models.ClickHouseConnectionRequest) (*ClickHouseClient, error) {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}

	if config.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: config.SkipVerify,
		}
	}

	// Add JWT token if using JWT authentication
	if config.UseJWT && config.JWTToken != "" {
		opts.Auth.Password = ""
		opts.Auth.JWTToken = config.JWTToken
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test the connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &ClickHouseClient{
		Conn:     conn,
		Config:   config,
		Database: config.Database,
	}, nil
}

// GetTables returns all tables in the database
func (c *ClickHouseClient) GetTables() ([]string, error) {
	ctx := context.Background()
	rows, err := c.Conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", c.Database))
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table row: %w", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// GetTableSchema returns the schema of a table
func (c *ClickHouseClient) GetTableSchema(tableName string) ([]models.Column, error) {
	ctx := context.Background()
	
	// Query to get column information
	query := fmt.Sprintf(`
		SELECT 
			name,
			type,
			is_nullable
		FROM system.columns
		WHERE database = '%s' AND table = '%s'
	`, c.Database, tableName)
	
	rows, err := c.Conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	var columns []models.Column
	for rows.Next() {
		var col models.Column
		var nullable string
		
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}
		
		col.IsNullable = (nullable == "1")
		col.Selected = true // Default all columns to selected
		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	return columns, nil
}

// PreviewTableData returns a preview of the data in a table
func (c *ClickHouseClient) PreviewTableData(tableName string, columns []string, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 {
		limit = 100 // Default to 100 rows
	}

	columnList := "*"
	if len(columns) > 0 {
		columnList = ""
		for i, col := range columns {
			if i > 0 {
				columnList += ", "
			}
			columnList += fmt.Sprintf("`%s`", col)
		}
	}

	ctx := context.Background()
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT %d", columnList, c.Database, tableName, limit)
	
	rows, err := c.Conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to preview data: %w", err)
	}
	defer rows.Close()

	// Get column names from the result set
	columnNames := rows.ColumnNames()
	if len(columnNames) == 0 {
		return nil, errors.New("no columns in result set")
	}

	// Convert rows to map for JSON response
	var result []map[string]interface{}
	for rows.Next() {
		// Create a slice to hold the column values
		values := make([]interface{}, len(columnNames))
		valuePointers := make([]interface{}, len(columnNames))
		
		// Create pointers to each element in the values slice
		for i := range values {
			valuePointers[i] = &values[i]
		}
		
		if err := rows.Scan(valuePointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			rowMap[colName] = values[i]
		}
		
		result = append(result, rowMap)
	}

	return result, nil
}

// CreateTable creates a new table in ClickHouse
func (c *ClickHouseClient) CreateTable(tableName string, columns []models.Column) error {
	if len(columns) == 0 {
		return errors.New("no columns provided for table creation")
	}

	// Build the CREATE TABLE statement
	queryBuilder := fmt.Sprintf("CREATE TABLE %s.%s (", c.Database, tableName)
	
	for i, col := range columns {
		if i > 0 {
			queryBuilder += ", "
		}
		
		// Add NULL constraint if needed
		if col.IsNullable {
			queryBuilder += fmt.Sprintf("`%s` Nullable(%s)", col.Name, col.Type)
		} else {
			queryBuilder += fmt.Sprintf("`%s` %s", col.Name, col.Type)
		}
	}
	
	queryBuilder += ") ENGINE = MergeTree() ORDER BY tuple()"
	
	ctx := context.Background()
	err := c.Conn.Exec(ctx, queryBuilder)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	return nil
}

// Close closes the connection to ClickHouse
func (c *ClickHouseClient) Close() error {
	return c.Conn.Close()
} 