package utils

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"clickhouse-ingestion/models"
)

// DataTransferService handles data transfer between ClickHouse and CSV files
type DataTransferService struct {
	clickHouseClient *ClickHouseClient
	csvHandler       *CSVHandler
	progressChan     chan models.ProgressUpdate
	cancelFunc       context.CancelFunc
	mu               sync.Mutex
	totalRecords     int64
	processedRecords int64
}

// NewDataTransferService creates a new data transfer service
func NewDataTransferService() *DataTransferService {
	return &DataTransferService{
		progressChan: make(chan models.ProgressUpdate, 10),
	}
}

// SetupClickHouseClient sets up the ClickHouse client
func (s *DataTransferService) SetupClickHouseClient(client *ClickHouseClient) {
	s.clickHouseClient = client
}

// SetupCSVHandler sets up the CSV handler
func (s *DataTransferService) SetupCSVHandler(handler *CSVHandler) {
	s.csvHandler = handler
}

// GetProgressChan returns the progress channel
func (s *DataTransferService) GetProgressChan() <-chan models.ProgressUpdate {
	return s.progressChan
}

// Close cancels any ongoing operations and closes resources
func (s *DataTransferService) Close() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	close(s.progressChan)
}

// updateProgress updates the progress of the data transfer
func (s *DataTransferService) updateProgress(processed int64, total int64, status string) {
	s.mu.Lock()
	s.processedRecords = processed
	s.totalRecords = total
	s.mu.Unlock()

	percentage := float64(0)
	if total > 0 {
		percentage = float64(processed) / float64(total) * 100
	}

	select {
	case s.progressChan <- models.ProgressUpdate{
		ProcessedRecords: processed,
		TotalRecords:     total,
		Percentage:       percentage,
		Status:           status,
	}:
	default:
		// Channel buffer is full, skip this update
	}
}

// ClickHouseToCSV transfers data from ClickHouse to a CSV file
func (s *DataTransferService) ClickHouseToCSV(ctx context.Context, request *models.DataTransferRequest) (int64, error) {
	if s.clickHouseClient == nil {
		return 0, errors.New("ClickHouse client not set up")
	}
	if s.csvHandler == nil {
		return 0, errors.New("CSV handler not set up")
	}

	// Open CSV file for writing
	if err := s.csvHandler.OpenForWriting(); err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer s.csvHandler.Close()

	// Create cancellable context
	ctx, s.cancelFunc = context.WithCancel(ctx)
	defer func() { s.cancelFunc = nil }()

	// Get column count from ClickHouse
	totalRows, err := s.getClickHouseRowCount(ctx, request.SourceTable, "")
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	s.updateProgress(0, totalRows, "Starting export from ClickHouse")

	// Write headers to CSV
	if s.csvHandler.HasHeader {
		if err := s.csvHandler.WriteHeaders(request.SelectedColumns); err != nil {
			return 0, fmt.Errorf("failed to write headers: %w", err)
		}
	}

	// Prepare batch size
	batchSize := request.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	// Create the query with selected columns
	columnsStr := ""
	for i, col := range request.SelectedColumns {
		if i > 0 {
			columnsStr += ", "
		}
		columnsStr += fmt.Sprintf("`%s`", col)
	}

	offset := 0
	processedRows := int64(0)

	for {
		select {
		case <-ctx.Done():
			return processedRows, ctx.Err()
		default:
			// Query for a batch of data
			query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT %d OFFSET %d",
				columnsStr, s.clickHouseClient.Database, request.SourceTable, batchSize, offset)

			rows, err := s.clickHouseClient.Conn.Query(ctx, query)
			if err != nil {
				return processedRows, fmt.Errorf("failed to query data: %w", err)
			}

			rowsInBatch := 0
			for rows.Next() {
				// Create a slice to hold the column values
				values := make([]interface{}, len(request.SelectedColumns))
				valuePointers := make([]interface{}, len(request.SelectedColumns))
				
				// Create pointers to each element in the values slice
				for i := range values {
					valuePointers[i] = &values[i]
				}
				
				if err := rows.Scan(valuePointers...); err != nil {
					rows.Close()
					return processedRows, fmt.Errorf("failed to scan row: %w", err)
				}
				
				// Convert values to strings for CSV
				strValues := make([]string, len(values))
				for i, v := range values {
					strValues[i] = fmt.Sprintf("%v", v)
				}
				
				if err := s.csvHandler.WriteRow(strValues); err != nil {
					rows.Close()
					return processedRows, fmt.Errorf("failed to write row to CSV: %w", err)
				}
				
				rowsInBatch++
			}
			rows.Close()

			// Flush after each batch
			if err := s.csvHandler.Flush(); err != nil {
				return processedRows, fmt.Errorf("failed to flush CSV: %w", err)
			}

			processedRows += int64(rowsInBatch)
			offset += rowsInBatch
			
			s.updateProgress(processedRows, totalRows, "Exporting data...")

			// If we got fewer rows than batch size, we're done
			if rowsInBatch < batchSize {
				break
			}
		}
	}

	s.updateProgress(processedRows, totalRows, "Export completed")
	return processedRows, nil
}

// CSVToClickHouse transfers data from a CSV file to ClickHouse
func (s *DataTransferService) CSVToClickHouse(ctx context.Context, request *models.DataTransferRequest, columns []models.Column) (int64, error) {
	if s.clickHouseClient == nil {
		return 0, errors.New("ClickHouse client not set up")
	}
	if s.csvHandler == nil {
		return 0, errors.New("CSV handler not set up")
	}

	// Open CSV file for reading
	if err := s.csvHandler.OpenForReading(); err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer s.csvHandler.Close()

	// Create cancellable context
	ctx, s.cancelFunc = context.WithCancel(ctx)
	defer func() { s.cancelFunc = nil }()

	// Get total row count from CSV
	totalRows, err := s.csvHandler.GetRowCount()
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	s.updateProgress(0, totalRows, "Starting import to ClickHouse")

	// Reset reader to beginning of file
	if err := s.csvHandler.ResetReader(); err != nil {
		return 0, fmt.Errorf("failed to reset reader: %w", err)
	}

	// Prepare batch size
	batchSize := request.BatchSize
	if batchSize <= 0 {
		batchSize = 10000 // Default batch size
	}

	// Filter columns to only selected ones and create a map for column index lookup
	selectedColumns := make([]models.Column, 0, len(request.SelectedColumns))
	columnIndexMap := make(map[string]int)

	// Get headers
	var headers []string
	if s.csvHandler.HasHeader {
		// Reset to beginning to read headers
		filePos := 0
		s.csvHandler.File.Seek(int64(filePos), 0)
		s.csvHandler.FileReader = csv.NewReader(s.csvHandler.File)
		s.csvHandler.FileReader.Comma = s.csvHandler.Delimiter
		s.csvHandler.FileReader.LazyQuotes = true

		var err error
		headers, err = s.csvHandler.GetHeaders()
		if err != nil {
			return 0, fmt.Errorf("failed to read headers: %w", err)
		}

		// Create column index map
		for i, header := range headers {
			columnIndexMap[header] = i
		}
	} else {
		// For files without headers, use column_1, column_2, etc.
		for i, col := range columns {
			columnIndexMap[col.Name] = i
		}
	}

	// Filter columns to only include selected ones
	for _, colName := range request.SelectedColumns {
		for _, col := range columns {
			if col.Name == colName {
				selectedColumns = append(selectedColumns, col)
				break
			}
		}
	}

	if len(selectedColumns) == 0 {
		return 0, errors.New("no columns selected")
	}

	// Create table if needed
	if request.DestTable != "" {
		// Check if table exists
		tables, err := s.clickHouseClient.GetTables()
		if err != nil {
			return 0, fmt.Errorf("failed to get tables: %w", err)
		}

		tableExists := false
		for _, t := range tables {
			if t == request.DestTable {
				tableExists = true
				break
			}
		}

		// Create table if it doesn't exist
		if !tableExists {
			err = s.clickHouseClient.CreateTable(request.DestTable, selectedColumns)
			if err != nil {
				return 0, fmt.Errorf("failed to create table: %w", err)
			}
			s.updateProgress(0, totalRows, "Created table in ClickHouse")
		}
	} else {
		return 0, errors.New("destination table name is required")
	}

	// Prepare the insert statement
	columnInsertList := ""
	for i, col := range selectedColumns {
		if i > 0 {
			columnInsertList += ", "
		}
		columnInsertList += fmt.Sprintf("`%s`", col.Name)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES",
		s.clickHouseClient.Database, request.DestTable, columnInsertList)

	// Process data in batches
	batch := make([][]interface{}, 0, batchSize)
	processedRows := int64(0)

	for {
		select {
		case <-ctx.Done():
			return processedRows, ctx.Err()
		default:
			record, err := s.csvHandler.FileReader.Read()
			if err == io.EOF {
				// End of file, process any remaining batch
				if len(batch) > 0 {
					if err := s.insertBatch(ctx, insertQuery, batch, selectedColumns); err != nil {
						return processedRows, fmt.Errorf("failed to insert final batch: %w", err)
					}
					processedRows += int64(len(batch))
					s.updateProgress(processedRows, totalRows, "Importing data...")
				}
				s.updateProgress(processedRows, totalRows, "Import completed")
				return processedRows, nil
			}
			if err != nil {
				return processedRows, fmt.Errorf("failed to read record: %w", err)
			}

			// Skip empty rows
			if len(record) == 0 {
				continue
			}

			// Extract values for selected columns
			row := make([]interface{}, len(selectedColumns))
			for i, col := range selectedColumns {
				var value interface{}
				colIndex, ok := columnIndexMap[col.Name]
				
				if ok && colIndex < len(record) {
					// Convert the string value to the appropriate type
					strValue := strings.TrimSpace(record[colIndex])
					
					if strValue == "" {
						// Handle NULL values
						if col.IsNullable {
							value = nil
						} else {
							// Use a default value for non-nullable columns
							switch col.Type {
							case "Int64":
								value = int64(0)
							case "Float64":
								value = float64(0)
							default:
								value = ""
							}
						}
					} else {
						// Convert to the appropriate type
						switch col.Type {
						case "Int64":
							if intVal, err := strconv.ParseInt(strValue, 10, 64); err == nil {
								value = intVal
							} else {
								value = int64(0)
							}
						case "Float64":
							if floatVal, err := strconv.ParseFloat(strValue, 64); err == nil {
								value = floatVal
							} else {
								value = float64(0)
							}
						default:
							value = strValue
						}
					}
				} else {
					// Column not found in record, use default value
					if col.IsNullable {
						value = nil
					} else {
						switch col.Type {
						case "Int64":
							value = int64(0)
						case "Float64":
							value = float64(0)
						default:
							value = ""
						}
					}
				}
				
				row[i] = value
			}

			batch = append(batch, row)

			// If batch is full, insert it
			if len(batch) >= batchSize {
				if err := s.insertBatch(ctx, insertQuery, batch, selectedColumns); err != nil {
					return processedRows, fmt.Errorf("failed to insert batch: %w", err)
				}
				processedRows += int64(len(batch))
				s.updateProgress(processedRows, totalRows, "Importing data...")
				batch = make([][]interface{}, 0, batchSize)
			}
		}
	}
}

// insertBatch inserts a batch of rows into ClickHouse
func (s *DataTransferService) insertBatch(ctx context.Context, baseQuery string, batch [][]interface{}, columns []models.Column) error {
	if len(batch) == 0 {
		return nil
	}

	// Batch insert using prepared statement
	stmt, err := s.clickHouseClient.Conn.PrepareBatch(ctx, baseQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, row := range batch {
		if err := stmt.Append(row...); err != nil {
			return fmt.Errorf("failed to append row to batch: %w", err)
		}
	}

	if err := stmt.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// getClickHouseRowCount gets the row count of a table with optional WHERE clause
func (s *DataTransferService) getClickHouseRowCount(ctx context.Context, tableName, whereClause string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", s.clickHouseClient.Database, tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	row := s.clickHouseClient.Conn.QueryRow(ctx, query)
	
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	return count, nil
} 