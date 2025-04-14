package utils

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"clickhouse-ingestion/models"
)

// CSVHandler handles operations on CSV files
type CSVHandler struct {
	Delimiter  rune
	HasHeader  bool
	FilePath   string
	FileReader *csv.Reader
	FileWriter *csv.Writer
	File       *os.File
}

// NewCSVHandler creates a new CSV handler
func NewCSVHandler(filePath string, delimiter string, hasHeader bool) (*CSVHandler, error) {
	// Default to comma if no delimiter is provided
	delimiterRune := ','
	if delimiter != "" {
		delimiterRune = []rune(delimiter)[0]
	}

	return &CSVHandler{
		Delimiter: delimiterRune,
		HasHeader: hasHeader,
		FilePath:  filePath,
	}, nil
}

// OpenForReading opens the CSV file for reading
func (h *CSVHandler) OpenForReading() error {
	file, err := os.Open(h.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file for reading: %w", err)
	}

	h.File = file
	h.FileReader = csv.NewReader(file)
	h.FileReader.Comma = h.Delimiter
	h.FileReader.LazyQuotes = true // Be more lenient with quotes

	return nil
}

// OpenForWriting opens the CSV file for writing
func (h *CSVHandler) OpenForWriting() error {
	file, err := os.Create(h.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %w", err)
	}

	h.File = file
	h.FileWriter = csv.NewWriter(file)
	h.FileWriter.Comma = h.Delimiter

	return nil
}

// GetHeaders reads the headers from the CSV file
func (h *CSVHandler) GetHeaders() ([]string, error) {
	if h.FileReader == nil {
		return nil, errors.New("file not open for reading")
	}

	// Read the first line
	headers, err := h.FileReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	// Clean header names
	for i, header := range headers {
		headers[i] = strings.TrimSpace(header)
	}

	return headers, nil
}

// ResetReader resets the CSV reader to the beginning of the file
func (h *CSVHandler) ResetReader() error {
	if h.File == nil {
		return errors.New("file not open")
	}

	_, err := h.File.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to reset reader: %w", err)
	}

	h.FileReader = csv.NewReader(h.File)
	h.FileReader.Comma = h.Delimiter
	h.FileReader.LazyQuotes = true

	// Skip header if needed
	if h.HasHeader {
		_, err = h.FileReader.Read()
		if err != nil {
			return fmt.Errorf("failed to skip header: %w", err)
		}
	}

	return nil
}

// DetectSchema attempts to detect the schema from the CSV file
func (h *CSVHandler) DetectSchema() ([]models.Column, error) {
	if h.FileReader == nil {
		return nil, errors.New("file not open for reading")
	}

	// Read headers if file has headers
	var headers []string
	var err error

	if h.HasHeader {
		headers, err = h.GetHeaders()
		if err != nil {
			return nil, err
		}
	}

	// Read first 100 rows to determine column types
	maxRows := 100
	records := make([][]string, 0, maxRows)
	rowCount := 0

	for rowCount < maxRows {
		record, err := h.FileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}

		records = append(records, record)
		rowCount++
	}

	// If no records, return error
	if len(records) == 0 {
		return nil, errors.New("file is empty")
	}

	// Get column count from first record
	columnCount := len(records[0])

	// Generate column names if no headers
	if !h.HasHeader || len(headers) == 0 {
		headers = make([]string, columnCount)
		for i := range headers {
			headers[i] = fmt.Sprintf("column_%d", i+1)
		}
	}

	// Make sure headers count matches column count
	if len(headers) != columnCount {
		return nil, fmt.Errorf("header count (%d) does not match column count (%d)", len(headers), columnCount)
	}

	// Detect column types
	columns := make([]models.Column, columnCount)
	for i := 0; i < columnCount; i++ {
		col := models.Column{
			Name:      headers[i],
			Type:      "String", // Default type
			IsNullable: false,
			Selected:  true,
		}

		// Try to detect numeric types
		isInt := true
		isFloat := true
		isNull := false

		for _, record := range records {
			if i >= len(record) {
				continue
			}

			value := strings.TrimSpace(record[i])

			// Check for NULL values
			if value == "" {
				isNull = true
				continue
			}

			// Check if value is integer
			_, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				isInt = false
			}

			// Check if value is float
			_, err = strconv.ParseFloat(value, 64)
			if err != nil {
				isFloat = false
			}
		}

		// Set the column type based on detected values
		if isInt {
			col.Type = "Int64"
		} else if isFloat {
			col.Type = "Float64"
		}

		// Set nullability based on detection
		col.IsNullable = isNull

		columns[i] = col
	}

	// Reset the reader for future operations
	if err := h.ResetReader(); err != nil {
		return nil, err
	}

	return columns, nil
}

// PreviewData returns a preview of the CSV data
func (h *CSVHandler) PreviewData(limit int) ([]string, []map[string]interface{}, error) {
	if h.FileReader == nil {
		return nil, nil, errors.New("file not open for reading")
	}

	if limit <= 0 {
		limit = 100 // Default limit
	}

	// Reset to beginning of file
	if err := h.ResetReader(); err != nil {
		return nil, nil, err
	}

	// Read headers if file has headers
	var headers []string
	var err error

	if h.HasHeader {
		headers, err = h.GetHeaders()
		if err != nil {
			return nil, nil, err
		}

		// Reset again after reading headers
		if err := h.ResetReader(); err != nil {
			return nil, nil, err
		}
	}

	// Read data rows
	rows := make([]map[string]interface{}, 0, limit)
	rowCount := 0

	for rowCount < limit {
		record, err := h.FileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read record: %w", err)
		}

		// Skip header row if file has headers
		if h.HasHeader && rowCount == 0 {
			rowCount++
			continue
		}

		// Generate headers if not available
		if len(headers) == 0 {
			headers = make([]string, len(record))
			for i := range headers {
				headers[i] = fmt.Sprintf("column_%d", i+1)
			}
		}

		// Convert row to map
		rowMap := make(map[string]interface{})
		for i, value := range record {
			if i < len(headers) {
				rowMap[headers[i]] = value
			}
		}

		rows = append(rows, rowMap)
		rowCount++
	}

	return headers, rows, nil
}

// WriteHeaders writes headers to the CSV file
func (h *CSVHandler) WriteHeaders(headers []string) error {
	if h.FileWriter == nil {
		return errors.New("file not open for writing")
	}

	if err := h.FileWriter.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	h.FileWriter.Flush()
	return h.FileWriter.Error()
}

// WriteRow writes a row to the CSV file
func (h *CSVHandler) WriteRow(row []string) error {
	if h.FileWriter == nil {
		return errors.New("file not open for writing")
	}

	if err := h.FileWriter.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// Flush flushes the CSV writer
func (h *CSVHandler) Flush() error {
	if h.FileWriter == nil {
		return errors.New("file not open for writing")
	}

	h.FileWriter.Flush()
	return h.FileWriter.Error()
}

// Close closes the CSV file
func (h *CSVHandler) Close() error {
	if h.File == nil {
		return nil
	}

	if h.FileWriter != nil {
		h.FileWriter.Flush()
	}

	return h.File.Close()
}

// GetRowCount returns the number of rows in the CSV file
func (h *CSVHandler) GetRowCount() (int64, error) {
	if h.FileReader == nil {
		return 0, errors.New("file not open for reading")
	}

	// Reset to beginning of file
	if err := h.ResetReader(); err != nil {
		return 0, err
	}

	var count int64 = 0
	for {
		_, err := h.FileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read record: %w", err)
		}
		count++
	}

	// Subtract header row if file has headers
	if h.HasHeader && count > 0 {
		count--
	}

	return count, nil
}

// ValidateFile validates that the file exists and is accessible
func ValidateFile(filePath string) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	// Try to open the file to ensure it's accessible
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot access file: %w", err)
	}
	file.Close()

	// Check file extension
	ext := strings.ToLower(filepath.Ext(filePath))
	if ext != ".csv" && ext != ".txt" && ext != ".dat" {
		return fmt.Errorf("unsupported file extension: %s, expected .csv, .txt, or .dat", ext)
	}

	return nil
} 