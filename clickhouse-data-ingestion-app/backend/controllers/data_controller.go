package controllers

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"clickhouse-ingestion/config"
	"clickhouse-ingestion/models"
	"clickhouse-ingestion/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// DataController handles data operations like preview and transfer
type DataController struct {
	connectionController *ConnectionController
	transfers            sync.Map // Map of transfer ID to DataTransferService
	config               *config.Config
}

// NewDataController creates a new data controller
func NewDataController(connectionController *ConnectionController, config *config.Config) *DataController {
	return &DataController{
		connectionController: connectionController,
		transfers:            sync.Map{},
		config:               config,
	}
}

// PreviewData previews data from a source
func (c *DataController) PreviewData(ctx *gin.Context) {
	var request models.DataPreviewRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// Set default limit if not provided
	if request.Limit <= 0 {
		request.Limit = 100
	}

	// Handle preview based on source type
	switch request.SourceType {
	case "clickhouse":
		c.previewClickHouseData(ctx, &request)
	case "file":
		c.previewFileData(ctx, &request)
	default:
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "Invalid source type: " + request.SourceType,
		})
	}
}

// previewClickHouseData previews data from a ClickHouse table
func (c *DataController) previewClickHouseData(ctx *gin.Context, request *models.DataPreviewRequest) {
	if request.SourceTable == "" {
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "Source table is required",
		})
		return
	}

	// Get the ClickHouse client
	client, ok := c.connectionController.GetClickHouseClient(request.ConnectionID)
	if !ok {
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "Connection not found",
		})
		return
	}

	// Preview data
	data, err := client.PreviewTableData(request.SourceTable, request.Columns, request.Limit)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.DataPreviewResponse{
			Success: false,
			Error:   "Failed to preview data: " + err.Error(),
		})
		return
	}

	// Get headers from the first row if available
	var headers []string
	if len(data) > 0 {
		headers = make([]string, 0, len(data[0]))
		for key := range data[0] {
			headers = append(headers, key)
		}
	}

	ctx.JSON(http.StatusOK, models.DataPreviewResponse{
		Success: true,
		Headers: headers,
		Data:    data,
	})
}

// previewFileData previews data from a file
func (c *DataController) previewFileData(ctx *gin.Context, request *models.DataPreviewRequest) {
	if request.FilePath == "" {
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "File path is required",
		})
		return
	}

	// Validate file
	if err := utils.ValidateFile(request.FilePath); err != nil {
		ctx.JSON(http.StatusBadRequest, models.DataPreviewResponse{
			Success: false,
			Error:   "Invalid file: " + err.Error(),
		})
		return
	}

	// Create CSV handler
	handler, err := c.connectionController.CreateCSVHandler(request.ConnectionID, request.FilePath)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.DataPreviewResponse{
			Success: false,
			Error:   "Failed to create CSV handler: " + err.Error(),
		})
		return
	}
	defer handler.Close()

	// Open file for reading
	if err := handler.OpenForReading(); err != nil {
		ctx.JSON(http.StatusInternalServerError, models.DataPreviewResponse{
			Success: false,
			Error:   "Failed to open file: " + err.Error(),
		})
		return
	}

	// Preview data
	headers, data, err := handler.PreviewData(request.Limit)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.DataPreviewResponse{
			Success: false,
			Error:   "Failed to preview data: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, models.DataPreviewResponse{
		Success: true,
		Headers: headers,
		Data:    data,
	})
}

// TransferData transfers data between ClickHouse and files
func (c *DataController) TransferData(ctx *gin.Context) {
	var request models.DataTransferRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.DataTransferResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// Validate request
	if err := c.validateTransferRequest(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.DataTransferResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	// Generate transfer ID
	transferID := uuid.New().String()

	// Create data transfer service
	transferService := utils.NewDataTransferService()

	// Store the transfer service
	c.transfers.Store(transferID, transferService)

	// Start transfer in a goroutine
	go func() {
		defer func() {
			// Clean up
			transferService.Close()
			c.transfers.Delete(transferID)
		}()

		// Execute transfer based on direction
		var recordCount int64
		var err error

		if request.SourceType == "clickhouse" && request.DestinationType == "file" {
			recordCount, err = c.transferClickHouseToFile(transferService, &request)
		} else if request.SourceType == "file" && request.DestinationType == "clickhouse" {
			recordCount, err = c.transferFileToClickHouse(transferService, &request)
		}

		// Log the result
		if err != nil {
			fmt.Printf("Transfer %s failed: %v\n", transferID, err)
		} else {
			fmt.Printf("Transfer %s completed: %d records processed\n", transferID, recordCount)
		}
	}()

	// Return response with transfer ID
	ctx.JSON(http.StatusOK, models.DataTransferResponse{
		Success: true,
		Message: "Transfer started",
	})
}

// GetTransferProgress gets the progress of a data transfer
func (c *DataController) GetTransferProgress(ctx *gin.Context) {
	transferID := ctx.Param("id")

	// Get the transfer service
	service, ok := c.transfers.Load(transferID)
	if !ok {
		ctx.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Transfer not found or completed",
		})
		return
	}

	// Get the progress channel
	progressChan := service.(*utils.DataTransferService).GetProgressChan()

	// Create a context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx.Request.Context(), 500*time.Millisecond)
	defer cancel()

	// Try to get the latest progress update
	var latestUpdate models.ProgressUpdate
	select {
	case update, ok := <-progressChan:
		if !ok {
			// Channel closed, transfer complete
			ctx.JSON(http.StatusOK, gin.H{
				"success": true,
				"status":  "complete",
			})
			return
		}
		latestUpdate = update
	case <-timeoutCtx.Done():
		// Timeout, return current status
		ctx.JSON(http.StatusOK, gin.H{
			"success": true,
			"status":  "in_progress",
			"message": "No recent progress updates",
		})
		return
	}

	// Return the progress update
	ctx.JSON(http.StatusOK, gin.H{
		"success":           true,
		"status":            "in_progress",
		"processed_records": latestUpdate.ProcessedRecords,
		"total_records":     latestUpdate.TotalRecords,
		"percentage":        latestUpdate.Percentage,
		"status_message":    latestUpdate.Status,
	})
}

// transferClickHouseToFile transfers data from ClickHouse to a file
func (c *DataController) transferClickHouseToFile(service *utils.DataTransferService, request *models.DataTransferRequest) (int64, error) {
	// Get the ClickHouse client
	client, ok := c.connectionController.GetClickHouseClient(request.ConnectionID)
	if !ok {
		return 0, fmt.Errorf("connection not found")
	}

	// Get destination file path
	destFilePath := request.DestFilePath
	if destFilePath == "" {
		// Generate a file path if not provided
		timestamp := time.Now().Format("20060102_150405")
		destFilePath = filepath.Join(c.config.TempDir, fmt.Sprintf("%s_%s.csv", request.SourceTable, timestamp))
	}

	// Create CSV handler
	handler, err := c.connectionController.CreateCSVHandler(request.ConnectionID, destFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create CSV handler: %w", err)
	}
	defer handler.Close()

	// Setup transfer service
	service.SetupClickHouseClient(client)
	service.SetupCSVHandler(handler)

	// Execute transfer
	return service.ClickHouseToCSV(context.Background(), request)
}

// transferFileToClickHouse transfers data from a file to ClickHouse
func (c *DataController) transferFileToClickHouse(service *utils.DataTransferService, request *models.DataTransferRequest) (int64, error) {
	// Get the ClickHouse client
	client, ok := c.connectionController.GetClickHouseClient(request.ConnectionID)
	if !ok {
		return 0, fmt.Errorf("connection not found")
	}

	// Create CSV handler
	handler, err := c.connectionController.CreateCSVHandler(request.ConnectionID, request.SourceFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create CSV handler: %w", err)
	}
	defer handler.Close()

	// Open file for schema detection
	if err := handler.OpenForReading(); err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}

	// Detect schema
	columns, err := handler.DetectSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to detect schema: %w", err)
	}

	// Setup transfer service
	service.SetupClickHouseClient(client)
	service.SetupCSVHandler(handler)

	// Execute transfer
	return service.CSVToClickHouse(context.Background(), request, columns)
}

// validateTransferRequest validates a data transfer request
func (c *DataController) validateTransferRequest(request *models.DataTransferRequest) error {
	// Check connection
	if request.ConnectionID == "" {
		return fmt.Errorf("connection ID is required")
	}

	// Check source type
	if request.SourceType != "clickhouse" && request.SourceType != "file" {
		return fmt.Errorf("invalid source type: %s", request.SourceType)
	}

	// Check destination type
	if request.DestinationType != "clickhouse" && request.DestinationType != "file" {
		return fmt.Errorf("invalid destination type: %s", request.DestinationType)
	}

	// Check source and destination are different
	if request.SourceType == request.DestinationType {
		return fmt.Errorf("source and destination types must be different")
	}

	// Check source specific requirements
	if request.SourceType == "clickhouse" {
		if request.SourceTable == "" {
			return fmt.Errorf("source table is required for ClickHouse source")
		}
	} else if request.SourceType == "file" {
		if request.SourceFilePath == "" {
			return fmt.Errorf("source file path is required for file source")
		}
		if err := utils.ValidateFile(request.SourceFilePath); err != nil {
			return fmt.Errorf("invalid source file: %w", err)
		}
	}

	// Check destination specific requirements
	if request.DestinationType == "clickhouse" {
		if request.DestTable == "" {
			return fmt.Errorf("destination table is required for ClickHouse destination")
		}
	}

	// Check columns
	if len(request.SelectedColumns) == 0 {
		return fmt.Errorf("at least one column must be selected")
	}

	return nil
} 