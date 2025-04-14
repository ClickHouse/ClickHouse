package controllers

import (
	"errors"
	"net/http"
	"sync"

	"clickhouse-ingestion/models"
	"clickhouse-ingestion/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ConnectionController handles connection requests
type ConnectionController struct {
	clickHouseConnections sync.Map // Map of connection ID to ClickHouseClient
	fileConnections       sync.Map // Map of connection ID to CSVHandler
}

// NewConnectionController creates a new connection controller
func NewConnectionController() *ConnectionController {
	return &ConnectionController{
		clickHouseConnections: sync.Map{},
		fileConnections:       sync.Map{},
	}
}

// ConnectToClickHouse handles ClickHouse connection requests
func (c *ConnectionController) ConnectToClickHouse(ctx *gin.Context) {
	var request models.ClickHouseConnectionRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.ConnectionResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// Create a new client
	client, err := utils.NewClickHouseClient(&request)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, models.ConnectionResponse{
			Success: false,
			Error:   "Failed to connect to ClickHouse: " + err.Error(),
		})
		return
	}

	// Generate a connection ID
	connectionID := uuid.New().String()

	// Store the client
	c.clickHouseConnections.Store(connectionID, client)

	// Return success
	ctx.JSON(http.StatusOK, models.ConnectionResponse{
		Success:      true,
		Message:      "Successfully connected to ClickHouse",
		ConnectionID: connectionID,
	})
}

// ConnectToFile handles file connection requests
func (c *ConnectionController) ConnectToFile(ctx *gin.Context) {
	var request models.FileConnectionRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.ConnectionResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// For file "connections", we just validate that the delimiter is valid
	// The actual file path will be provided later
	if len(request.Delimiter) == 0 {
		request.Delimiter = "," // Default delimiter
	}
	
	// Generate a connection ID
	connectionID := uuid.New().String()

	// Store the connection config - we'll create handlers as needed
	c.fileConnections.Store(connectionID, request)

	// Return success
	ctx.JSON(http.StatusOK, models.ConnectionResponse{
		Success:      true,
		Message:      "Successfully configured file connection",
		ConnectionID: connectionID,
	})
}

// GetClickHouseClient returns a ClickHouse client for a connection ID
func (c *ConnectionController) GetClickHouseClient(connectionID string) (*utils.ClickHouseClient, bool) {
	client, ok := c.clickHouseConnections.Load(connectionID)
	if !ok {
		return nil, false
	}
	return client.(*utils.ClickHouseClient), true
}

// GetFileConnection returns file connection settings for a connection ID
func (c *ConnectionController) GetFileConnection(connectionID string) (*models.FileConnectionRequest, bool) {
	config, ok := c.fileConnections.Load(connectionID)
	if !ok {
		return nil, false
	}
	conn := config.(models.FileConnectionRequest)
	return &conn, true
}

// CloseConnection closes a connection
func (c *ConnectionController) CloseConnection(ctx *gin.Context) {
	connectionID := ctx.Param("id")
	
	// Try to close ClickHouse connection
	if client, ok := c.clickHouseConnections.Load(connectionID); ok {
		client.(*utils.ClickHouseClient).Close()
		c.clickHouseConnections.Delete(connectionID)
		ctx.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "ClickHouse connection closed",
		})
		return
	}
	
	// Try to close file connection
	if _, ok := c.fileConnections.Load(connectionID); ok {
		c.fileConnections.Delete(connectionID)
		ctx.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "File connection closed",
		})
		return
	}
	
	ctx.JSON(http.StatusNotFound, gin.H{
		"success": false,
		"error":   "Connection not found",
	})
}

// CreateCSVHandler creates a CSV handler for a file
func (c *ConnectionController) CreateCSVHandler(connectionID, filePath string) (*utils.CSVHandler, error) {
	fileConn, ok := c.GetFileConnection(connectionID)
	if !ok {
		return nil, errors.New("file connection not found")
	}
	
	// Create a new CSV handler
	handler, err := utils.NewCSVHandler(filePath, fileConn.Delimiter, fileConn.HasHeader)
	if err != nil {
		return nil, err
	}
	
	return handler, nil
} 