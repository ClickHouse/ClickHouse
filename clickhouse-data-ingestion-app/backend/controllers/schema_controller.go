package controllers

import (
	"net/http"

	"clickhouse-ingestion/models"
	"clickhouse-ingestion/utils"

	"github.com/gin-gonic/gin"
)

// SchemaController handles schema discovery operations
type SchemaController struct {
	connectionController *ConnectionController
}

// NewSchemaController creates a new schema controller
func NewSchemaController(connectionController *ConnectionController) *SchemaController {
	return &SchemaController{
		connectionController: connectionController,
	}
}

// DiscoverClickHouseSchema discovers schema from a ClickHouse table
func (c *SchemaController) DiscoverClickHouseSchema(ctx *gin.Context) {
	var request models.SchemaRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// Get the ClickHouse client
	client, ok := c.connectionController.GetClickHouseClient(request.ConnectionID)
	if !ok {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "Connection not found",
		})
		return
	}

	// If no table is specified, get tables list
	if request.TableName == "" {
		tables, err := client.GetTables()
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, models.SchemaResponse{
				Success: false,
				Error:   "Failed to get tables: " + err.Error(),
			})
			return
		}

		// Create a column for each table
		columns := make([]models.Column, len(tables))
		for i, table := range tables {
			columns[i] = models.Column{
				Name:      table,
				Type:      "Table",
				IsNullable: false,
				Selected:  false,
			}
		}

		ctx.JSON(http.StatusOK, models.SchemaResponse{
			Success: true,
			Columns: columns,
		})
		return
	}

	// Get the schema for the specified table
	columns, err := client.GetTableSchema(request.TableName)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.SchemaResponse{
			Success: false,
			Error:   "Failed to get schema: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, models.SchemaResponse{
		Success: true,
		Columns: columns,
	})
}

// DiscoverFileSchema discovers schema from a flat file
func (c *SchemaController) DiscoverFileSchema(ctx *gin.Context) {
	var request models.SchemaRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "Invalid request: " + err.Error(),
		})
		return
	}

	// Check if file path is provided
	if request.FilePath == "" {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "File path is required",
		})
		return
	}

	// Validate file
	if err := utils.ValidateFile(request.FilePath); err != nil {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "Invalid file: " + err.Error(),
		})
		return
	}

	// Get the file connection settings
	fileConn, ok := c.connectionController.GetFileConnection(request.ConnectionID)
	if !ok {
		ctx.JSON(http.StatusBadRequest, models.SchemaResponse{
			Success: false,
			Error:   "Connection not found",
		})
		return
	}

	// Create a CSV handler
	handler, err := utils.NewCSVHandler(request.FilePath, fileConn.Delimiter, fileConn.HasHeader)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.SchemaResponse{
			Success: false,
			Error:   "Failed to create CSV handler: " + err.Error(),
		})
		return
	}
	defer handler.Close()

	// Open file for reading
	if err := handler.OpenForReading(); err != nil {
		ctx.JSON(http.StatusInternalServerError, models.SchemaResponse{
			Success: false,
			Error:   "Failed to open file: " + err.Error(),
		})
		return
	}

	// Detect schema
	columns, err := handler.DetectSchema()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, models.SchemaResponse{
			Success: false,
			Error:   "Failed to detect schema: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, models.SchemaResponse{
		Success: true,
		Columns: columns,
	})
} 