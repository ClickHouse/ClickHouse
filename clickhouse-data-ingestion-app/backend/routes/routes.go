package routes

import (
	"clickhouse-ingestion/controllers"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// SetupRouter sets up the routing for the application
func SetupRouter(
	connectionController *controllers.ConnectionController,
	schemaController *controllers.SchemaController,
	dataController *controllers.DataController,
) *gin.Engine {
	router := gin.Default()

	// Configure CORS
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	// Add health check route
	router.GET("/api/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	// API routes
	api := router.Group("/api")
	{
		// Connection routes
		connections := api.Group("/connections")
		{
			connections.POST("/clickhouse", connectionController.ConnectToClickHouse)
			connections.POST("/file", connectionController.ConnectToFile)
			connections.DELETE("/:id", connectionController.CloseConnection)
		}

		// Schema discovery routes
		schemas := api.Group("/schemas")
		{
			schemas.POST("/clickhouse", schemaController.DiscoverClickHouseSchema)
			schemas.POST("/file", schemaController.DiscoverFileSchema)
		}

		// Data routes
		data := api.Group("/data")
		{
			data.POST("/preview", dataController.PreviewData)
			data.POST("/transfer", dataController.TransferData)
			data.GET("/transfer/:id/progress", dataController.GetTransferProgress)
		}
	}

	return router
} 