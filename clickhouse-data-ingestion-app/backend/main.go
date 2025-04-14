package main

import (
	"fmt"
	"os"
	"path/filepath"

	"clickhouse-ingestion/config"
	"clickhouse-ingestion/controllers"
	"clickhouse-ingestion/routes"
	"github.com/gin-gonic/gin"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Ensure temp directory exists
	tempDir, err := cfg.EnsureTempDir()
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Using temp directory: %s\n", tempDir)

	// Create controllers
	connectionController := controllers.NewConnectionController()
	schemaController := controllers.NewSchemaController(connectionController)
	dataController := controllers.NewDataController(connectionController, cfg)

	// Setup router
	router := routes.SetupRouter(connectionController, schemaController, dataController)

	// Serve static files from the frontend
	frontendPath := filepath.Join(".", "..", "frontend", "build")
	if _, err := os.Stat(frontendPath); !os.IsNotExist(err) {
		router.StaticFS("/", gin.Dir(frontendPath, false))
		fmt.Printf("Serving frontend from: %s\n", frontendPath)
	} else {
		fmt.Printf("Frontend directory not found at %s. API-only mode.\n", frontendPath)
	}

	// Start server
	fmt.Printf("Starting server on port %s\n", cfg.Port)
	if err := router.Run(":" + cfg.Port); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
} 