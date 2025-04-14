package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// Config holds all the configuration values for the application
type Config struct {
	Port            string
	TempDir         string
	MaxUploadSize   int64
	JWTSecret       string
	ClickHouseHosts []string
	ClickHousePort  string
}

// LoadConfig loads the configuration from environment variables
func LoadConfig() (*Config, error) {
	// Try to load .env file if exists
	godotenv.Load()

	// Create temp directory for file uploads if it doesn't exist
	tempDir := "./temp"
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return nil, err
	}

	// Default config values
	config := &Config{
		Port:            getEnv("PORT", "8080"),
		TempDir:         tempDir,
		MaxUploadSize:   getEnvAsInt64("MAX_UPLOAD_SIZE", 100*1024*1024), // 100MB default
		JWTSecret:       getEnv("JWT_SECRET", "default_secret_key_change_in_production"),
		ClickHouseHosts: []string{getEnv("CLICKHOUSE_HOST", "localhost")},
		ClickHousePort:  getEnv("CLICKHOUSE_PORT", "9000"),
	}

	return config, nil
}

// Helper functions to get environment variables with defaults
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt64(key string, defaultValue int64) int64 {
	if value, exists := os.LookupEnv(key); exists {
		val := 0
		if _, err := fmt.Sscanf(value, "%d", &val); err == nil {
			return int64(val)
		}
	}
	return defaultValue
}

// EnsureTempDir creates the temporary directory and returns the path
func (c *Config) EnsureTempDir() (string, error) {
	if err := os.MkdirAll(c.TempDir, os.ModePerm); err != nil {
		return "", err
	}
	return c.TempDir, nil
}

// GetTempFilePath returns a path for a temporary file with the given name
func (c *Config) GetTempFilePath(filename string) string {
	return filepath.Join(c.TempDir, filename)
} 