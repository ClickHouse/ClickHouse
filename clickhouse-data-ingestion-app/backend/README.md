# ClickHouse Data Ingestion Backend

This is the backend component of the ClickHouse Data Ingestion Application, built with Go.

## Features

- Connect to ClickHouse databases with JWT token authentication
- Connect to flat files (CSV, TSV, etc.) with custom delimiters
- Discover table schemas from ClickHouse
- Discover column structures from flat files
- Transfer data bidirectionally between ClickHouse and flat files
- Preview data from both sources
- Progress tracking for data transfers
- Batched data processing for better performance

## Requirements

- Go 1.17 or higher
- ClickHouse server
- Access to filesystem for flat file operations

## Project Structure

```
backend/
├── config/         # Application configuration
├── controllers/    # API controllers
├── models/         # Data models
├── routes/         # API routes
├── utils/          # Utility functions and services
├── main.go         # Main application entry point
├── go.mod          # Go module definition
└── go.sum          # Go module checksums
```

## Getting Started

1. Install Go (version 1.17 or higher)
2. Clone the repository
3. Install dependencies:
   ```
   go mod download
   ```
4. Build the application:
   ```
   go build -o clickhouse-ingestion
   ```
5. Run the application:
   ```
   ./clickhouse-ingestion
   ```

By default, the server will run on port 8080. You can change this by setting the `PORT` environment variable.

## Environment Variables

- `PORT`: Server port (default: 8080)
- `MAX_UPLOAD_SIZE`: Maximum file upload size in bytes (default: 100MB)
- `JWT_SECRET`: Secret key for JWT validation
- `CLICKHOUSE_HOST`: Default ClickHouse host (default: localhost)
- `CLICKHOUSE_PORT`: Default ClickHouse port (default: 9000)

## API Endpoints

### Connection Management

- `POST /api/connections/clickhouse`: Connect to ClickHouse
- `POST /api/connections/file`: Configure file connection settings
- `DELETE /api/connections/:id`: Close a connection

### Schema Discovery

- `POST /api/schemas/clickhouse`: Discover schema from ClickHouse
- `POST /api/schemas/file`: Discover schema from a flat file

### Data Operations

- `POST /api/data/preview`: Preview data from ClickHouse or a file
- `POST /api/data/transfer`: Transfer data between ClickHouse and files
- `GET /api/data/transfer/:id/progress`: Get progress of a data transfer

## Error Handling

The backend provides detailed error messages for all operations and handles errors gracefully with appropriate HTTP status codes.

## License

MIT 