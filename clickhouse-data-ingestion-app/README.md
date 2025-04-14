# ClickHouse Data Ingestion Application

A web-based application for bidirectional data transfer between ClickHouse databases and flat files (CSV, TSV, etc.).

## Features

- **ClickHouse Connection**: Connect to ClickHouse databases with standard or JWT token authentication
- **File Handling**: Support for CSV and other flat files with custom delimiters
- **Schema Discovery**: Automatically discover and display database table schemas and flat file structures
- **Bidirectional Transfer**:
  - ClickHouse to File: Export data from database tables to flat files
  - File to ClickHouse: Import data from flat files to database tables
- **Data Preview**: Preview data from both sources before transfer
- **Progress Tracking**: Real-time progress updates during data transfer
- **Error Handling**: Graceful error handling with user-friendly messages

## Architecture

The application follows a client-server architecture:

- **Backend**: Go server with ClickHouse client and file handling capabilities
- **Frontend**: React application with Material UI components
- **API**: RESTful API for communication between frontend and backend

## Project Structure

```
clickhouse-data-ingestion-app/
├── backend/              # Go backend code
│   ├── config/           # Application configuration
│   ├── controllers/      # API controllers
│   ├── models/           # Data models
│   ├── routes/           # API routes
│   ├── utils/            # Utility functions
│   └── main.go           # Entry point
│
├── frontend/             # React frontend code
│   ├── public/           # Static assets
│   └── src/              # React components and logic
│       ├── components/   # Reusable UI components
│       ├── pages/        # Page components
│       └── services/     # API client services
│
└── README.md             # This file
```

## Getting Started

### Prerequisites

- Go 1.17+
- Node.js 14+
- ClickHouse database
- Git

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/clickhouse-data-ingestion-app.git
   cd clickhouse-data-ingestion-app
   ```

2. Install backend dependencies:
   ```
   cd backend
   go mod download
   ```

3. Install frontend dependencies:
   ```
   cd ../frontend
   npm install
   ```

### Running the Application

1. Start the backend server:
   ```
   cd backend
   go run main.go
   ```

2. Start the frontend development server:
   ```
   cd frontend
   npm start
   ```

3. Open your browser and navigate to `http://localhost:3000`

## Usage

1. **Connect to ClickHouse**:
   - Enter your ClickHouse connection details
   - Use JWT token if required

2. **For ClickHouse to File**:
   - Select source table
   - Choose columns to export
   - Select destination file path

3. **For File to ClickHouse**:
   - Upload or select the source file
   - Configure delimiter and header settings
   - Select destination table (create new or use existing)
   - Map columns if needed

4. **Start Transfer**:
   - Click the transfer button
   - Monitor progress
   - View results when complete

## License

MIT

## Acknowledgements

- [ClickHouse](https://clickhouse.com/) - High-performance column-oriented database management system
- [Gin Web Framework](https://github.com/gin-gonic/gin) - HTTP web framework for Go
- [React](https://reactjs.org/) - JavaScript library for building user interfaces
- [Material UI](https://mui.com/) - React UI framework 