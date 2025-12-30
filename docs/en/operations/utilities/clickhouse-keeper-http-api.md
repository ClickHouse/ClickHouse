---
description: 'Documentation for the ClickHouse Keeper HTTP API and embedded dashboard'
sidebar_label: 'Keeper HTTP API'
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'Keeper HTTP API and Dashboard'
doc_type: 'reference'
---

# Keeper HTTP API and Dashboard

ClickHouse Keeper provides an HTTP API and embedded web dashboard for monitoring, health checks, and storage management. 
This interface allows operators to inspect cluster status, execute commands, and manage Keeper storage through a web browser or HTTP clients.

## Configuration

To enable the HTTP API, add the `http_control` section to your `keeper_server` configuration:

```xml
<keeper_server>
    <!-- Other keeper_server configuration -->

    <http_control>
        <port>9182</port>
        <!-- <secure_port>9443</secure_port> -->
    </http_control>
</keeper_server>
```

### Configuration Options

| Setting                                   | Default  | Description                                |
|-------------------------------------------|----------|--------------------------------------------|
| `http_control.port`                       | -        | HTTP port for dashboard and API            |
| `http_control.secure_port`                | -        | HTTPS port (requires SSL configuration)    |
| `http_control.readiness.endpoint`         | `/ready` | Custom path for the readiness probe        |
| `http_control.storage.session_timeout_ms` | `30000`  | Session timeout for storage API operations |

## Endpoints

### Dashboard

- **Path**: `/dashboard`
- **Method**: GET
- **Description**: Serves an embedded web dashboard for monitoring and managing Keeper

The dashboard provides:
- Real-time cluster status visualization
- Node monitoring (role, latency, connections)
- Storage browser
- Command execution interface

### Readiness Probe

- **Path**: `/ready` (configurable)
- **Method**: GET
- **Description**: Health check endpoint

Success response (HTTP 200):
```json
{
  "status": "ok",
  "details": {
    "role": "leader",
    "hasLeader": true
  }
}
```

### Commands API

- **Path**: `/api/v1/commands/{command}`
- **Methods**: GET, POST
- **Description**: Executes Four-Letter Word commands or ClickHouse Keeper Client CLI commands

Query parameters:
- `command` - The command to execute
- `cwd` - Current working directory for path-based commands (default: `/`)

Examples:
```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

### Storage API

- **Base Path**: `/api/v1/storage`
- **Description**: REST API for ZooKeeper-compatible storage operations

| Operation | Path                                        | Method | Description          |
|-----------|---------------------------------------------|--------|----------------------|
| List      | `/api/v1/storage/list/{path}`               | GET    | List child nodes     |
| Get       | `/api/v1/storage/get/{path}`                | GET    | Get node data        |
| Exists    | `/api/v1/storage/exists/{path}`             | GET    | Check if node exists |
| Create    | `/api/v1/storage/create/{path}`             | POST   | Create new node      |
| Set       | `/api/v1/storage/set/{path}?version={v}`    | POST   | Update node data     |
| Remove    | `/api/v1/storage/remove/{path}?version={v}` | POST   | Delete node          |
