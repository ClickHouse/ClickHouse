---
description: 'Documentation for the ClickHouse Keeper HTTP API and embedded dashboard'
sidebar_label: 'Keeper HTTP API'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'Keeper HTTP API and Dashboard'
doc_type: 'reference'
---

ClickHouse Keeper provides an HTTP API and embedded web dashboard for monitoring, health checks, and storage management. 
This interface allows operators to inspect cluster status, execute commands, and manage Keeper storage through a web browser or HTTP clients.

## Configuration {#configuration}

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

### Configuration Options {#configuration-options}

| Setting                                   | Default  | Description                                |
|-------------------------------------------|----------|--------------------------------------------|
| `http_control.port`                       | -        | HTTP port for dashboard and API            |
| `http_control.secure_port`                | -        | HTTPS port (requires SSL configuration)    |
| `http_control.readiness.endpoint`         | `/ready` | Custom path for the readiness probe        |
| `http_control.storage.session_timeout_ms` | `30000`  | Session timeout for storage API operations |

## Endpoints {#endpoints}

### Dashboard {#dashboard}

- **Path**: `/dashboard`
- **Method**: GET
- **Description**: Serves an embedded web dashboard for monitoring and managing Keeper

The dashboard provides:
- Real-time cluster status visualization
- Node monitoring (role, latency, connections)
- Storage browser
- Command execution interface

#### Cluster tab {#dashboard-cluster-tab}

The **Cluster** tab renders the Raft membership as a topology graph and a table. Each member is shown with a health color:

- **Green** — alive and in sync with the leader
- **Yellow** — alive but lagging behind the leader by more than `stale_log_gap` log entries
- **Red** — unreachable (no successful Raft response within the heartbeat expiry window)
- **Gray** — unknown (peer health is only visible from the leader; followers see their peers as unknown)

The table also shows each member's role (leader, follower, or observer), Raft priority, last log index, replication lag relative to the leader, and the time since the last successful Raft response. When the current node is not the leader, the tab offers a deep-link that opens the leader's dashboard, where full peer health is available. The tab can be opened directly with `/dashboard?tab=cluster`.

### Readiness Probe {#readiness-probe}

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

### Commands API {#commands-api}

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

### Storage API {#storage-api}

- **Base Path**: `/api/v1/storage`
- **Description**: REST API for Keeper storage operations

The Storage API follows REST conventions where HTTP methods indicate the operation type:

| Operation | Path                                       | Method | Status Code | Description          |
|-----------|--------------------------------------------|--------|-------------|----------------------|
| Get       | `/api/v1/storage/{path}`                   | GET    | 200         | Get node data        |
| List      | `/api/v1/storage/{path}?children=true`     | GET    | 200         | List child nodes     |
| Exists    | `/api/v1/storage/{path}`                   | HEAD   | 200         | Check if node exists |
| Create    | `/api/v1/storage/{path}`                   | POST   | 201         | Create new node      |
| Update    | `/api/v1/storage/{path}?version={v}`       | PUT    | 200         | Update node data     |
| Delete    | `/api/v1/storage/{path}?version={v}`       | DELETE | 204         | Delete node          |
