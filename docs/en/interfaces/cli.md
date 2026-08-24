---
description: 'Documentation for clickhousectl, the CLI for ClickHouse: local and cloud'
sidebar_label: 'clickhousectl'
sidebar_position: 17
slug: /interfaces/cli
title: 'clickhousectl'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge/>

`clickhousectl` is the CLI for ClickHouse: local and cloud.

With `clickhousectl` you can:
- Install and manage local ClickHouse versions
- Launch and manage local ClickHouse servers
- Execute queries against ClickHouse servers
- Set up ClickHouse Cloud and create cloud-managed ClickHouse clusters
- Manage ClickHouse Cloud resources
- Install the official ClickHouse agent skills into supported coding agents
- Push your local ClickHouse development to cloud

`clickhousectl` helps humans and AI-agents to develop with ClickHouse.

## Installation {#installation}

### Quick install {#quick-install}

```bash
curl https://clickhouse.com/cli | sh
```

The install script downloads the correct version for your OS and installs to `~/.local/bin/clickhousectl`. A `chctl` alias is also created automatically for convenience.

## Requirements {#requirements}

- macOS (aarch64, x86_64) or Linux (aarch64, x86_64)
- Cloud commands require a [ClickHouse Cloud API key](/cloud/manage/api/api-overview)

## Local {#local}

### Installing and managing ClickHouse versions {#installing-versions}

`clickhousectl` downloads ClickHouse binaries from [GitHub releases](https://github.com/ClickHouse/ClickHouse/releases).

```bash
# Install a version
clickhousectl local install stable          # Latest stable release
clickhousectl local install lts             # Latest LTS release
clickhousectl local install 26.3            # Latest 26.3.x.x
clickhousectl local install 26.3.4.3        # Exact version

# List versions
clickhousectl local list                    # Installed versions
clickhousectl local list --remote           # Available for download

# Manage default version
clickhousectl local use stable              # Latest stable (installs if needed)
clickhousectl local use lts                 # Latest LTS (installs if needed)
clickhousectl local use 26.3                # Latest 26.3.x.x (installs if needed)
clickhousectl local use 26.3.4.3            # Exact version
clickhousectl local which                   # Show current default

# Remove a version
clickhousectl local remove 26.3.4.3
```

#### ClickHouse binary storage {#binary-storage}

ClickHouse binaries are stored in a global repository, so they can be used by multiple projects without duplicating storage. Binaries are stored in `~/.clickhousectl/`:

```bash
~/.clickhousectl/
├── versions/
│   └── 26.3.4.3/
│       └── clickhouse
└── default              # tracks the active version
```

### Initializing a project {#initializing-project}

```bash
clickhousectl local init
```

`init` bootstraps your current working directory with a standard folder structure for your ClickHouse project files. It is optional; you are welcome to use your own folder structure if preferred.

It creates the following structure:

```bash
clickhouse/
├── tables/                 # Table definitions (CREATE TABLE ...)
├── materialized_views/     # Materialized view definitions
├── queries/                # Saved queries
└── seed/                   # Seed data / INSERT statements
```

### Running queries {#running-queries}

```bash
# Connect to a running server with clickhouse-client
clickhousectl local client                           # Connects to "default" server
clickhousectl local client --name dev                # Connects to "dev" server
clickhousectl local client --query "SHOW DATABASES"  # Run a query
clickhousectl local client --queries-file schema.sql # Run queries from a file
clickhousectl local client --host remote-host --port 9000  # Connect to a specific host/port
```

### Creating and managing ClickHouse servers {#managing-servers}

Start and manage ClickHouse server instances. Each server gets its own isolated data directory at `.clickhousectl/servers/<name>/data/`.

```bash
# Start a server (runs in background by default)
clickhousectl local server start                          # Named "default"
clickhousectl local server start --name dev               # Named "dev"
clickhousectl local server start --foreground             # Run in foreground (-F / --fg)
clickhousectl local server start --http-port 8124 --tcp-port 9001  # Explicit ports
clickhousectl local server start -- --config-file=/path/to/config.xml

# List all servers (running and stopped)
clickhousectl local server list

# Stop servers
clickhousectl local server stop default                   # Stop by name
clickhousectl local server stop-all                       # Stop all running servers

# Remove a stopped server and its data
clickhousectl local server remove test
```

**Server naming:** Without `--name`, the first server is called "default". If "default" is already running, a random name is generated (e.g. "bold-crane"). Use `--name` for stable identities you can start/stop repeatedly.

**Ports:** Defaults are HTTP 8123 and TCP 9000. If these are already in use, free ports are automatically assigned and shown in the output. Use `--http-port` and `--tcp-port` to set explicit ports.

#### Project-local data directory {#project-local-data}

All server data lives inside `.clickhousectl/` in your project directory:

```bash
.clickhousectl/
├── .gitignore              # auto-created, ignores everything
├── credentials.json        # cloud API credentials (if configured)
└── servers/
    ├── default/
    │   └── data/           # ClickHouse data files for "default" server
    └── dev/
        └── data/           # ClickHouse data files for "dev" server
```

Each named server has its own data directory, so servers are fully isolated from each other. Data persists between restarts — stop and start a server by name to pick up where you left off. Use `clickhousectl local server remove <name>` to permanently delete a server's data.

## Authentication {#authentication}

Authenticate to ClickHouse Cloud using OAuth (browser-based) or API keys.

### OAuth login (recommended) {#oauth-login}

```bash
clickhousectl cloud auth login
```

This opens your browser for authentication via the OAuth device flow. Tokens are saved to `.clickhousectl/tokens.json` (project-local).

### API key/secret {#api-key}

```bash
# Non-interactive (CI-friendly)
clickhousectl cloud auth login --api-key YOUR_KEY --api-secret YOUR_SECRET

# Interactive prompt
clickhousectl cloud auth login --interactive
```

Credentials are saved to `.clickhousectl/credentials.json` (project-local).

You can also use environment variables:
```bash
export CLICKHOUSE_CLOUD_API_KEY=your-key
export CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

Or pass credentials directly via flags on any command:
```bash
clickhousectl cloud --api-key KEY --api-secret SECRET ...
```

### Auth status and logout {#auth-status}

```bash
clickhousectl cloud auth status    # Show current auth state
clickhousectl cloud auth logout    # Clear all saved credentials (credentials.json & tokens.json)
```

Credential resolution order: CLI flags > OAuth tokens > `.clickhousectl/credentials.json` > environment variables.

## Cloud {#cloud}

Manage ClickHouse Cloud services via the API.

### Organizations {#organizations}

```bash
clickhousectl cloud org list              # List organizations
clickhousectl cloud org get <org-id>      # Get organization details
clickhousectl cloud org update <org-id> --name "Renamed Org"
clickhousectl cloud org update <org-id> \
  --remove-private-endpoint pe-1,cloud-provider=aws,region=us-east-1 \
  --enable-core-dumps false
clickhousectl cloud org prometheus <org-id> --filtered-metrics true
clickhousectl cloud org usage <org-id> \
  --from-date 2024-01-01 \
  --to-date 2024-01-31
```

### Services {#services}

```bash
# List services
clickhousectl cloud service list

# Get service details
clickhousectl cloud service get <service-id>

# Create a service (minimal)
clickhousectl cloud service create --name my-service

# Create with scaling options
clickhousectl cloud service create --name my-service \
  --provider aws \
  --region us-east-1 \
  --min-replica-memory-gb 8 \
  --max-replica-memory-gb 32 \
  --num-replicas 2

# Create with specific IP allowlist
clickhousectl cloud service create --name my-service \
  --ip-allow 10.0.0.0/8 \
  --ip-allow 192.168.1.0/24

# Create from backup
clickhousectl cloud service create --name restored-service --backup-id <backup-uuid>

# Create with release channel
clickhousectl cloud service create --name my-service --release-channel fast

# Start/stop a service
clickhousectl cloud service start <service-id>
clickhousectl cloud service stop <service-id>

# Connect to a cloud service with clickhouse-client
clickhousectl cloud service client --name my-service --password secret
clickhousectl cloud service client --id <service-id> -q "SELECT 1" --password secret

# Use CLICKHOUSE_PASSWORD env var (recommended for scripts/agents)
CLICKHOUSE_PASSWORD=secret clickhousectl cloud service client \
  --name my-service -q "SELECT count() FROM system.tables"

# Update service metadata and patches
clickhousectl cloud service update <service-id> \
  --name my-renamed-service \
  --add-ip-allow 10.0.0.0/8 \
  --remove-ip-allow 0.0.0.0/0 \
  --release-channel fast

# Update replica scaling
clickhousectl cloud service scale <service-id> \
  --min-replica-memory-gb 24 \
  --max-replica-memory-gb 48 \
  --num-replicas 3 \
  --idle-scaling true \
  --idle-timeout-minutes 10

# Reset password with generated credentials
clickhousectl cloud service reset-password <service-id>

# Delete a service (must be stopped first)
clickhousectl cloud service delete <service-id>

# Force delete: stops a running service then deletes
clickhousectl cloud service delete <service-id> --force
```

#### Service create options {#service-create-options}

| Option | Description |
|--------|-------------|
| `--name` | Service name (required) |
| `--provider` | Cloud provider: `aws`, `gcp`, `azure` (default: `aws`) |
| `--region` | Region (default: `us-east-1`) |
| `--min-replica-memory-gb` | Min memory per replica in GB (8-356, multiple of 4) |
| `--max-replica-memory-gb` | Max memory per replica in GB (8-356, multiple of 4) |
| `--num-replicas` | Number of replicas (1-20) |
| `--idle-scaling` | Allow scale to zero (default: `true`) |
| `--idle-timeout-minutes` | Min idle timeout in minutes (>= 5) |
| `--ip-allow` | IP CIDR to allow (repeatable, default: `0.0.0.0/0`) |
| `--backup-id` | Backup ID to restore from |
| `--release-channel` | Release channel: `slow`, `default`, `fast` |

#### Query endpoint management {#query-endpoints}

```bash
clickhousectl cloud service query-endpoint get <service-id>
clickhousectl cloud service query-endpoint create <service-id> \
  --role admin \
  --open-api-key key-1 \
  --allowed-origins https://app.example.com
clickhousectl cloud service query-endpoint delete <service-id>
```

#### Private endpoint management {#private-endpoints}

```bash
clickhousectl cloud service private-endpoint create <service-id> --endpoint-id vpce-123
clickhousectl cloud service private-endpoint get-config <service-id>
```

#### Backup configuration {#backup-config}

```bash
clickhousectl cloud service backup-config get <service-id>
clickhousectl cloud service backup-config update <service-id> \
  --backup-period-hours 24 \
  --backup-retention-period-hours 720 \
  --backup-start-time 02:00
```

### Backups {#backups}

```bash
clickhousectl cloud backup list <service-id>
clickhousectl cloud backup get <service-id> <backup-id>
```

### Members {#members}

```bash
clickhousectl cloud member list
clickhousectl cloud member get <user-id>
clickhousectl cloud member update <user-id> --role-id <role-id>
clickhousectl cloud member remove <user-id>
```

### Invitations {#invitations}

```bash
clickhousectl cloud invitation list
clickhousectl cloud invitation create --email dev@example.com --role-id <role-id>
clickhousectl cloud invitation get <invitation-id>
clickhousectl cloud invitation delete <invitation-id>
```

### Keys {#keys}

```bash
clickhousectl cloud key list
clickhousectl cloud key get <key-id>
clickhousectl cloud key create --name ci-key --role-id <role-id> --ip-allow 10.0.0.0/8
clickhousectl cloud key update <key-id> \
  --name renamed-key \
  --expires-at 2025-12-31T00:00:00Z \
  --state disabled \
  --ip-allow 0.0.0.0/0
clickhousectl cloud key delete <key-id>
```

### Activity {#activity}

```bash
clickhousectl cloud activity list --from-date 2024-01-01 --to-date 2024-12-31
clickhousectl cloud activity get <activity-id>
```

### JSON output {#json-output}

Use the `--json` flag to print JSON-formatted responses.

```bash
clickhousectl cloud --json service list
clickhousectl cloud --json service get <service-id>
```

## Skills {#skills}

Install the official ClickHouse Agent Skills from [ClickHouse/agent-skills](https://github.com/ClickHouse/agent-skills).

```bash
# Default: interactive mode for humans, choose scope, then choose agents
clickhousectl skills

# Non-interactive: install into every supported project-local agent folder
clickhousectl skills --all

# Non-interactive: install only into detected agents
clickhousectl skills --detected-only

# Non-interactive: install into every supported global agent folder
clickhousectl skills --global --all

# Non-interactive: install into specific project-local agents
clickhousectl skills --agent claude --agent codex
```

### Non-interactive flags {#non-interactive-flags}

| Flag | Description |
|------|-------------|
| `--agent <name>` | Install Skills for a specific agent (can be repeated) |
| `--global` | Use global scope; if omitted, project scope is used |
| `--all` | Install Skills for all supported agents |
| `--detected-only` | Install Skills for supported agents that were detected on the system |
