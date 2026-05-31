---
description: 'Documentation for clickhousectl, the CLI for ClickHouse: local and cloud'
sidebar_label: 'clickhousectl'
sidebar_position: 17
slug: /interfaces/cli
title: 'clickhousectl'
doc_type: 'reference'
---

`clickhousectl` is the CLI for ClickHouse: local and cloud.

With `clickhousectl` you can:
- Install and manage local ClickHouse versions
- Launch and manage local ClickHouse servers
- Run and manage local Postgres instances
- Execute queries against ClickHouse servers
- Set up ClickHouse Cloud and create cloud-managed ClickHouse clusters
- Create and manage ClickHouse Cloud Postgres services
- Manage ClickHouse Cloud resources
- Create and manage ClickPipes for data ingestion (S3, Kafka, Kinesis, Postgres, MySQL, MongoDB, BigQuery)
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

`clickhousectl` downloads ClickHouse binaries from `builds.clickhouse.com`, falling back to `packages.clickhouse.com` (Linux) or [GitHub releases](https://github.com/ClickHouse/ClickHouse/releases) (macOS) when a build isn't available there.

```bash
# Install a version
clickhousectl local install latest          # Latest release (recommended)
clickhousectl local install 26.5            # Latest 26.5.x.x
clickhousectl local install 26.5.2.39       # Exact version

# List versions
clickhousectl local list                    # Installed versions
clickhousectl local list --remote           # Available for download

# Manage default version
clickhousectl local use latest              # Latest release (installs if needed, recommended)
clickhousectl local use 26.5                # Latest 26.5.x.x (installs if needed)
clickhousectl local use 26.5.2.39           # Exact version
clickhousectl local use latest --no-global  # Set default but don't touch ~/.local/bin/clickhouse
clickhousectl local which                   # Show current default

# Remove a version
clickhousectl local remove 26.5.2.39
```

`local use` also creates a symlink at `~/.local/bin/clickhouse` pointing to the selected version's binary, so the plain `clickhouse` command (e.g. `clickhouse local`, `clickhouse client`) is on `PATH`. Pass `--no-global` to skip. If a regular file already exists at that path it is left alone with a warning. `local remove` of the active default version also clears the symlink.

#### ClickHouse binary storage {#binary-storage}

ClickHouse binaries are stored in a global repository, so they can be used by multiple projects without duplicating storage. Binaries are stored in `~/.clickhouse/`:

```bash
~/.clickhouse/
├── versions/
│   └── 26.5.2.39/
│       └── clickhouse
└── default              # tracks the active version
```

### Initializing a project {#initializing-project}

```bash
clickhousectl local init
```

`init` bootstraps your current working directory with a standard folder structure for your ClickHouse and Postgres project files. It is optional; you are welcome to use your own folder structure if preferred.

It creates the following structure:

```bash
clickhouse/
├── tables/                 # Table definitions (CREATE TABLE ...)
├── materialized_views/     # Materialized view definitions
├── queries/                # Saved queries
└── seed/                   # Seed data / INSERT statements

postgres/
├── tables/                 # Table definitions (CREATE TABLE ...)
├── views/                  # View definitions
├── functions/              # Function definitions
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

Start and manage ClickHouse server instances. Each server gets its own isolated data directory at `.clickhouse/servers/<name>/data/`.

```bash
# Start a server (runs in background by default)
clickhousectl local server start                          # Named "default"
clickhousectl local server start --name dev               # Named "dev"
clickhousectl local server start --version stable         # Use a specific version (installs if needed, doesn't change default)
clickhousectl local server start --foreground             # Run in foreground (-F / --fg)
clickhousectl local server start --http-port 8124 --tcp-port 9001  # Explicit ports
clickhousectl local server start --config-file querylog          # Apply a named custom config

# List all servers (running and stopped)
clickhousectl local server list
clickhousectl local server list --global                  # List servers across all projects

# Stop servers
clickhousectl local server stop default                   # Stop by name
clickhousectl local server stop default --global          # Stop from any project
clickhousectl local server stop-all                       # Stop all running servers

# Remove a stopped server and its data
clickhousectl local server remove test

# Write connection env vars to a .env file
clickhousectl local server dotenv                         # From "default" server → .env
clickhousectl local server dotenv --name dev              # From "dev" server → .env
clickhousectl local server dotenv --local                 # Write to .env.local instead
```

**Server naming:** Without `--name`, the first server is called "default". If "default" is already running, a random name is generated (e.g. "bold-crane"). Use `--name` for stable identities you can start/stop repeatedly.

**Ports:** Defaults are HTTP 8123 and TCP 9000. If these are already in use, free ports are automatically assigned and shown in the output. Use `--http-port` and `--tcp-port` to set explicit ports.

**Global server management:** Use `--global` with `list`, `stop`, and `stop-all` to operate across all projects system-wide. `server list --global` shows all running ClickHouse servers with a Project column indicating which directory each belongs to.

#### Custom config files for local servers {#custom-config-files}

Local servers start with sensible defaults, but sometimes you need to flip a setting. Drop a config file into `~/.clickhouse/configs/` and apply it by name when starting a server:

```bash
mkdir -p ~/.clickhouse/configs
cat > ~/.clickhouse/configs/querylog.yaml <<'EOF'
query_log:
    database: system
    table: query_log
EOF

# See which configs are available
clickhousectl local server configs

# Start a server with one applied
clickhousectl local server start --config-file querylog
```

The named file is **overlaid on top of ClickHouse's built-in defaults** (via `config.d`), so it only needs to contain the settings you want to change, and there's no need to reproduce a full config. Files can be `.xml`, `.yaml`, or `.yml`, and you can reference them by name with or without the extension.

#### Project-local data directory {#project-local-data}

All server data lives inside `.clickhouse/` in your project directory:

```bash
.clickhouse/
├── .gitignore              # auto-created, ignores everything
├── credentials.json        # cloud API credentials (if configured)
└── servers/
    ├── default/
    │   └── data/           # ClickHouse data files for "default" server
    └── dev/
        └── data/           # ClickHouse data files for "dev" server
```

Each named server has its own data directory, so servers are fully isolated from each other. Data persists between restarts. Stop and start a server by name to pick up where you left off. Use `clickhousectl local server remove <name>` to permanently delete a server's data.

### Running local Postgres {#local-postgres}

In addition to ClickHouse, `clickhousectl` can run and manage local Postgres instances. Local Postgres is Docker-backed, so Docker must be installed and running. Each instance is identified by its name and major version, so multiple Postgres versions can run side by side with separate data directories.

```bash
# Optionally pre-pull a Postgres image (supports 17, 18 and tags like 18-alpine)
clickhousectl local install postgres@18

# Start an instance (defaults to postgres:18 on port 5432)
clickhousectl local postgres start
clickhousectl local postgres start --name dev --version 17 --port 5433
clickhousectl local postgres start --user app --password s3cret --database myapp
clickhousectl local postgres start -e POSTGRES_INITDB_ARGS=--data-checksums

# Connect with psql
clickhousectl local postgres client --name dev
clickhousectl local postgres client --name dev --query "SELECT 1"

# Export connection variables to a .env file
clickhousectl local postgres dotenv --name dev

# Stop (preserves data) and remove (deletes data)
clickhousectl local postgres stop dev
clickhousectl local postgres remove dev
```

## Authentication {#authentication}

Authenticate to ClickHouse Cloud using API keys (recommended) or OAuth (browser-based).

If you don't have a ClickHouse Cloud account yet, `clickhousectl cloud auth signup` opens the sign-up page in your browser.

### API key/secret (recommended) {#api-key}

API keys are the recommended way to authenticate, especially when driving the CLI from an AI agent. You can [create scoped API keys](/cloud/manage/openapi) that grant only the permissions you choose (read-only or read/write), and each key is tied to a single organization. This makes it a safe, least-privilege way to give the CLI access.

```bash
# Non-interactive (CI-friendly)
clickhousectl cloud auth login --api-key YOUR_KEY --api-secret YOUR_SECRET

# Interactive prompt
clickhousectl cloud auth login --interactive
```

Credentials are saved to `.clickhouse/credentials.json` (project-local).

You can also use environment variables, either exported in your session:
```bash
export CLICKHOUSE_CLOUD_API_KEY=your-key
export CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

Or placed in a `.env` file in your current working directory:
```env
CLICKHOUSE_CLOUD_API_KEY=your-key
CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

Or pass credentials directly via flags on any command:
```bash
clickhousectl cloud --api-key KEY --api-secret SECRET ...
```

### OAuth login {#oauth-login}

```bash
clickhousectl cloud auth login
```

This opens your browser for authentication via the OAuth device flow. Tokens are saved to `.clickhouse/tokens.json` (project-local).

:::note
OAuth access is currently **read-only** and grants access to **all organizations you belong to**. For write access, or to scope the CLI to a single organization, [create a scoped API key](#api-key) instead.
:::

### Auth status and logout {#auth-status}

```bash
clickhousectl cloud auth status    # Show current auth state
clickhousectl cloud auth logout    # Clear all saved credentials (credentials.json & tokens.json)
```

Credential resolution order: CLI flags > `.clickhouse/credentials.json` > exported environment variables > `.env` file > OAuth tokens.

### Debugging which credential source was used {#debug-credentials}

Pass `--debug` to any `cloud` command to print the resolved credential source (and the API URL) to stderr before the command runs.

```bash
clickhousectl cloud --debug service list
# [debug] auth source: credentials file (.clickhouse/credentials.json)
# [debug] api url: https://api.clickhouse.cloud/v1
# ... normal output ...
```

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

# Create with GA request-only extras
clickhousectl cloud service create --name my-service \
  --tag env=prod \
  --enable-endpoint mysql \
  --private-preview-terms-checked \
  --enable-core-dumps true

# Start/stop a service
clickhousectl cloud service start <service-id>
clickhousectl cloud service stop <service-id>

# Run SQL over HTTP via the Query API (no local clickhouse binary needed)
clickhousectl cloud service query --name my-service --query "SELECT 1"
clickhousectl cloud service query --id <service-id> --query "SELECT count() FROM system.tables" --format JSONEachRow
clickhousectl cloud service query --name my-service --queries-file schema.sql   # "-" reads from stdin
clickhousectl cloud service query --name my-service --database mydb --query "SHOW TABLES"
echo "SELECT 1+1" | clickhousectl cloud service query --name my-service

# Update service metadata and patches
clickhousectl cloud service update <service-id> \
  --name my-renamed-service \
  --add-ip-allow 10.0.0.0/8 \
  --remove-ip-allow 0.0.0.0/0 \
  --add-private-endpoint-id pe-1 \
  --release-channel fast \
  --enable-endpoint mysql \
  --add-tag env=staging \
  --transparent-data-encryption-key-id tde-key-1 \
  --enable-core-dumps false

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
| `--data-warehouse-id` | Data warehouse ID (for read replicas) |
| `--readonly` | Make service read-only |
| `--encryption-key` | Customer disk encryption key |
| `--encryption-role` | Role ARN for disk encryption |
| `--enable-tde` | Enable Transparent Data Encryption |
| `--compliance-type` | Compliance: `hipaa`, `pci` |
| `--profile` | Instance profile (enterprise) |
| `--tag` | Attach a GA service tag (`key` or `key=value`) |
| `--enable-endpoint` / `--disable-endpoint` | Toggle GA service endpoints (currently `mysql`) |
| `--private-preview-terms-checked` | Accept private preview terms when required |
| `--enable-core-dumps` | Enable or disable service core dump collection |

#### Query API auth modes {#query-api-auth-modes}

`cloud service query` is the canonical way to run SQL against a cloud service over HTTP, with no `clickhouse` binary and no service password required. It works with both credential modes:

- **API key auth** (read + write SQL): the first time `cloud service query` runs against a service without a stored key, it provisions a Query API endpoint for that service and creates a dedicated API key bound to it. The key (`keyId`, `keySecret`, and `endpointId`) is stored in `.clickhouse/credentials.json` under `service_query_keys.<service-id>`. The key is scoped to a single service, so it can read and write (SELECT, INSERT, DDL) against that service but cannot reach any other service in the org. Pass `--no-auto-enable` to fail instead of provisioning.
- **OAuth** (`cloud auth login`): the query runs as your own identity, just like the web SQL-console. Your SQL permissions on the service are **read-only** when using OAuth. No Query API key is provisioned or stored. `--no-auto-enable` has no effect in this mode.

Querying an **idled** service wakes it automatically in both auth modes (the first query may take a minute). A **stopped** service is never woken: the query fails with a hint to run `cloud service start`. Set `CLICKHOUSE_CLOUD_QUERY_HOST` to override the derived Query API host.

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

### Postgres services {#postgres-services}

`clickhousectl` can also create and manage [ClickHouse Cloud Postgres](/cloud/managed-postgres) services, mirroring the ClickHouse service commands above.

```bash
# List and inspect
clickhousectl cloud postgres list
clickhousectl cloud postgres list --filter state=running
clickhousectl cloud postgres get <pg-id>

# Create a service
clickhousectl cloud postgres create \
  --name my-pg \
  --region us-east-1 \
  --size m7i.2xlarge \
  --pg-version 17 \
  --ha-type sync

# Update and delete
clickhousectl cloud postgres update <pg-id> --size m7i.4xlarge
clickhousectl cloud postgres update <pg-id> --add-tag env=prod --remove-tag legacy
clickhousectl cloud postgres delete <pg-id>

# Connection certificates
clickhousectl cloud postgres certs get <pg-id>                   # raw PEM to stdout
clickhousectl cloud postgres certs get <pg-id> --output ca.pem   # write to a file

# Configuration
clickhousectl cloud postgres config get <pg-id>
clickhousectl cloud postgres config replace <pg-id> --file cfg.json
clickhousectl cloud postgres config patch <pg-id> --set max_connections=500

# Reset the password
clickhousectl cloud postgres reset-password <pg-id> --generate

# Lifecycle: restart and high-availability promotion/switchover
clickhousectl cloud postgres restart <pg-id>
clickhousectl cloud postgres promote <pg-id>
clickhousectl cloud postgres switchover <pg-id>

# Read replicas and point-in-time restore
clickhousectl cloud postgres read-replica create <pg-id> --name replica-1
clickhousectl cloud postgres restore <pg-id> --name restored --restore-target 2026-04-16T12:00:00Z
```

#### Postgres service create options {#postgres-create-options}

| Option | Description |
|--------|-------------|
| `--name` | Service name (required) |
| `--region` | Region, e.g. `us-east-1` (required) |
| `--size` | Instance size, e.g. `m7i.2xlarge` (required) |
| `--provider` | Cloud provider (default: `aws`) |
| `--pg-version` | Major version: `18`, `17` |
| `--ha-type` | High availability: `none`, `async`, `sync` |
| `--tag` | Resource tag `key` or `key=value` (repeatable) |
| `--pg-config-file` | Path to a JSON file with a `PgConfig` object |
| `--pg-bouncer-config-file` | Path to a JSON file with a `PgBouncerConfig` object |

### Backups {#backups}

```bash
clickhousectl cloud backup list <service-id>
clickhousectl cloud backup get <service-id> <backup-id>
```

### ClickPipes {#clickpipes}

Manage ClickPipes for ingesting data into ClickHouse Cloud from external sources.

```bash
# List ClickPipes for a service
clickhousectl cloud clickpipe list <service-id>

# Get ClickPipe details
clickhousectl cloud clickpipe get <service-id> <clickpipe-id>

# Start/stop/resync a ClickPipe
clickhousectl cloud clickpipe start <service-id> <clickpipe-id>
clickhousectl cloud clickpipe stop <service-id> <clickpipe-id>
clickhousectl cloud clickpipe resync <service-id> <clickpipe-id>   # CDC pipes only

# Delete a ClickPipe
clickhousectl cloud clickpipe delete <service-id> <clickpipe-id>

# Update scaling
clickhousectl cloud clickpipe scale <service-id> <clickpipe-id> \
  --replicas 2 --cpu-millicores 250 --memory-gb 1

# Get/update settings
clickhousectl cloud clickpipe settings get <service-id> <clickpipe-id>
clickhousectl cloud clickpipe settings update <service-id> <clickpipe-id> \
  --streaming-max-insert-wait-ms 10000
```

#### Creating ClickPipes {#creating-clickpipes}

Each source type has its own subcommand under `clickpipe create`:

```bash
# From S3 / object storage
clickhousectl cloud clickpipe create object-storage <service-id> \
  --name my-s3-pipe \
  --source-url 'https://bucket.s3.us-east-1.amazonaws.com/data/**' \
  --format JSONEachRow \
  --database default --table events \
  --column "event_id:Int64" --column "name:String"

# From Google Cloud Storage (object storage)
clickhousectl cloud clickpipe create object-storage <service-id> \
  --name my-gcs-pipe \
  --storage-type gcs \
  --source-url 'https://storage.googleapis.com/bucket/data/**' \
  --format JSONEachRow \
  --service-account-file ./sa-key.json \
  --database default --table events \
  --column "event_id:Int64" --column "name:String"

# From Kafka / Redpanda / Confluent / MSK
clickhousectl cloud clickpipe create kafka <service-id> \
  --name my-kafka-pipe \
  --brokers 'broker:9092' --topics events \
  --format JSONEachRow \
  --kafka-type redpanda \
  --auth SCRAM-SHA-256 --username user --password pass \
  --ca-certificate ./ca.crt \
  --database default --table events \
  --column "event_id:Int64" --column "name:String"

# From Amazon Kinesis
clickhousectl cloud clickpipe create kinesis <service-id> \
  --name my-kinesis-pipe \
  --stream-name events --region us-east-1 \
  --format JSONEachRow \
  --auth IAM_USER --access-key-id AKIA... --secret-key ... \
  --database default --table events \
  --column "event_id:Int64" --column "name:String"

# From PostgreSQL (CDC)
clickhousectl cloud clickpipe create postgres <service-id> \
  --name my-pg-pipe \
  --host db.example.com --pg-database mydb \
  --username pguser --password pgpass \
  --table-mapping "public.users:public_users" \
  --table-mapping "public.orders:public_orders"

# From MySQL (CDC)
clickhousectl cloud clickpipe create mysql <service-id> \
  --name my-mysql-pipe \
  --host mysql.example.com \
  --username root --password pass \
  --table-mapping "mydb.users:mydb_users"

# From MongoDB (CDC)
clickhousectl cloud clickpipe create mongodb <service-id> \
  --name my-mongo-pipe \
  --uri 'mongodb+srv://cluster.example.net/mydb' \
  --username mongouser --password mongopass \
  --table-mapping "mydb.users:mydb_users"

# From BigQuery (snapshot)
clickhousectl cloud clickpipe create bigquery <service-id> \
  --name my-bq-pipe \
  --service-account-file ./sa-key.json \
  --staging-path gs://bucket/staging \
  --table-mapping "dataset.table:target_table"
```

Use `clickhousectl cloud clickpipe create <source> --help` for the full list of options per source type.

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

`clickhousectl` auto-detects coding-agent contexts (Claude Code, Cursor, Codex, Gemini CLI, Goose, Devin, and any tool that sets the standard `AGENT` env var) and emits JSON to stdout automatically without setting `--json`.

### Exit codes {#exit-codes}

Exit codes follow the `gh` CLI conventions:

| Code | Meaning |
| ---- | ------- |
| `0` | Success |
| `1` | Error (anything not classified below) |
| `2` | Cancelled (user aborted) |
| `4` | Auth required (no credentials, 401/403, OAuth-only writes) |

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

## Self-update {#self-update}

`clickhousectl` can update itself to the latest release:

```bash
# Update to the latest version
clickhousectl update

# Check for updates without installing
clickhousectl update --check
```

<<<<<<< HEAD
The CLI also checks for updates in the background (at most once per 24 hours) and displays a notice when a newer version is available.
=======
Connect to `localhost` on port 9000 in multiline mode.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Connect to `localhost` using port 9000 as the user `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Connect to `localhost` on port 9000 and default to the `my_database` database.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Connect to `localhost` on port 9000 and default to the `my_database` database specified in the connection string and a secure connection using the shorthanded `s` parameter.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Connect to the default host using the default port, the default user, and the default database.

```bash
clickhouse-client clickhouse:
```

Connect to the default host using the default port, as the user `my_user` and no password.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Connect to `localhost` using the email as the user name. `@` symbol is percent encoded to `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Connect to one of two hosts: `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

## Query ID format {#query-id-format}

In interactive mode ClickHouse Client shows the query ID for every query. By default, the ID is formatted like this:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

A custom format may be specified in a configuration file inside a `query_id_formats` tag. The `{query_id}` placeholder in the format string is replaced with the query ID. Several format strings are allowed inside the tag.
This feature can be used to generate URLs to facilitate profiling of queries.

**Example**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

With the configuration above, the ID of a query is shown in the following format:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

## Configuration files {#configuration_files}

ClickHouse Client uses the first existing file of the following:

- A file that is defined with the `-c [ -C, --config, --config-file ]` parameter.
- `./clickhouse-client.[xml|yaml|yml]`
- `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (or `~/.config/clickhouse/config.[xml|yaml|yml]` if `XDG_CONFIG_HOME` is not set)
- `~/.clickhouse-client/config.[xml|yaml|yml]`
- `/etc/clickhouse-client/config.[xml|yaml|yml]`

See the sample configuration file in the ClickHouse repository: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>
  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

## Environment variable options {#environment-variable-options}

The user name, password and host can be set via environment variables `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` and `CLICKHOUSE_HOST`.
Command line arguments `--user`, `--password` or `--host`, or a [connection string](#connection_string) (if specified) take precedence over environment variables.

## Command-line options {#command-line-options}

All command-line options can be specified directly on the command line or as defaults in the [configuration file](#configuration_files).

### General options {#command-line-options-general}

| Option                                              | Description                                                                                                                        | Default                      |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `-c [ -C, --config, --config-file ] <path-to-file>` | The location of the configuration file for the client, if it is not at one of the default locations. See [Configuration Files](#configuration_files). | -                            |
| `--help`                                            | Print usage summary and exit. Combine with `--verbose` to display all possible options including query settings.                  | -                            |
| `--history_file <path-to-file>`                     | Path to a file containing the command history.                                                                                     | -                            |
| `--history_max_entries`                             | Maximum number of entries in the history file.                                                                                     | `1000000` (1 million)        |
| `--prompt <prompt>`                                 | Specify a custom prompt.                                                                                                           | The `display_name` of the server |
| `--verbose`                                         | Increase output verbosity.                                                                                                         | -                            |
| `-V [ --version ]`                                  | Print version and exit.                                                                                                            | -                            |

### Connection options {#command-line-options-connection}

| Option                           | Description                                                                                                                                                                                                                                                                                                                        | Default                                                                                                          |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `--connection <name>`            | The name of preconfigured connection details from the configuration file. See [Connection credentials](#connection-credentials).                                                                                                                                                                                                   | -                                                                                                                |
| `-d [ --database ] <database>`   | Select the database to default to for this connection.                                                                                                                                                                                                                                                                             | The current database from the server settings (`default` by default)                                             |
| `-h [ --host ] <host>`           | The hostname of the ClickHouse server to connect to. Can either be a hostname or an IPv4 or IPv6 address. Multiple hosts can be passed via multiple arguments.                                                                                                                                                                    | `localhost`                                                                                                      |
| `--jwt <value>`                  | Use JSON Web Token (JWT) for authentication. <br/><br/>Server JWT authorization is only available in ClickHouse Cloud.                                                                                                                                                                                                            | -                                                                                                                |
| `--jwt-command <command>`        | Shell command whose stdout is used as the JWT. Invoked on the first connect, before reconnects when the cached JWT is near expiry, and after the server rejects the cached token. See [`--jwt-command` details](#jwt-command-details) below.                                                                                       | -                                                                                                                |
| `--jwt-command-timeout <seconds>` | Timeout for `--jwt-command`. Also settable as `<jwt-command-timeout>` in the config file; CLI wins.                                                                                                                                                                                                                                  | `30`                                                                                                            |
| `--login[=<mode>]`       | Authenticate via OAuth2. Bare `--login` (no `=<mode>`) triggers ClickHouse Cloud automatic login — the provider is inferred from the server. To authenticate against a custom OpenID Connect provider, supply a `mode` and `--oauth-credentials`: `--login=browser` runs the Authorization Code + PKCE flow (opens a browser), `--login=device` runs the Device Authorization flow (prints a URL and short code — no browser needed). | -                                                                                                                |
| `--oauth-credentials <path>` | Path to an OAuth2 credentials JSON file (Google Cloud Console format). Required when using `--login=browser` or `--login=device` with a custom OpenID Connect provider. See [OAuth credentials file format](#oauth-credentials-file) below. Refresh tokens are cached in `~/.clickhouse-client/oauth_cache.json` (mode `0600`). | `~/.clickhouse-client/oauth_client.json`                                                                        |
| `--no-warnings`                  | Disable showing warnings from `system.warnings` when the client connects to the server.                                                                                                                                                                                                                                            | -                                                                                                                |
| `--no-server-client-version-message`                  | Suppress server-client version mismatch message when the client connects to the server.                                                                                                                                                                                                                                            | -                                                                                                                |
| `--password <password>`          | The password of the database user. You can also specify the password for a connection in the configuration file. If you do not specify the password, the client will ask for it.                                                                                                                                                   | -                                                                                                                |
| `--port <port>`                  | The port the server is accepting connections on. The default ports are 9440 (TLS) and 9000 (no TLS). <br/><br/>Note: The client uses the native protocol and not HTTP(S).                                                                                                                                                         | `9440` if `--secure` is specified, `9000` otherwise. Always defaults to `9440` if the hostname ends in `.clickhouse.cloud`. |
| `-s [ --secure ]`                | Whether to use TLS. <br/><br/>Enabled automatically when connecting to port 9440 (the default secure port) or ClickHouse Cloud. <br/><br/>You might need to configure your CA certificates in the [configuration file](#configuration_files). The available configuration settings are the same as for [server-side TLS configuration](../operations/server-configuration-parameters/settings.md#openssl). | Auto-enabled when connecting to port 9440 or ClickHouse Cloud                                                   |
| `--ssh-key-file <path-to-file>`  | File containing the SSH private key for authenticate with the server.                                                                                                                                                                                                                                                              | -                                                                                                                |
| `--ssh-key-passphrase <value>`   | Passphrase for the SSH private key specified in `--ssh-key-file`.                                                                                                                                                                                                                                                                 | -                                                                                                                |
| `--tls-sni-override <server name>`       | If using TLS, the server name (SNI) to pass in the handshake.                                                                                                                                                                                                                                                                                                    | The host provided via `-h` or `--host`.                                                                                                        |
| `-u [ --user ] <username>`       | The database user to connect as.                                                                                                                                                                                                                                                                                                   | `default`                                                                                                        |

:::note
Instead of the `--host`, `--port`, `--user` and `--password` options, the client also supports [connection strings](#connection_string).
:::

### OAuth credentials file {#oauth-credentials-file}

When using `--login=browser` or `--login=device` with a custom OpenID Connect provider, the client reads a credentials JSON file. The file uses the same format produced by the Google Cloud Console ("OAuth 2.0 Client IDs" → "Download JSON"):

```json
{
  "installed": {
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "redirect_uris": ["http://127.0.0.1"]
  }
}
```

The top-level key can be `installed` (desktop/CLI apps) or `web`. Required fields: `client_id`, `auth_uri`, `token_uri`. Optional fields:

| Field | Description |
|---|---|
| `client_secret` | Confidential-client secret. Omit (or leave empty) for OIDC public clients — the auth-code flow is always protected by PKCE and the device flow by the device code, so a secret is not required by the protocol. When the field is absent the client never sends a `client_secret` form parameter, which is the form public-client registrations require (Auth0, Microsoft Entra ID, Keycloak, Okta and others reject empty secrets with `invalid_client`). |
| `device_authorization_uri` | Device authorization endpoint. Discovered automatically via OIDC Discovery if absent. |
| `issuer` | OIDC issuer URL (e.g. `https://accounts.google.com`). Used to locate the discovery document when `device_authorization_uri` is not set. |

The default path is `~/.clickhouse-client/oauth_client.json`. Override it with `--oauth-credentials <path>`.

After a successful login the obtained refresh token is cached in `~/.clickhouse-client/oauth_cache.json` (file mode `0600`). Subsequent runs reuse the cached token silently and only open the browser or print a device code when the refresh token has expired.

### `--jwt-command` details {#jwt-command-details}

The command is executed via `/bin/sh -c`. Stdout is taken as the JWT (one trailing newline stripped); any human-facing output (prompts, URLs, device codes) must go to stderr — it is forwarded unbuffered to the client's stderr. Stdin is closed.

The command runs on the first connect to obtain the initial token. On subsequent (re)connects the client reuses the cached token; it re-invokes the command only when (a) the cached token parses as a JWT whose `exp` claim is within 30 seconds, or (b) the server rejects the cached token with an authentication failure, in which case the client refetches the token and retries the handshake once. Opaque tokens (anything that does not parse as a JWT) and JWTs without a usable `exp` claim are reused until the server rejects them — caching/refresh in those cases is the script's responsibility.

```bash
clickhouse-client --jwt-command "curl -sS https://idp.example/token | jq -r .access_token"
```

Cannot be combined with `--jwt`, `--login`, or a non-default `--user`. Non-zero exit, empty output, or exceeding `--jwt-command-timeout` (default `30`s, overridable via `<jwt-command-timeout>` in `~/.clickhouse-client/config.xml`) fails authentication. On timeout the entire helper subprocess tree is terminated.

### Query options {#command-line-options-query}

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--param_<name>=<value>`        | Substitution value for a parameter of a [query with parameters](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                    |
| `-q [ --query ] <query>`        | The query to run in batch mode. Can be specified multiple times (`--query "SELECT 1" --query "SELECT 2"`) or once with multiple semicolon-separated queries (`--query "SELECT 1; SELECT 2;"`). In the latter case, `INSERT` queries with formats other than `VALUES` must be separated by empty lines. <br/><br/>A single query can also be specified without a parameter: `clickhouse-client "SELECT 1"` <br/><br/>Cannot be used together with `--queries-file`.                               |
| `--queries-file <path-to-file>` | Path to a file containing queries. `--queries-file` can be specified multiple times, e.g. `--queries-file queries1.sql --queries-file queries2.sql`. <br/><br/>Cannot be used together with `--query`.                                                                                                                                                                                                                                                                                            |
| `-m [ --multiline ]`            | If specified, allow multiline queries (do not send the query on Enter). Queries will be sent only when they are ended with a semicolon.                                                                                                                                                                                                                                                                                                                                                           |

### Query settings {#command-line-options-query-settings}

Query settings can be specified as command-line options in the client, for example:
```bash
$ clickhouse-client --max_threads 1
```

See [Settings](../operations/settings/settings.md) for a list of settings.

### Formatting options {#command-line-options-formatting}

| Option                    | Description                                                                                                                                                                                                                   | Default        |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `-f [ --format ] <format>` | Use the specified format to output the result. <br/><br/>See [Formats for Input and Output Data](formats.md) for a list of supported formats.                                                                                | `TabSeparated` |
| `--pager <command>`       | Pipe all output into this command. Typically `less` (e.g., `less -S` to display wide result sets) or similar.                                                                                                                | -              |
| `-E [ --vertical ]`       | Use the [Vertical format](/interfaces/formats/Vertical) to output the result. This is the same as `–-format Vertical`. In this format, each value is printed on a separate line, which is helpful when displaying wide tables. | -              |

### Execution details {#command-line-options-execution-details}

| Option                            | Description                                                                                                                                                                                                                                                                                                         | Default                                                             |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `--enable-progress-table-toggle`  | Enable toggling of the progress table by pressing the control key (Space). Only applicable in interactive mode with progress table printing enabled.                                                                                                                                                                | `enabled`                                                           |
| `--hardware-utilization`          | Print hardware utilization information in progress bar.                                                                                                                                                                                                                                                             | -                                                                   |
| `--memory-usage`                  | If specified, print memory usage to `stderr` in non-interactive mode. <br/><br/>Possible values: <br/>• `none` - do not print memory usage <br/>• `default` - print number of bytes <br/>• `readable` - print memory usage in human-readable format                                                                | -                                                                   |
| `--print-profile-events`          | Print `ProfileEvents` packets.                                                                                                                                                                                                                                                                                      | -                                                                   |
| `--progress`                      | Print progress of query execution. <br/><br/>Possible values: <br/>• `tty\|on\|1\|true\|yes` - outputs to the terminal in interactive mode <br/>• `err` - outputs to `stderr` in non-interactive mode <br/>• `off\|0\|false\|no` - disables progress printing                                                       | `tty` in interactive mode, `off` in non-interactive (batch) mode    |
| `--progress-table`                | Print a progress table with changing metrics during query execution. <br/><br/>Possible values: <br/>• `tty\|on\|1\|true\|yes` - outputs to the terminal in interactive mode <br/>• `err` - outputs to `stderr` non-interactive mode <br/>• `off\|0\|false\|no` - disables the progress table                      | `tty` in interactive mode, `off` in non-interactive (batch) mode    |
| `--stacktrace`                    | Print stack traces of exceptions.                                                                                                                                                                                                                                                                                   | -                                                                   |
| `-t [ --time ]`                   | Print query execution time to `stderr` in non-interactive mode (for benchmarks).                                                                                                                                                                                                                                    | -                                                                   |
>>>>>>> 40a2b77fcc6 (Merge pull request #1809 from Altinity/feature/antalya-26.3/oauth-executable-token-in-client)
