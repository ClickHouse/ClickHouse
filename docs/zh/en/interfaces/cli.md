---
description: 'clickhousectl 文档，ClickHouse 的命令行客户端：本地版与云端'
sidebar_label: 'clickhousectl'
sidebar_position: 17
slug: /interfaces/cli
title: 'clickhousectl'
doc_type: 'reference'
---

`clickhousectl` 是 ClickHouse 的命令行客户端，适用于本地版和云端。

使用 `clickhousectl`，你可以：

* 安装并管理本地 ClickHouse 版本
* 启动并管理本地 ClickHouse 服务器
* 运行并管理本地 Postgres 实例
* 对 ClickHouse 服务器执行查询
* 配置 ClickHouse Cloud，并创建由云托管的 ClickHouse 集群
* 创建并管理 ClickHouse Cloud Postgres 服务
* 管理 ClickHouse Cloud 资源
* 创建并管理用于数据摄取的 ClickPipes (S3、Kafka、Kinesis、Postgres、MySQL、MongoDB、BigQuery)
* 将官方 ClickHouse agent skills 安装到受支持的编码智能体中
* 将本地 ClickHouse 开发环境推送到云端

`clickhousectl` 可帮助开发者和 AI 智能体使用 ClickHouse 进行开发。

<div id="installation">
  ## 安装
</div>

<div id="quick-install">
  ### 快速安装
</div>

```bash
curl https://clickhouse.com/cli | sh
```

安装脚本会下载适用于你所用操作系统的正确版本，并将其安装到 `~/.local/bin/clickhousectl`。为方便使用，还会自动创建一个 `chctl` 别名。

<div id="requirements">
  ## 要求
</div>

* macOS (aarch64、x86&#95;64) 或 Linux (aarch64、x86&#95;64)
* Cloud 命令需要 [ClickHouse Cloud API 密钥](/zh/cloud/manage/api/api-overview)

<div id="local">
  ## 本地
</div>

<div id="installing-versions">
  ### 安装和管理 ClickHouse 版本
</div>

`clickhousectl` 会从 `builds.clickhouse.com` 下载 ClickHouse 二进制文件；如果那里没有可用构建，则会改为从 `packages.clickhouse.com` (Linux) 或 [GitHub releases](https://github.com/ClickHouse/ClickHouse/releases) (macOS) 下载。

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

`local use` 还会在 `~/.local/bin/clickhouse` 创建一个指向所选版本二进制可执行文件的符号链接，因此可直接使用 `clickhouse` 命令 (例如 `clickhouse local`、`clickhouse client`) ，并确保其在 `PATH` 中。传入 `--no-global` 可跳过此操作。如果该路径下已存在普通文件，则会保留该文件不变，并发出警告。对当前生效的默认版本执行 `local remove` 也会清除该符号链接。

<div id="binary-storage">
  #### ClickHouse 二进制文件存储
</div>

ClickHouse 二进制文件存放在全局仓库中，因此多个项目可以共用这些文件，而无需重复占用存储空间。二进制文件存放在 `~/.clickhouse/` 中：

```bash
~/.clickhouse/
├── versions/
│   └── 26.5.2.39/
│       └── clickhouse
└── default              # tracks the active version
```

<div id="initializing-project">
  ### 项目初始化
</div>

```bash
clickhousectl local init
```

`init` 会在你当前的工作目录中创建一套适用于 ClickHouse 和 Postgres 项目文件的标准目录结构。这一步是可选的；如果你愿意，也可以使用自己偏好的目录结构。

它会创建以下结构：

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

<div id="running-queries">
  ### 执行查询
</div>

```bash
# Connect to a running server with clickhouse-client
clickhousectl local client                           # Connects to "default" server
clickhousectl local client --name dev                # Connects to "dev" server
clickhousectl local client --query "SHOW DATABASES"  # Run a query
clickhousectl local client --queries-file schema.sql # Run queries from a file
clickhousectl local client --host remote-host --port 9000  # Connect to a specific host/port
```

<div id="managing-servers">
  ### 创建和管理 ClickHouse 服务器
</div>

启动和管理 ClickHouse 服务器实例。每个服务器都会在 `.clickhouse/servers/<name>/data/` 下拥有各自独立的数据目录。

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

**服务器命名：** 不使用 `--name` 时，第一个服务器名为 &quot;default&quot;。如果 &quot;default&quot; 已在运行，则会随机生成一个名称 (例如 &quot;bold-crane&quot;) 。如果需要可反复启动/停止的稳定标识，请使用 `--name`。

**端口：** 默认端口为 HTTP 8123 和 TCP 9000。如果这些端口已被占用，系统会自动分配空闲端口并在输出中显示。使用 `--http-port` 和 `--tcp-port` 可指定端口。

**全局服务器管理：** 将 `--global` 与 `list`、`stop` 和 `stop-all` 搭配使用，即可在系统范围内跨所有项目进行操作。`server list --global` 会显示所有正在运行的 ClickHouse 服务器，并带有一个 Project 列，用于标明每个服务器所属的目录。

<div id="custom-config-files">
  #### 本地服务器的自定义配置文件
</div>

本地服务器启动时会采用合理的默认配置，但有时你也需要修改某项设置。将配置文件放入 `~/.clickhouse/configs/`，并在启动服务器时按名称应用：

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

该命名文件会**覆盖在 ClickHouse 的内置默认配置之上** (通过 `config.d`) ，因此其中只需包含你想要修改的设置，无需提供完整的 config。文件可以是 `.xml`、`.yaml` 或 `.yml`，引用时既可以带扩展名，也可以不带。

<div id="project-local-data">
  #### 项目本地数据目录
</div>

所有服务器数据都存放在项目目录下的 `.clickhouse/` 中：

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

每个具名服务器都有自己的数据目录，因此服务器之间完全相互隔离。数据在重启后仍会保留。按名称停止并重新启动服务器，即可从上次停下的地方继续。使用 `clickhousectl local server remove <name>` 可永久删除该服务器的数据。

<div id="local-postgres">
  ### 运行本地 Postgres
</div>

除了 ClickHouse，`clickhousectl` 还可以运行和管理本地 Postgres 实例。本地 Postgres 基于 Docker 运行，因此必须先安装并启动 Docker。每个实例通过其名称和主版本号来标识，因此可以让多个 Postgres 版本并存运行，并分别使用独立的数据目录。

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

<div id="authentication">
  ## 身份验证
</div>

使用 API 密钥 (推荐) 或 OAuth (基于浏览器) 对 ClickHouse Cloud 进行身份验证。

如果你还没有 ClickHouse Cloud 账户，`clickhousectl cloud auth signup` 会在浏览器中打开注册页面。

<div id="api-key">
  ### API key/secret (推荐)
</div>

API key 是推荐的身份验证方式，尤其是在通过 AI 智能体调用命令行客户端时。你可以[创建限定作用域的 API key](/zh/cloud/manage/openapi)，只授予你所需的权限 (只读或读/写) ，而且每个 key 都只绑定到一个组织。这样可以用一种安全、遵循最小权限原则的方式，为命令行客户端提供访问权限。

```bash
# Non-interactive (CI-friendly)
clickhousectl cloud auth login --api-key YOUR_KEY --api-secret YOUR_SECRET

# Interactive prompt
clickhousectl cloud auth login --interactive
```

凭据会保存到 `.clickhouse/credentials.json` (项目本地) 。

你也可以使用环境变量，可以在当前会话中导出：

```bash
export CLICKHOUSE_CLOUD_API_KEY=your-key
export CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

或者将其放在当前工作目录下的 `.env` 文件中：

```env
CLICKHOUSE_CLOUD_API_KEY=your-key
CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

或者在任何命令中通过 flag 直接传入凭据：

```bash
clickhousectl cloud --api-key KEY --api-secret SECRET ...
```

<div id="oauth-login">
  ### OAuth 登录
</div>

```bash
clickhousectl cloud auth login
```

这将打开浏览器，通过 OAuth 设备流程完成身份验证。令牌会保存到 `.clickhouse/tokens.json` (项目本地) 。

:::note
OAuth 访问目前仅为**只读**，并且可访问你所属的**所有组织**。如果需要写入权限，或希望将命令行客户端限定为仅访问单个组织，请改为[创建一个限定作用域的 API key](#api-key)。
:::

<div id="auth-status">
  ### 认证状态和退出登录
</div>

```bash
clickhousectl cloud auth status    # Show current auth state
clickhousectl cloud auth logout    # Clear all saved credentials (credentials.json & tokens.json)
```

凭据解析顺序：命令行客户端参数 &gt; `.clickhouse/credentials.json` &gt; 已导出的环境变量 &gt; `.env` 文件 &gt; OAuth 令牌。

<div id="debug-credentials">
  ### 调试所使用的凭证来源
</div>

向任意 `cloud` 命令传入 `--debug`，即可在命令运行前将已解析的凭证来源 (以及 API URL) 打印到 stderr。

```bash
clickhousectl cloud --debug service list
# [debug] auth source: credentials file (.clickhouse/credentials.json)
# [debug] api url: https://api.clickhouse.cloud/v1
# ... normal output ...
```

<div id="cloud">
  ## Cloud
</div>

使用 API 管理 ClickHouse Cloud 服务。

<div id="organizations">
  ### 组织
</div>

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

<div id="services">
  ### 服务
</div>

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

<div id="service-create-options">
  #### 服务创建选项
</div>

| 选项                                         | 描述                                   |
| ------------------------------------------ | ------------------------------------ |
| `--name`                                   | 服务名称 (必填)                            |
| `--provider`                               | 云提供商：`aws`、`gcp`、`azure` (默认：`aws`)  |
| `--region`                                 | 区域 (默认：`us-east-1`)                  |
| `--min-replica-memory-gb`                  | 每个副本的最小内存 (GB)  (8–356，且必须为 4 的倍数)   |
| `--max-replica-memory-gb`                  | 每个副本的最大内存 (GB)  (8–356，且必须为 4 的倍数)   |
| `--num-replicas`                           | 副本数 (1–20)                           |
| `--idle-scaling`                           | 允许扩缩容到零 (默认：`true`)                  |
| `--idle-timeout-minutes`                   | 最短空闲超时时间 (分钟)  (&gt;= 5)             |
| `--ip-allow`                               | 允许的 IP CIDR (可重复指定，默认：`0.0.0.0/0`)   |
| `--backup-id`                              | 用于恢复的 Backup ID                      |
| `--release-channel`                        | 发布渠道：`slow`、`default`、`fast`         |
| `--data-warehouse-id`                      | 数据仓库 ID (用于只读副本)                     |
| `--readonly`                               | 将服务设为只读                              |
| `--encryption-key`                         | 客户提供的磁盘加密密钥                          |
| `--encryption-role`                        | 用于磁盘加密的角色 ARN                        |
| `--enable-tde`                             | 启用 Transparent Data Encryption       |
| `--compliance-type`                        | 合规类型：`hipaa`、`pci`                   |
| `--profile`                                | 实例 profile (Enterprise)              |
| `--tag`                                    | 附加 GA 服务标签 (`key` 或 `key=value`)     |
| `--enable-endpoint` / `--disable-endpoint` | 启用或禁用 GA 服务端点 (当前仅支持 `mysql`)        |
| `--private-preview-terms-checked`          | 在需要时接受私有预览条款                         |
| `--enable-core-dumps`                      | 启用或禁用服务核心转储收集                        |

<div id="query-api-auth-modes">
  #### Query API 认证模式
</div>

`cloud service query` 是通过 HTTP 对云服务执行 SQL 的规范方式，无需 `clickhouse` 可执行文件，也不需要服务密码。它支持以下两种凭据模式：

* **API key 认证** (可读写 SQL) ：当 `cloud service query` 首次对某个尚未存储密钥的服务运行时，它会为该服务预配一个 Query API 端点，并创建一个绑定到该端点的专用 API key。该密钥 (`keyId`、`keySecret` 和 `endpointId`) 会存储在 `.clickhouse/credentials.json` 的 `service_query_keys.<service-id>` 下。该密钥仅作用于单个服务，因此可以对该服务执行读写操作 (SELECT、INSERT、DDL) ，但无法访问组织中的任何其他服务。传入 `--no-auto-enable` 可使其直接失败，而不是进行预配。
* **OAuth** (`cloud auth login`) ：查询会以你自己的身份运行，就像在 Web SQL 控制台中一样。使用 OAuth 时，你在该服务上的 SQL 权限为 **只读**。不会预配或存储 Query API key。`--no-auto-enable` 在此模式下无效。

在这两种认证模式下，查询处于 **休眠** 状态的服务都会自动将其唤醒 (第一次查询可能需要一分钟) 。而 **已停止** 的服务绝不会被唤醒：查询会失败，并提示你运行 `cloud service start`。设置 `CLICKHOUSE_CLOUD_QUERY_HOST` 可覆盖自动推导出的 Query API host。

<div id="query-endpoints">
  #### 查询端点管理
</div>

```bash
clickhousectl cloud service query-endpoint get <service-id>
clickhousectl cloud service query-endpoint create <service-id> \
  --role admin \
  --open-api-key key-1 \
  --allowed-origins https://app.example.com
clickhousectl cloud service query-endpoint delete <service-id>
```

<div id="private-endpoints">
  #### 专用终结点管理
</div>

```bash
clickhousectl cloud service private-endpoint create <service-id> --endpoint-id vpce-123
clickhousectl cloud service private-endpoint get-config <service-id>
```

<div id="backup-config">
  #### Backup 配置
</div>

```bash
clickhousectl cloud service backup-config get <service-id>
clickhousectl cloud service backup-config update <service-id> \
  --backup-period-hours 24 \
  --backup-retention-period-hours 720 \
  --backup-start-time 02:00
```

<div id="postgres-services">
  ### Postgres 服务
</div>

`clickhousectl` 也可以创建和管理 [ClickHouse Cloud Postgres](/zh/cloud/managed-postgres) 服务，其命令与上方的 ClickHouse 服务命令相对应。

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

<div id="postgres-create-options">
  #### Postgres service 创建选项
</div>

| 选项                         | 说明                                 |
| -------------------------- | ---------------------------------- |
| `--name`                   | 服务名 (必填)                           |
| `--region`                 | 区域，例如 `us-east-1` (必填)             |
| `--size`                   | 实例规格，例如 `m7i.2xlarge` (必填)         |
| `--provider`               | 云提供商 (默认：`aws`)                    |
| `--pg-version`             | 主版本：`18`、`17`                      |
| `--ha-type`                | 高可用性：`none`、`async`、`sync`         |
| `--tag`                    | 资源标签 `key` 或 `key=value` (可重复)     |
| `--pg-config-file`         | 包含 `PgConfig` 对象的 JSON 文件路径        |
| `--pg-bouncer-config-file` | 包含 `PgBouncerConfig` 对象的 JSON 文件路径 |

<div id="backups">
  ### 备份
</div>

```bash
clickhousectl cloud backup list <service-id>
clickhousectl cloud backup get <service-id> <backup-id>
```

<div id="clickpipes">
  ### ClickPipes
</div>

管理 ClickPipes，用于将外部来源的数据摄取到 ClickHouse Cloud。

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

<div id="creating-clickpipes">
  #### 创建 ClickPipes
</div>

每种源类型在 `clickpipe create` 下都有对应的子命令：

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

使用 `clickhousectl cloud clickpipe create <source> --help` 查看各源类型的完整选项列表。

<div id="members">
  ### 成员
</div>

```bash
clickhousectl cloud member list
clickhousectl cloud member get <user-id>
clickhousectl cloud member update <user-id> --role-id <role-id>
clickhousectl cloud member remove <user-id>
```

<div id="invitations">
  ### 邀请
</div>

```bash
clickhousectl cloud invitation list
clickhousectl cloud invitation create --email dev@example.com --role-id <role-id>
clickhousectl cloud invitation get <invitation-id>
clickhousectl cloud invitation delete <invitation-id>
```

<div id="keys">
  ### 密钥
</div>

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

<div id="activity">
  ### Activity
</div>

```bash
clickhousectl cloud activity list --from-date 2024-01-01 --to-date 2024-12-31
clickhousectl cloud activity get <activity-id>
```

<div id="json-output">
  ### JSON 输出
</div>

使用 `--json` 标志以输出 JSON 格式的响应。

```bash
clickhousectl cloud --json service list
clickhousectl cloud --json service get <service-id>
```

`clickhousectl` 会自动检测 coding-agent 上下文 (Claude Code、Cursor、Codex、Gemini CLI、Goose、Devin，以及任何设置了标准 `AGENT` 环境变量的工具) ，并自动将 JSON 输出到 stdout，无需设置 `--json`。

<div id="exit-codes">
  ### 退出代码
</div>

退出代码遵循 `gh` 命令行客户端的约定：

| 代码  | 含义                                  |
| --- | ----------------------------------- |
| `0` | 成功                                  |
| `1` | 错误 (任何未归入下列类别的情况)                   |
| `2` | 已取消 (用户中止)                          |
| `4` | 需要身份验证 (无凭据、401/403、仅支持 OAuth 的写入)  |

<div id="skills">
  ## 技能
</div>

从 [ClickHouse/agent-skills](https://github.com/ClickHouse/agent-skills) 安装官方的 ClickHouse Agent Skills。

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

<div id="non-interactive-flags">
  ### 非交互式选项
</div>

| 选项                | 说明                   |
| ----------------- | -------------------- |
| `--agent <name>`  | 为指定智能体安装技能 (可重复指定)   |
| `--global`        | 使用全局作用域；若省略，则使用项目作用域 |
| `--all`           | 为所有受支持的智能体安装技能       |
| `--detected-only` | 为系统中已检测到的受支持智能体安装技能  |

<div id="self-update">
  ## 自行更新
</div>

`clickhousectl` 可以自行更新到最新版本：

```bash
# Update to the latest version
clickhousectl update

# Check for updates without installing
clickhousectl update --check
```

命令行客户端还会在后台检查更新 (最多每 24 小时一次) ，并在有新版本可用时显示提示。