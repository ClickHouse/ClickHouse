---
description: 'ClickHouse の CLI である clickhousectl のドキュメント: ローカルおよびクラウド'
sidebar_label: 'clickhousectl'
sidebar_position: 17
slug: /interfaces/cli
title: 'clickhousectl'
doc_type: 'reference'
---

`clickhousectl` は、ローカルおよびクラウド向けの ClickHouse CLI です。

`clickhousectl` では、次のことができます。

* ローカルの ClickHouse バージョンをインストールして管理する
* ローカルの ClickHouse サーバー を起動して管理する
* ローカルの Postgres インスタンスを起動して管理する
* ClickHouse サーバー に対してクエリを実行する
* ClickHouse Cloud をセットアップし、Cloud で管理される ClickHouse クラスターを作成する
* ClickHouse Cloud の Postgres service を作成して管理する
* ClickHouse Cloud リソースを管理する
* データインジェスト用の ClickPipes を作成して管理する (S3、Kafka、Kinesis、Postgres、MySQL、MongoDB、BigQuery)
* 対応するコーディングエージェントに公式の ClickHouse agent skills をインストールする
* ローカルの ClickHouse 開発環境をクラウドにプッシュする

`clickhousectl` は、人間と AI エージェントの ClickHouse を使った開発を支援します。

<div id="installation">
  ## インストール
</div>

<div id="quick-install">
  ### クイックインストール
</div>

```bash
curl https://clickhouse.com/cli | sh
```

インストールスクリプトは、お使いのOSに対応する適切なバージョンをダウンロードし、`~/.local/bin/clickhousectl` にインストールします。利便性のため、`chctl` のエイリアスも自動的に作成されます。

<div id="requirements">
  ## 要件
</div>

* macOS (aarch64, x86&#95;64) または Linux (aarch64, x86&#95;64)
* Cloud コマンドの実行には [ClickHouse Cloud API キー](/ja/cloud/manage/api/api-overview) が必要です

<div id="local">
  ## ローカル
</div>

<div id="installing-versions">
  ### ClickHouse バージョンのインストールと管理
</div>

`clickhousectl` は `builds.clickhouse.com` から ClickHouse バイナリをダウンロードし、そこに利用可能な build がない場合は、`packages.clickhouse.com` (Linux) または [GitHub releases](https://github.com/ClickHouse/ClickHouse/releases) (macOS) から取得します。

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

`local use` は、選択したバージョンのバイナリを指す `~/.local/bin/clickhouse` へのシンボリックリンクも作成するため、通常の `clickhouse` コマンド (例: `clickhouse local`、`clickhouse client`) を `PATH` から利用できるようになります。スキップするには `--no-global` を指定します。そのパスに通常のファイルがすでに存在する場合は、警告を表示してそのまま残します。アクティブなデフォルトバージョンに対して `local remove` を実行すると、このシンボリックリンクも削除されます。

<div id="binary-storage">
  #### ClickHouse バイナリの保存先
</div>

ClickHouse バイナリはグローバルなリポジトリに保存されるため、ストレージを重複させることなく複数のプロジェクトで利用できます。バイナリは `~/.clickhouse/` に保存されます:

```bash
~/.clickhouse/
├── versions/
│   └── 26.5.2.39/
│       └── clickhouse
└── default              # tracks the active version
```

<div id="initializing-project">
  ### プロジェクトの初期化
</div>

```bash
clickhousectl local init
```

`init` は、現在の作業ディレクトリを、ClickHouse と Postgres のプロジェクトファイル向けの標準的なフォルダ構成で初期化します。これは必須ではなく、必要に応じて独自のフォルダ構成を使用しても問題ありません。

次の構成が作成されます。

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
  ### クエリの実行
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
  ### ClickHouseサーバーの作成と管理
</div>

ClickHouseサーバーインスタンスを起動・管理します。各サーバーには、`.clickhouse/servers/<name>/data/` に個別に分離されたデータディレクトリが用意されます。

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

**サーバー名:** `--name` を指定しない場合、最初のサーバーの名前は &quot;default&quot; になります。&quot;default&quot; がすでに実行中の場合は、ランダムな名前 (例: &quot;bold-crane&quot;) が自動生成されます。繰り返し起動・停止する際に同じ識別名を使いたい場合は、`--name` を使用してください。

**ポート:** デフォルトは HTTP 8123 と TCP 9000 です。これらがすでに使用中の場合は、空いているポートが自動的に割り当てられ、出力に表示されます。ポートを明示的に指定するには、`--http-port` と `--tcp-port` を使用してください。

**グローバルなサーバー管理:** `list`、`stop`、`stop-all` とともに `--global` を使用すると、すべてのプロジェクトを対象にシステム全体で操作できます。`server list --global` を実行すると、実行中のすべての ClickHouse サーバーが表示され、各サーバーがどのディレクトリに属しているかを示す Project カラムも表示されます。

<div id="custom-config-files">
  #### ローカルサーバー向けのカスタム設定ファイル
</div>

ローカルサーバーは適切なデフォルト設定で起動しますが、設定を変更したい場合もあります。設定ファイルを `~/.clickhouse/configs/` に配置し、サーバーの起動時に名前を指定して適用します:

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

指定したファイルは、ClickHouse の**組み込みデフォルト設定の上に重ねて適用されます** (`config.d` 経由) 。そのため、変更したい設定だけを記述すればよく、完全な config を再現する必要はありません。ファイルは `.xml`、`.yaml`、`.yml` を使用でき、拡張子の有無にかかわらず名前で参照できます。

<div id="project-local-data">
  #### プロジェクトローカルのデータディレクトリ
</div>

すべてのサーバーデータは、プロジェクトディレクトリ内の `.clickhouse/` に保存されます。

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

名前付きサーバーごとに専用のデータディレクトリがあるため、サーバー同士は完全に分離されています。データは再起動しても保持されます。中断したところから再開するには、名前を指定してサーバーを停止し、再度起動します。サーバーのデータを完全に削除するには、`clickhousectl local server remove <name>` を使用します。

<div id="local-postgres">
  ### ローカル Postgres の実行
</div>

`clickhousectl` は、ClickHouse に加えてローカルの Postgres インスタンスも実行・管理できます。ローカル Postgres は Docker をバックエンドとしているため、Docker がインストールされており、実行中である必要があります。各インスタンスは名前とメジャーバージョンで識別されるため、複数の Postgres バージョンをそれぞれ別のデータディレクトリで並行して実行できます。

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
  ## 認証
</div>

ClickHouse Cloud には、API キー (推奨) または OAuth (ブラウザベース) で認証します。

まだ ClickHouse Cloud のアカウントをお持ちでない場合は、`clickhousectl cloud auth signup` を実行すると、ブラウザでサインアップページが開きます。

<div id="api-key">
  ### API キー/シークレット (推奨)
</div>

認証には API キーの使用を推奨します。特に、AIエージェントから CLI を操作する場合に適しています。[スコープ付き API キーを作成](/ja/cloud/manage/openapi)すると、選択した権限 (読み取り専用または読み取り/書き込み) のみを付与でき、各キーは 1 つの組織に紐づけられます。これにより、CLI にアクセスを許可する安全な最小権限の方法となります。

```bash
# Non-interactive (CI-friendly)
clickhousectl cloud auth login --api-key YOUR_KEY --api-secret YOUR_SECRET

# Interactive prompt
clickhousectl cloud auth login --interactive
```

認証情報は `.clickhouse/credentials.json` に保存されます (プロジェクトローカル) 。

環境変数を使用することもできます。たとえば、現在のセッションで `export` する場合は次のとおりです。

```bash
export CLICKHOUSE_CLOUD_API_KEY=your-key
export CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

または、現在の作業ディレクトリにある `.env` ファイルに記載します。

```env
CLICKHOUSE_CLOUD_API_KEY=your-key
CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

または、任意のコマンドでフラグを使って認証情報を直接指定します:

```bash
clickhousectl cloud --api-key KEY --api-secret SECRET ...
```

<div id="oauth-login">
  ### OAuth ログイン
</div>

```bash
clickhousectl cloud auth login
```

これにより、OAuth デバイスフローによる認証のためにブラウザーが開きます。トークンは `.clickhouse/tokens.json` (プロジェクトローカル) に保存されます。

:::note
OAuth アクセスは現在 **読み取り専用** で、**自分が所属するすべての組織** にアクセスできます。書き込みアクセスが必要な場合、または CLI の対象を単一の組織に限定したい場合は、代わりに[スコープ付き API キー を作成](#api-key)してください。
:::

<div id="auth-status">
  ### 認証状態とログアウト
</div>

```bash
clickhousectl cloud auth status    # Show current auth state
clickhousectl cloud auth logout    # Clear all saved credentials (credentials.json & tokens.json)
```

認証情報の解決順: CLIフラグ &gt; `.clickhouse/credentials.json` &gt; exportされた環境変数 &gt; `.env` ファイル &gt; OAuthトークン。

<div id="debug-credentials">
  ### 使用された認証情報ソースのデバッグ
</div>

任意の`cloud`コマンドに`--debug`を指定すると、コマンドの実行前に、解決された認証情報ソース (および API URL) が stderr に出力されます。

```bash
clickhousectl cloud --debug service list
# [debug] auth source: credentials file (.clickhouse/credentials.json)
# [debug] api url: https://api.clickhouse.cloud/v1
# ... normal output ...
```

<div id="cloud">
  ## Cloud
</div>

API を使用して ClickHouse Cloud サービスを管理します。

<div id="organizations">
  ### 組織
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
  ### サービス
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
  #### サービス作成オプション
</div>

| Option                                     | Description                                     |
| ------------------------------------------ | ----------------------------------------------- |
| `--name`                                   | サービス名 (必須)                                      |
| `--provider`                               | クラウドプロバイダー: `aws`、`gcp`、`azure` (デフォルト: `aws`)  |
| `--region`                                 | リージョン (デフォルト: `us-east-1`)                      |
| `--min-replica-memory-gb`                  | レプリカあたりの最小メモリ容量 (GB)  (8～356、4 の倍数)             |
| `--max-replica-memory-gb`                  | レプリカあたりの最大メモリ容量 (GB)  (8～356、4 の倍数)             |
| `--num-replicas`                           | レプリカ数 (1～20)                                    |
| `--idle-scaling`                           | ゼロまでのスケーリングを許可 (デフォルト: `true`)                  |
| `--idle-timeout-minutes`                   | 最小アイドルタイムアウト (分)  (&gt;= 5)                     |
| `--ip-allow`                               | 許可する IP CIDR (複数回指定可、デフォルト: `0.0.0.0/0`)        |
| `--backup-id`                              | 復元元のバックアップ ID                                   |
| `--release-channel`                        | リリースチャネル: `slow`、`default`、`fast`               |
| `--data-warehouse-id`                      | data warehouse ID (読み取りレプリカ用)                   |
| `--readonly`                               | サービスを読み取り専用にする                                  |
| `--encryption-key`                         | 顧客管理のディスク暗号化秘密鍵                                 |
| `--encryption-role`                        | ディスク暗号化用のロール ARN                                |
| `--enable-tde`                             | 透過的データ暗号化を有効にする                                 |
| `--compliance-type`                        | コンプライアンス: `hipaa`、`pci`                         |
| `--profile`                                | インスタンスプロファイル (enterprise)                       |
| `--tag`                                    | GA サービスタグを付与する (`key` または `key=value`)          |
| `--enable-endpoint` / `--disable-endpoint` | GA サービスエンドポイントを切り替える (現在は `mysql`)              |
| `--private-preview-terms-checked`          | 必要な場合はプライベートプレビューの利用条件に同意する                     |
| `--enable-core-dumps`                      | サービスのコアダンプ収集を有効または無効にする                         |

<div id="query-api-auth-modes">
  #### Query API の認証モード
</div>

`cloud service query` は、`clickhouse` バイナリやサービスのパスワードを使わずに、HTTP 経由で Cloud サービスに対して SQL を実行するための標準的な方法です。どちらの認証モードにも対応しています。

* **API キー auth** (SQL の読み取り + 書き込み) : 保存済みの key がない service に対して `cloud service query` を初めて実行すると、その service 用の Query API エンドポイントが Provisioning され、それに紐づく専用の API キー が作成されます。この key (`keyId`、`keySecret`、`endpointId`) は、`.clickhouse/credentials.json` の `service_query_keys.<service-id>` に保存されます。この key は単一の service のみにスコープされているため、その service に対しては読み取りと書き込み (SELECT、INSERT、DDL) ができますが、組織内の他の service にはアクセスできません。Provisioning を行わずに失敗させるには、`--no-auto-enable` を指定します。
* **OAuth** (`cloud auth login`) : クエリは、Web の SQL コンソールと同様に、あなた自身の identity として実行されます。OAuth 使用時の service に対する SQL permissions は **read-only** です。Query API キー が Provisioning または保存されることはありません。このモードでは `--no-auto-enable` は効果がありません。

**idled** 状態の service にクエリを実行すると、どちらの認証モードでも自動的に復帰します (最初のクエリには 1 分ほどかかる場合があります) 。**stopped** 状態の service は復帰せず、クエリは `cloud service start` を実行するよう示す hint とともに失敗します。導出された Query API host を override するには、`CLICKHOUSE_CLOUD_QUERY_HOST` を設定します。

<div id="query-endpoints">
  #### クエリエンドポイントの管理
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
  #### プライベート エンドポイントの管理
</div>

```bash
clickhousectl cloud service private-endpoint create <service-id> --endpoint-id vpce-123
clickhousectl cloud service private-endpoint get-config <service-id>
```

<div id="backup-config">
  #### バックアップの設定
</div>

```bash
clickhousectl cloud service backup-config get <service-id>
clickhousectl cloud service backup-config update <service-id> \
  --backup-period-hours 24 \
  --backup-retention-period-hours 720 \
  --backup-start-time 02:00
```

<div id="postgres-services">
  ### Postgres サービス
</div>

`clickhousectl` では、上記の ClickHouse service コマンドと同様に、[ClickHouse Cloud Postgres](/ja/cloud/managed-postgres) サービスを作成・管理することもできます。

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
  #### Postgres serviceの作成オプション
</div>

| Option                     | Description                              |
| -------------------------- | ---------------------------------------- |
| `--name`                   | サービス名 (必須)                               |
| `--region`                 | リージョン (例: `us-east-1`、必須)                |
| `--size`                   | インスタンスサイズ (例: `m7i.2xlarge`、必須)          |
| `--provider`               | クラウドプロバイダー (デフォルト: `aws`)                |
| `--pg-version`             | メジャーバージョン: `18`、`17`                     |
| `--ha-type`                | 高可用性: `none`、`async`、`sync`              |
| `--tag`                    | リソースタグ `key` または `key=value` (複数回指定可)    |
| `--pg-config-file`         | `PgConfig` オブジェクトを含む JSON ファイルのパス        |
| `--pg-bouncer-config-file` | `PgBouncerConfig` オブジェクトを含む JSON ファイルのパス |

<div id="backups">
  ### バックアップ
</div>

```bash
clickhousectl cloud backup list <service-id>
clickhousectl cloud backup get <service-id> <backup-id>
```

<div id="clickpipes">
  ### ClickPipes
</div>

外部ソースからClickHouse Cloudへデータを取り込むためのClickPipesを管理します。

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
  #### ClickPipes の作成
</div>

各ソースタイプには、`clickpipe create` の配下にそれぞれ専用のサブコマンドがあります。

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

ソースタイプごとのすべてのオプションを確認するには、`clickhousectl cloud clickpipe create <source> --help` を使用します。

<div id="members">
  ### メンバー
</div>

```bash
clickhousectl cloud member list
clickhousectl cloud member get <user-id>
clickhousectl cloud member update <user-id> --role-id <role-id>
clickhousectl cloud member remove <user-id>
```

<div id="invitations">
  ### 招待
</div>

```bash
clickhousectl cloud invitation list
clickhousectl cloud invitation create --email dev@example.com --role-id <role-id>
clickhousectl cloud invitation get <invitation-id>
clickhousectl cloud invitation delete <invitation-id>
```

<div id="keys">
  ### キー
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
  ### JSON 出力
</div>

JSON 形式でレスポンスを出力するには、`--json` フラグを使用します。

```bash
clickhousectl cloud --json service list
clickhousectl cloud --json service get <service-id>
```

`clickhousectl` は、コーディングエージェントのコンテキスト (Claude Code、Cursor、Codex、Gemini CLI、Goose、Devin、および標準の `AGENT` 環境変数を設定する任意のツール) を自動検出し、`--json` を指定しなくても自動的に JSON を stdout に出力します。

<div id="exit-codes">
  ### 終了コード
</div>

終了コードは `gh` CLI の規約に従います。

| コード | 意味                                    |
| --- | ------------------------------------- |
| `0` | 成功                                    |
| `1` | エラー (以下に分類されないすべてのもの)                 |
| `2` | キャンセル (ユーザーが中止)                       |
| `4` | 認証が必要 (認証情報なし、401/403、OAuth 専用の書き込み)  |

<div id="skills">
  ## Skills
</div>

[ClickHouse/agent-skills](https://github.com/ClickHouse/agent-skills) から、公式の ClickHouse Agent Skills をインストールします。

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
  ### 非対話型フラグ
</div>

| Flag              | Description                                 |
| ----------------- | ------------------------------------------- |
| `--agent <name>`  | 特定のエージェント向けにスキルをインストールします (複数回指定可能)         |
| `--global`        | グローバルスコープを使用します。省略した場合はプロジェクトスコープが使用されます    |
| `--all`           | サポートされているすべてのエージェント向けにスキルをインストールします         |
| `--detected-only` | システム上で検出された、サポートされているエージェント向けにスキルをインストールします |

<div id="self-update">
  ## セルフアップデート
</div>

`clickhousectl` は自身を最新のリリースに更新できます。

```bash
# Update to the latest version
clickhousectl update

# Check for updates without installing
clickhousectl update --check
```

CLI はバックグラウンドで更新を確認し (24 時間に最大 1 回) 、新しいバージョンが利用可能な場合は通知を表示します。