---
description: 'Documentação do clickhousectl, a CLI do ClickHouse: local e na nuvem'
sidebar_label: 'clickhousectl'
sidebar_position: 17
slug: /interfaces/cli
title: 'clickhousectl'
doc_type: 'reference'
---

`clickhousectl` é a CLI do ClickHouse: local e na nuvem.

Com o `clickhousectl`, você pode:

* Instalar e gerenciar versões locais do ClickHouse
* Iniciar e gerenciar servidores locais do ClickHouse
* Executar e gerenciar instâncias locais do Postgres
* Executar consultas em servidores do ClickHouse
* Configurar o ClickHouse Cloud e criar clusters do ClickHouse gerenciados na nuvem
* Criar e gerenciar serviços ClickHouse Cloud Postgres
* Gerenciar recursos do ClickHouse Cloud
* Criar e gerenciar ClickPipes para ingestão de dados (S3, Kafka, Kinesis, Postgres, MySQL, MongoDB, BigQuery)
* Instalar o pacote oficial ClickHouse Agent Skills em agentes de programação compatíveis
* Enviar seu ambiente de desenvolvimento local do ClickHouse para a nuvem

O `clickhousectl` ajuda pessoas e agentes de IA a desenvolver com o ClickHouse.

<div id="installation">
  ## Instalação
</div>

<div id="quick-install">
  ### Instalação rápida
</div>

```bash
curl https://clickhouse.com/cli | sh
```

O script de instalação baixa a versão correta para o seu SO e a instala em `~/.local/bin/clickhousectl`. Um alias `chctl` também é criado automaticamente para facilitar.

<div id="requirements">
  ## Requisitos
</div>

* macOS (aarch64, x86&#95;64) ou Linux (aarch64, x86&#95;64)
* Os comandos da Cloud exigem uma [chave de API do ClickHouse Cloud](/pt-BR/cloud/manage/api/api-overview)

<div id="local">
  ## Local
</div>

<div id="installing-versions">
  ### Instalando e gerenciando versões do ClickHouse
</div>

`clickhousectl` baixa os binários do ClickHouse de `builds.clickhouse.com` e, se uma compilação não estiver disponível ali, recorre a `packages.clickhouse.com` (Linux) ou aos [lançamentos do GitHub](https://github.com/ClickHouse/ClickHouse/releases) (macOS).

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

`local use` também cria um link simbólico em `~/.local/bin/clickhouse` apontando para o binário da versão selecionada, para que o comando `clickhouse` sem subcomando (por exemplo, `clickhouse local`, `clickhouse client`) esteja no `PATH`. Passe `--no-global` para pular isso. Se já existir um arquivo comum nesse caminho, ele será mantido como está e um aviso será exibido. O `local remove` da versão `default` ativa também remove o link simbólico.

<div id="binary-storage">
  #### Armazenamento dos binários do ClickHouse
</div>

Os binários do ClickHouse ficam armazenados em um repositório global, para que possam ser usados por vários projetos sem duplicar o espaço de armazenamento. Os binários são armazenados em `~/.clickhouse/`:

```bash
~/.clickhouse/
├── versions/
│   └── 26.5.2.39/
│       └── clickhouse
└── default              # tracks the active version
```

<div id="initializing-project">
  ### Iniciando um projeto
</div>

```bash
clickhousectl local init
```

`init` inicializa seu diretório de trabalho atual com uma estrutura de pastas padrão para os arquivos do seu projeto ClickHouse e Postgres. Isso é opcional; se preferir, você pode usar sua própria estrutura de pastas.

Ele cria a seguinte estrutura:

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
  ### Executando consultas
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
  ### Criando e gerenciando servidores ClickHouse
</div>

Inicie e gerencie instâncias de servidor do ClickHouse. Cada servidor recebe seu próprio diretório de dados isolado em `.clickhouse/servers/<name>/data/`.

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

**Nomes dos servidores:** Sem `--name`, o primeiro servidor recebe o nome &quot;default&quot;. Se &quot;default&quot; já estiver em execução, um nome aleatório será gerado (por exemplo, &quot;bold-crane&quot;). Use `--name` para ter identidades estáveis que possam ser iniciadas/interrompidas repetidamente.

**Portas:** As portas padrão são HTTP 8123 e TCP 9000. Se já estiverem em uso, portas livres serão atribuídas automaticamente e exibidas na saída. Use `--http-port` e `--tcp-port` para definir portas explícitas.

**Gerenciamento global de servidores:** Use `--global` com `list`, `stop` e `stop-all` para operar em todos os projetos no sistema inteiro. `server list --global` mostra todos os servidores ClickHouse em execução, com uma coluna Project indicando a qual diretório cada um pertence.

<div id="custom-config-files">
  #### Arquivos de configuração personalizados para servidores locais
</div>

Os servidores locais vêm com configurações padrão sensatas, mas às vezes você precisa ajustar alguma configuração. Coloque um arquivo de configuração em `~/.clickhouse/configs/` e aplique-o pelo nome ao iniciar um servidor:

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

O arquivo nomeado é **aplicado sobre as configurações padrão internas do ClickHouse** (via `config.d`), portanto ele só precisa conter as configurações que você deseja alterar, sem necessidade de reproduzir uma config completa. Os arquivos podem ser `.xml`, `.yaml` ou `.yml`, e você pode referenciá-los pelo nome, com ou sem a extensão.

<div id="project-local-data">
  #### Diretório de dados local ao projeto
</div>

Todos os dados do servidor ficam em `.clickhouse/`, no diretório do projeto:

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

Cada servidor nomeado tem seu próprio diretório de dados, portanto os servidores ficam totalmente isolados uns dos outros. Os dados persistem entre reinicializações. Pare e inicie um servidor pelo nome para continuar de onde parou. Use `clickhousectl local server remove <name>` para excluir permanentemente os dados de um servidor.

<div id="local-postgres">
  ### Executando o Postgres local
</div>

Além do ClickHouse, o `clickhousectl` pode executar e gerenciar instâncias locais do Postgres. O Postgres local usa o Docker, portanto o Docker deve estar instalado e em execução. Cada instância é identificada pelo nome e pela versão principal, portanto várias versões do Postgres podem ser executadas lado a lado com diretórios de dados separados.

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
  ## Autenticação
</div>

Autentique-se no ClickHouse Cloud usando chaves de API (recomendado) ou OAuth (no navegador).

Se você ainda não tiver uma conta do ClickHouse Cloud, `clickhousectl cloud auth signup` abrirá a página de cadastro no seu navegador.

<div id="api-key">
  ### Chave/segredo de API (recomendado)
</div>

As chaves de API são a forma recomendada de autenticação, especialmente ao usar a CLI por meio de um agente de IA. Você pode [criar chaves de API com escopo](/pt-BR/cloud/manage/openapi) que concedem apenas as permissões que você escolher (somente leitura ou leitura/gravação), e cada chave é vinculada a uma única organização. Isso faz delas uma forma segura, com privilégio mínimo, de conceder acesso à CLI.

```bash
# Non-interactive (CI-friendly)
clickhousectl cloud auth login --api-key YOUR_KEY --api-secret YOUR_SECRET

# Interactive prompt
clickhousectl cloud auth login --interactive
```

As credenciais são armazenadas em `.clickhouse/credentials.json` (local do projeto).

Você também pode usar variáveis de ambiente, exportando-as na sua sessão:

```bash
export CLICKHOUSE_CLOUD_API_KEY=your-key
export CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

Ou em um arquivo `.env` no diretório de trabalho atual:

```env
CLICKHOUSE_CLOUD_API_KEY=your-key
CLICKHOUSE_CLOUD_API_SECRET=your-secret
```

Ou passe as credenciais diretamente usando flags em qualquer comando:

```bash
clickhousectl cloud --api-key KEY --api-secret SECRET ...
```

<div id="oauth-login">
  ### Login via OAuth
</div>

```bash
clickhousectl cloud auth login
```

Isso abre seu navegador para autenticação por meio do fluxo de dispositivo do OAuth. Os tokens são salvos em `.clickhouse/tokens.json` (local ao projeto).

:::note
No momento, o acesso via OAuth é **somente leitura** e concede acesso a **todas as organizações às quais você pertence**. Para ter acesso de gravação, ou para restringir a CLI a uma única organização, [crie uma chave de API com escopo](#api-key).
:::

<div id="auth-status">
  ### Status de autenticação e logout
</div>

```bash
clickhousectl cloud auth status    # Show current auth state
clickhousectl cloud auth logout    # Clear all saved credentials (credentials.json & tokens.json)
```

Ordem de resolução das credenciais: flags da CLI &gt; `.clickhouse/credentials.json` &gt; variáveis de ambiente exportadas &gt; arquivo `.env` &gt; tokens OAuth.

<div id="debug-credentials">
  ### Depuração da fonte de credenciais usada
</div>

Use `--debug` com qualquer comando `cloud` para imprimir em stderr a fonte de credenciais resolvida (e a URL da API) antes da execução do comando.

```bash
clickhousectl cloud --debug service list
# [debug] auth source: credentials file (.clickhouse/credentials.json)
# [debug] api url: https://api.clickhouse.cloud/v1
# ... normal output ...
```

<div id="cloud">
  ## Cloud
</div>

Gerencie os serviços do ClickHouse Cloud por meio da API.

<div id="organizations">
  ### Organizações
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
  ### Serviços
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
  #### Opções de criação de serviço
</div>

| Opção                                      | Descrição                                                     |
| ------------------------------------------ | ------------------------------------------------------------- |
| `--name`                                   | Nome do serviço (obrigatório)                                 |
| `--provider`                               | Provedor de Cloud: `aws`, `gcp`, `azure` (padrão: `aws`)      |
| `--region`                                 | Região (padrão: `us-east-1`)                                  |
| `--min-replica-memory-gb`                  | Memória mínima por réplica em GB (8-356, múltiplo de 4)       |
| `--max-replica-memory-gb`                  | Memória máxima por réplica em GB (8-356, múltiplo de 4)       |
| `--num-replicas`                           | Número de réplicas (1-20)                                     |
| `--idle-scaling`                           | Permitir escalar até zero (padrão: `true`)                    |
| `--idle-timeout-minutes`                   | Tempo limite mínimo de inatividade em minutos (&gt;= 5)       |
| `--ip-allow`                               | CIDR de IP a permitir (repetível, padrão: `0.0.0.0/0`)        |
| `--backup-id`                              | ID do backup a partir do qual restaurar                       |
| `--release-channel`                        | Canal de lançamento: `slow`, `default`, `fast`                |
| `--data-warehouse-id`                      | ID do data warehouse (para réplicas de leitura)               |
| `--readonly`                               | Tornar o serviço somente leitura                              |
| `--encryption-key`                         | Chave de criptografia de disco do cliente                     |
| `--encryption-role`                        | ARN da função para criptografia de disco                      |
| `--enable-tde`                             | Ativar Transparent Data Encryption                            |
| `--compliance-type`                        | Compliance: `hipaa`, `pci`                                    |
| `--profile`                                | Perfil da instância (Enterprise)                              |
| `--tag`                                    | Adicionar uma tag de serviço GA (`key` ou `key=value`)        |
| `--enable-endpoint` / `--disable-endpoint` | Ativar/desativar endpoints de serviço GA (atualmente `mysql`) |
| `--private-preview-terms-checked`          | Aceitar os termos da private preview quando necessário        |
| `--enable-core-dumps`                      | Ativar ou desativar a coleta de core dumps do serviço         |

<div id="query-api-auth-modes">
  #### Modos de autenticação da Query API
</div>

`cloud service query` é a forma canônica de executar SQL em um serviço em nuvem via HTTP, sem o binário `clickhouse` e sem exigir a senha do serviço. Ele funciona com ambos os modos de credenciais:

* **autenticação com API key** (leitura + escrita em SQL): na primeira vez que `cloud service query` é executado em um serviço sem uma chave armazenada, ele provisiona um endpoint da Query API para esse serviço e cria uma API key dedicada vinculada a ele. A chave (`keyId`, `keySecret` e `endpointId`) é armazenada em `.clickhouse/credentials.json`, em `service_query_keys.<service-id>`. A chave é restrita a um único serviço, portanto pode ler e gravar (SELECT, INSERT, DDL) nesse serviço, mas não pode acessar nenhum outro serviço na org. Passe `--no-auto-enable` para falhar em vez de provisionar.
* **OAuth** (`cloud auth login`): a consulta é executada com a sua própria identidade, assim como no Console SQL da web. Suas permissões de SQL no serviço são **somente leitura** ao usar OAuth. Nenhuma chave da Query API é provisionada nem armazenada. `--no-auto-enable` não tem efeito nesse modo.

Consultar um serviço **ocioso** o ativa automaticamente em ambos os modos de autenticação (a primeira consulta pode levar um minuto). Um serviço **parado** nunca é ativado: a consulta falha com uma indicação para executar `cloud service start`. Defina `CLICKHOUSE_CLOUD_QUERY_HOST` para substituir o host derivado da Query API.

<div id="query-endpoints">
  #### Gerenciamento de endpoints de consulta
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
  #### Gerenciamento de endpoint privado
</div>

```bash
clickhousectl cloud service private-endpoint create <service-id> --endpoint-id vpce-123
clickhousectl cloud service private-endpoint get-config <service-id>
```

<div id="backup-config">
  #### Configuração de backup
</div>

```bash
clickhousectl cloud service backup-config get <service-id>
clickhousectl cloud service backup-config update <service-id> \
  --backup-period-hours 24 \
  --backup-retention-period-hours 720 \
  --backup-start-time 02:00
```

<div id="postgres-services">
  ### Serviços Postgres
</div>

O `clickhousectl` também pode criar e gerenciar serviços [ClickHouse Cloud Postgres](/pt-BR/cloud/managed-postgres), seguindo os comandos do serviço ClickHouse acima.

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
  #### Opções de criação do serviço Postgres
</div>

| Opção                      | Descrição                                                         |
| -------------------------- | ----------------------------------------------------------------- |
| `--name`                   | Nome do serviço (obrigatório)                                     |
| `--region`                 | Região, por exemplo `us-east-1` (obrigatório)                     |
| `--size`                   | Tamanho da instância, por exemplo `m7i.2xlarge` (obrigatório)     |
| `--provider`               | provedor de Cloud (padrão: `aws`)                                 |
| `--pg-version`             | Versão principal: `18`, `17`                                      |
| `--ha-type`                | Alta disponibilidade: `none`, `async`, `sync`                     |
| `--tag`                    | Tag do recurso `key` ou `key=value` (repetível)                   |
| `--pg-config-file`         | Caminho para um arquivo JSON contendo um objeto `PgConfig`        |
| `--pg-bouncer-config-file` | Caminho para um arquivo JSON contendo um objeto `PgBouncerConfig` |

<div id="backups">
  ### Backups
</div>

```bash
clickhousectl cloud backup list <service-id>
clickhousectl cloud backup get <service-id> <backup-id>
```

<div id="clickpipes">
  ### ClickPipes
</div>

Gerencie o ClickPipes para fazer a ingestão de dados de fontes externas no ClickHouse Cloud.

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
  #### Como criar ClickPipes
</div>

Cada tipo de origem tem seu próprio subcomando em `clickpipe create`:

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

Use `clickhousectl cloud clickpipe create <source> --help` para ver a lista completa de opções para cada tipo de origem.

<div id="members">
  ### Membros
</div>

```bash
clickhousectl cloud member list
clickhousectl cloud member get <user-id>
clickhousectl cloud member update <user-id> --role-id <role-id>
clickhousectl cloud member remove <user-id>
```

<div id="invitations">
  ### Convites
</div>

```bash
clickhousectl cloud invitation list
clickhousectl cloud invitation create --email dev@example.com --role-id <role-id>
clickhousectl cloud invitation get <invitation-id>
clickhousectl cloud invitation delete <invitation-id>
```

<div id="keys">
  ### Chaves
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
  ### Saída em JSON
</div>

Use a flag `--json` para exibir respostas em formato JSON.

```bash
clickhousectl cloud --json service list
clickhousectl cloud --json service get <service-id>
```

`clickhousectl` detecta automaticamente contextos de agentes de código (Claude Code, Cursor, Codex, Gemini CLI, Goose, Devin e qualquer ferramenta que defina a variável de ambiente padrão `AGENT`) e envia JSON para stdout sem precisar definir `--json`.

<div id="exit-codes">
  ### Códigos de saída
</div>

Os códigos de saída seguem as convenções da CLI `gh`:

| Código | Significado                                                                     |
| ------ | ------------------------------------------------------------------------------- |
| `0`    | Sucesso                                                                         |
| `1`    | Erro (qualquer item não classificado abaixo)                                    |
| `2`    | Cancelado (o usuário abortou)                                                   |
| `4`    | Autenticação necessária (sem credenciais, 401/403, gravações somente via OAuth) |

<div id="skills">
  ## Skills
</div>

Instale o pacote oficial ClickHouse Agent Skills em [ClickHouse/agent-skills](https://github.com/ClickHouse/agent-skills).

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
  ### Flags não interativas
</div>

| Flag              | Descrição                                                                 |
| ----------------- | ------------------------------------------------------------------------- |
| `--agent <name>`  | Instala Skills para um agente específico (pode ser usado mais de uma vez) |
| `--global`        | Usa o escopo global; se omitido, usa o escopo do projeto                  |
| `--all`           | Instala Skills para todos os agentes compatíveis                          |
| `--detected-only` | Instala Skills para os agentes compatíveis detectados no sistema          |

<div id="self-update">
  ## Autoatualização
</div>

`clickhousectl` pode se autoatualizar para o lançamento mais recente:

```bash
# Update to the latest version
clickhousectl update

# Check for updates without installing
clickhousectl update --check
```

A CLI também verifica atualizações em segundo plano (no máximo uma vez a cada 24 horas) e exibe um aviso quando há uma versão mais recente disponível.