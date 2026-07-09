---
description: 'O motor de tabela Kafka funciona com o Apache Kafka e permite publicar em fluxos de dados ou assinar
  esses fluxos, organizar armazenamento tolerante a falhas e processar streams à medida que ficam
  disponíveis.'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Motor de tabela Kafka'
keywords: ['Kafka', 'motor de tabela']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # motor de tabela Kafka
</div>

:::tip
Se você usa o ClickHouse Cloud, recomendamos usar [ClickPipes](/pt-BR/integrations/clickpipes). O ClickPipes oferece suporte nativo a conexões em rede privada, permite escalar a ingestão e os recursos do cluster de forma independente e fornece monitoramento abrangente para streaming de dados do Kafka para o ClickHouse.
:::

* Publique ou assine fluxos de dados.
* Organize um armazenamento tolerante a falhas.
* Processe streams à medida que ficam disponíveis.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

Parâmetros obrigatórios:

* `kafka_broker_list` — Uma lista de brokers separada por vírgulas (por exemplo, `localhost:9092`).
* `kafka_topic_list` — Uma lista de tópicos do Kafka.
* `kafka_group_name` — Um grupo de consumidores do Kafka. Os offsets de leitura são rastreados separadamente para cada grupo. Se você não quiser que as mensagens sejam duplicadas no cluster, use o mesmo nome de grupo em todos os lugares.
* `kafka_format` — Formato da mensagem. Usa a mesma notação que a função SQL `FORMAT`, como `JSONEachRow`. Para mais informações, consulte a seção [Formatos](../../../interfaces/formats.md).

Parâmetros opcionais:

* `kafka_security_protocol` - Protocolo usado para se comunicar com os brokers. Valores possíveis: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`.
* `kafka_sasl_mechanism` - Mecanismo SASL a ser usado na autenticação. Valores possíveis: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`.
* `kafka_aws_region` - Região da AWS para autenticação MSK IAM. Detectada automaticamente a partir do endereço do broker, se não for especificada. Especifique explicitamente ao usar aliases do PrivateLink ou hostname DNS personalizados que não contenham informações da região. Padrão: vazio (detecção automática).
* `kafka_sasl_username` - Nome de usuário SASL para uso com os mecanismos `PLAIN` e `SASL-SCRAM-..`.
* `kafka_sasl_password` - Senha SASL para uso com os mecanismos `PLAIN` e `SASL-SCRAM-..`.
* `kafka_schema` — Parâmetro que deve ser usado se o format exigir uma definição de schema. Por exemplo, [Cap&#39;n Proto](https://capnproto.org/) requer o caminho para o schema file e o nome do objeto raiz `schema.capnp:Message`.
* `kafka_schema_registry_skip_bytes` — Número de bytes a ignorar no início de cada mensagem ao usar schema registry com headers de envelope (por exemplo, AWS Glue Schema Registry, que inclui um envelope de 19 bytes). Intervalo: `[0, 255]`. Padrão: `0`.
* `kafka_num_consumers` — Número de consumers por tabela. Especifique mais consumers se a throughput de um consumer for insuficiente. O número total de consumers não deve exceder o número de partitions no tópico, já que apenas um consumer pode ser atribuído por partition, e não deve ser maior que o número de núcleos físicos no servidor em que o ClickHouse está implantado. Padrão: `1`.
* `kafka_max_block_size` — Tamanho máximo do batch (em mensagens) para poll. Padrão: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `kafka_skip_broken_messages` — Tolerância do parser de mensagens do Kafka a mensagens incompatíveis com o schema por bloco. Se `kafka_skip_broken_messages = N`, o engine ignora *N* mensagens do Kafka que não podem ser analisadas (uma mensagem equivale a uma linha de dados). Padrão: `0`.
* `kafka_commit_every_batch` — Faz commit de cada batch consumido e processado, em vez de um único commit após gravar um bloco inteiro. Padrão: `0`.
* `kafka_client_id` — Identificador do client. Vazio por padrão.
* `kafka_poll_timeout_ms` — Timeout para um único poll do Kafka. Padrão: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `kafka_poll_max_batch_size` — Quantidade máxima de mensagens a ser obtida em um único poll do Kafka. Padrão: [max&#95;block&#95;size](/pt-BR/operations/settings/settings#max_block_size).
* `kafka_flush_interval_ms` — Timeout para flush dos dados do Kafka. Padrão: [stream&#95;flush&#95;interval&#95;ms](/pt-BR/operations/settings/settings#stream_flush_interval_ms).
* `kafka_consumer_reschedule_ms` — Intervalo de reagendamento quando o stream processing do Kafka fica interrompido (por exemplo, quando não há mensagens disponíveis para consumo). Essa configuração controla o atraso antes de o consumer tentar fazer poll novamente. Não deve exceder `kafka_consumers_pool_ttl_ms`. Padrão: `500` milissegundos.
* `kafka_thread_per_consumer` — Fornece uma thread independente para cada consumer. Quando habilitado, cada consumer faz flush dos dados de forma independente, em paralelo (caso contrário, as linhas de vários consumers são combinadas para formar um bloco). Padrão: `0`.
* `kafka_handle_error_mode` — Como tratar erros no motor Kafka. Valores possíveis: default (a exceção será lançada se não for possível analisar uma mensagem), stream (a mensagem de exceção e a mensagem bruta serão salvas nas colunas virtuais `_error` e `_raw_message`), dead&#95;letter&#95;queue (os dados relacionados ao erro serão salvos em system.dead&#95;letter&#95;queue).
* `kafka_commit_on_select` —  Faz commit das mensagens quando uma consulta `SELECT` é executada. Padrão: `false`.
* `kafka_consumer_acquire_timeout_ms` — Timeout em milissegundos para adquirir um consumer Kafka durante consultas diretas `SELECT` em uma tabela `Kafka2` (com armazenamento de offsets baseado em Keeper). Quando várias consultas diretas `SELECT` concorrentes são executadas na mesma tabela, cada uma deve esperar até que consumers fiquem disponíveis. O timeout evita deadlocks quando as consultas retêm subconjuntos diferentes de consumers. Padrão: `30000`.
* `kafka_max_rows_per_message` — O número máximo de linhas gravadas em uma mensagem Kafka para formatos baseados em linhas. Padrão: `1`.
* `kafka_autodetect_client_rack` — Define automaticamente o parâmetro `client.rack` para `librdkafka` a fim de dar preferência às réplicas Kafka mais próximas.
  Fontes compatíveis:
  `AWS_ZONE_ID` para o ID da zona de disponibilidade do AWS IMDSv2, por exemplo `euc1-az1`;
  `AWS_ZONE_NAME` para o nome da zona de disponibilidade do AWS IMDSv2, por exemplo `eu-central-1a`;
  `GCP_ZONE` para a zona do serviço de metadados do GCP, por exemplo `europe-central2-a`;
  `CLICKHOUSE` para usar a detecção interna do ClickHouse, que pode depender de metadados da nuvem ou da configuração;
  `AWS_ZONE_NAME_THEN_GCP_ZONE` para tentar `AWS_ZONE_NAME` e depois `GCP_ZONE`.
  Padrão: string vazia, desabilitado.
  Dica: ambientes diferentes usam formatos diferentes de zona de disponibilidade. O Amazon MSK normalmente usa IDs de zona, portanto prefira `AWS_ZONE_ID`. O Confluent Cloud normalmente usa nomes de zona, portanto prefira `AWS_ZONE_NAME`. Se não tiver certeza, use `AWS_ZONE_NAME_THEN_GCP_ZONE` ou verifique o valor de `broker.rack` no seu cluster.
  Nota: os brokers Kafka devem ser configurados com `broker.rack` e `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
* `kafka_compression_codec` — codec de compressão usado para produzir mensagens. Compatíveis: string vazia, `none`, `gzip`, `snappy`, `lz4`, `zstd`. No caso de string vazia, o codec de compressão não é definido pela tabela; portanto, serão usados os valores dos arquivos de configuração ou o valor padrão de `librdkafka`. Padrão: string vazia.
* `kafka_compression_level` — Parâmetro de nível de compressão para o algoritmo selecionado por kafka&#95;compression&#95;codec. Valores mais altos resultarão em melhor compressão, ao custo de maior uso de CPU. O intervalo utilizável depende do algoritmo: `[0-9]` para `gzip`; `[0-12]` para `lz4`; apenas `0` para `snappy`; `[0-12]` para `zstd`; `-1` = nível de compressão padrão dependente do codec. Padrão: `-1`.
* `kafka_map_virtual_columns_on_write` — Se habilitado, colunas com nomes especiais `_key`, `_timestamp`, `_headers.name` e `_headers.value` no esquema da tabela são mapeadas para os metadados correspondentes da mensagem Kafka em `INSERT` e são excluídas do payload da mensagem. Veja [Mapeamento de colunas para metadados de mensagens Kafka](#mapping-columns-to-kafka-message-metadata). Padrão: `false`.

Exemplos:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">
  <summary>Método descontinuado para criar uma tabela</summary>

  :::note
  Não use este método em novos projetos. Se possível, migre os projetos antigos para o método descrito acima.
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
O mecanismo de tabela Kafka não oferece suporte a colunas com [valor padrão](/pt-BR/sql-reference/statements/create/table#default_values). Se você precisar de colunas com valor padrão, poderá adicioná-las na visão materializada (veja abaixo).
:::

<div id="description">
  ## Descrição
</div>

As mensagens entregues são rastreadas automaticamente, portanto cada mensagem em um grupo é contabilizada apenas uma vez. Se você quiser obter os dados duas vezes, crie uma cópia da tabela com outro nome de grupo.

Os grupos são flexíveis e sincronizados no cluster. Por exemplo, se você tiver 10 tópicos e 5 cópias de uma tabela em um cluster, cada cópia receberá 2 tópicos. Se o número de cópias mudar, os tópicos serão redistribuídos automaticamente entre as cópias. Leia mais sobre isso em http://kafka.apache.org/intro.

Recomenda-se que cada tópico do Kafka tenha seu próprio grupo de consumidores dedicado, garantindo um pareamento exclusivo entre o tópico e o grupo, especialmente em ambientes em que os tópicos podem ser criados e excluídos dinamicamente (por exemplo, em testes ou homologação).

`SELECT` não é particularmente útil para ler mensagens (exceto para depuração), porque cada mensagem pode ser lida apenas uma vez. É mais prático criar fluxos em tempo real usando visões materializadas. Para fazer isso:

1. Use o engine para criar um consumidor Kafka e trate-o como um fluxo de dados.
2. Crie uma tabela com a estrutura desejada.
3. Crie uma visão materializada que converta os dados do engine e os grave em uma tabela criada anteriormente.

Quando a `MATERIALIZED VIEW` é vinculada ao engine, ela começa a coletar dados em segundo plano. Isso permite que você receba continuamente mensagens do Kafka e as converta para o formato necessário usando `SELECT`.
Uma tabela Kafka pode ter quantas visões materializadas você quiser; elas não leem dados da tabela Kafka diretamente, mas recebem novos registros (em blocos). Dessa forma, você pode gravar em várias tabelas com diferentes níveis de detalhamento (com agrupamento — agregação — e sem).

Exemplo:

```sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Para melhorar o desempenho, as mensagens recebidas são agrupadas em blocos com o tamanho definido por [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size). Se o bloco não for formado dentro de [stream&#95;flush&#95;interval&#95;ms](/pt-BR/operations/settings/settings#stream_flush_interval_ms) milissegundos, os dados serão gravados na tabela mesmo que o bloco não esteja completo.

Para parar de receber dados do tópico ou alterar a lógica de conversão, desanexe a visão materializada:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Se quiser alterar a tabela de destino usando `ALTER`, recomendamos desabilitar a visão materializada para evitar discrepâncias entre a tabela de destino e os dados da view.

<div id="configuration">
  ## Configuração
</div>

Assim como o GraphiteMergeTree, o motor Kafka oferece suporte a configuração estendida por meio do arquivo de configuração do ClickHouse. Há duas chaves de configuração que você pode usar: global (em `<kafka>`) e no nível do tópico (em `<kafka><kafka_topic>`). A configuração global é aplicada primeiro, e depois a configuração no nível do tópico é aplicada (se existir).

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- Settings for producer -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

Para ver uma lista das opções de configuração disponíveis, consulte a [referência de configuração do librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Use o sublinhado (`_`) em vez do ponto na configuração do ClickHouse. Por exemplo, `check.crcs=true` será `<check_crcs>true</check_crcs>`.

<div id="kafka-aws-msk-iam">
  ### Autenticação IAM do AWS MSK
</div>

:::note
A autenticação IAM do AWS MSK exige que o ClickHouse seja compilado com o suporte ao AWS S3 habilitado.
:::

O AWS MSK oferece suporte à autenticação baseada em IAM, permitindo a conexão com clusters Kafka usando credenciais da AWS em vez de gerenciar nomes de usuário e senhas separados.

**Configuração básica:**

Defina `kafka_sasl_mechanism = 'AWS_MSK_IAM'` nas configurações da tabela:

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

A região da AWS é extraída automaticamente do endpoint do broker por correspondência de padrões:

* MSK provisionado: `b-X.cluster.kafka.<region>.amazonaws.com:9098`
* MSK serverless: `boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* Endpoint da VPC: `vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**Credenciais da AWS:**

As credenciais são sempre carregadas de `~/.aws/credentials` e `~/.aws/config` (arquivos de perfil da AWS), quando presentes. Para habilitar também perfis de instância do EC2, variáveis de ambiente (`AWS_ACCESS_KEY_ID`, etc.), roles de tarefa do ECS e outras fontes automáticas de credenciais, adicione o seguinte à configuração do servidor:

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

Essa configuração só pode ser definida por administradores do servidor. Padrão: `false`.

**PrivateLink e DNS personalizado:**

Ao usar aliases do PrivateLink ou hostnames de DNS personalizados que não contêm informações de região, especifique explicitamente a região da AWS:

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**Permissões do IAM:**

Permissões do consumer (para ler mensagens):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

Permissões do producer (para gravar mensagens):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### Suporte ao Kerberos
</div>

Para lidar com o Kafka com suporte a Kerberos, adicione o elemento filho `security_protocol` com o valor `sasl_plaintext`. Isso é suficiente se o ticket de concessão de tickets (TGT) do Kerberos tiver sido obtido e armazenado em cache pelos recursos do sistema operacional.
O ClickHouse pode manter credenciais do Kerberos usando um arquivo keytab. Considere os elementos filho `sasl_kerberos_service_name`, `sasl_kerberos_keytab` e `sasl_kerberos_principal`.

Exemplo:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_topic` — Tópico do Kafka. Tipo de dado: `LowCardinality(String)`.
* `_key` — Chave da mensagem. Tipo de dado: `String`.
* `_offset` — Offset da mensagem. Tipo de dado: `UInt64`.
* `_timestamp` — Timestamp da mensagem. Tipo de dado: `Nullable(DateTime)`.
* `_timestamp_ms` — Timestamp da mensagem em milissegundos. Tipo de dado: `Nullable(DateTime64(3))`.
* `_partition` — Partição do tópico do Kafka. Tipo de dado: `UInt64`.
* `_headers.name` — Array com as chaves dos cabeçalhos da mensagem. Tipo de dado: `Array(String)`.
* `_headers.value` — Array com os valores dos cabeçalhos da mensagem. Tipo de dado: `Array(String)`.

Colunas virtuais adicionais quando `kafka_handle_error_mode='stream'`:

* `_raw_message` - Mensagem bruta que não pôde ser processada com sucesso. Tipo de dado: `String`.
* `_error` - Mensagem de exceção gerada durante a falha no processamento. Tipo de dado: `String`.

Observação: as colunas virtuais `_raw_message` e `_error` são preenchidas apenas em caso de exceção durante o processamento; elas ficam sempre vazias quando a mensagem é processada com sucesso.

<div id="mapping-columns-to-kafka-message-metadata">
  ## Mapeando colunas para os metadados das mensagens do Kafka
</div>

Ao produzir mensagens com `INSERT INTO`, o motor Kafka sempre usa uma coluna chamada `_key` (do tipo `String`) como a chave da mensagem do Kafka e uma coluna chamada `_timestamp` (do tipo `DateTime`) como o timestamp da mensagem do Kafka — se essas colunas existirem na tabela. Por padrão, essas colunas também aparecem no payload da mensagem produzida junto com as outras colunas.

Com `kafka_map_virtual_columns_on_write = 1`, o comportamento muda:

* `_key` (tipo `String`) — mapeada para a chave da mensagem do Kafka.
* `_timestamp` (tipo `DateTime`) — mapeada para o timestamp da mensagem do Kafka.
* `_headers.name` (tipo `Array(String)`) e `_headers.value` (tipo `Array(String)`) — mapeados para os cabeçalhos da mensagem do Kafka. Cada par `(_headers.name[i], _headers.value[i])` se torna um cabeçalho do Kafka. Como `_headers.name` e `_headers.value` compartilham o prefixo Nested `_headers`, o ClickHouse exige que ambos os arrays tenham o mesmo tamanho em cada linha.

Colunas com esses nomes são **excluídas do payload da mensagem** somente se seus tipos corresponderem aos listados acima; caso contrário, elas permanecem no payload, de modo que esquemas que por acaso reutilizem esses nomes para dados não relacionados continuem funcionando.

Exemplo:

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

A mensagem produzida no Kafka tem o payload `{"event_json":"{\"a\":1}"}`, a chave `session-42`, o timestamp atual e dois headers `source=api` e `trace_id=abc-123`.

<div id="data-formats-support">
  ## Suporte a formatos de dados
</div>

O motor Kafka oferece suporte a todos os [formatos](../../../interfaces/formats.md) compatíveis com o ClickHouse.
O número de linhas em uma mensagem do Kafka depende de o formato ser baseado em linhas ou em blocos:

* Para formatos baseados em linhas, o número de linhas em uma mensagem do Kafka pode ser controlado pela configuração `kafka_max_rows_per_message`.
* Para formatos baseados em blocos, não é possível dividir um bloco em partes menores, mas o número de linhas em um bloco pode ser controlado pela configuração geral [max&#95;block&#95;size](/pt-BR/operations/settings/settings#max_block_size).

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## Engine para armazenar offsets confirmados no ClickHouse Keeper
</div>

<ExperimentalBadge />

Se `allow_experimental_kafka_offsets_storage_in_keeper` estiver habilitado, mais duas configurações poderão ser especificadas para a engine de tabela Kafka:

* `kafka_keeper_path` especifica o path da tabela no ClickHouse Keeper
* `kafka_replica_name` especifica o nome da réplica no ClickHouse Keeper

As duas configurações devem ser especificadas juntas, ou nenhuma delas. Quando ambas são especificadas, uma nova engine Kafka experimental é usada. A nova engine não depende do armazenamento dos offsets confirmados no Kafka, mas os armazena no ClickHouse Keeper. Ela ainda tenta fazer o commit dos offsets no Kafka, mas só usa esses offsets quando a tabela é criada. Em qualquer outra situação (se a tabela for reiniciada ou recuperada após algum erro), os offsets armazenados no ClickHouse Keeper serão usados para continuar consumindo mensagens a partir desse offset. Além do offset confirmado, ela também armazena quantas mensagens foram consumidas no último lote; portanto, se o insert falhar, a mesma quantidade de mensagens será consumida novamente, permitindo a desduplicação, se necessário.

Exemplo:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### Limitações conhecidas
</div>

Como o novo motor é experimental, ele ainda não está pronto para produção. Há algumas limitações conhecidas na implementação:

* Remover e recriar rapidamente a tabela ou especificar o mesmo caminho do ClickHouse Keeper para motores diferentes pode causar problemas. Como prática recomendada, você pode usar `{uuid}` em `kafka_keeper_path` para evitar conflitos entre caminhos.
* Para garantir leituras repetíveis, as mensagens não podem ser consumidas de várias partições em uma única thread. Por outro lado, é preciso fazer polling regular dos consumidores do Kafka para mantê-los ativos. Como esses dois objetivos precisam ser atendidos, decidimos permitir a criação de vários consumidores apenas se `kafka_thread_per_consumer` estiver habilitado; caso contrário, é complexo demais evitar problemas relacionados ao polling regular dos consumidores.

**Veja também**

* [Colunas virtuais](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)