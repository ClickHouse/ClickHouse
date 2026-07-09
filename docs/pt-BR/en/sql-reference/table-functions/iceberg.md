---
description: 'Fornece uma interface semelhante a tabela, somente leitura, para tabelas Apache Iceberg no
  Amazon S3, Azure, HDFS ou armazenadas localmente.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Fornece uma interface semelhante a tabela, somente leitura, para tabelas Apache [Iceberg](https://iceberg.apache.org/) no Amazon S3, Azure, HDFS ou armazenadas localmente.

<div id="syntax">
  ## Sintaxe
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Argumentos
</div>

A descrição dos argumentos coincide com a descrição dos argumentos nas funções de tabela `s3`, `azureBlobStorage`, `HDFS` e `file`, respectivamente.
`format` refere-se ao formato dos arquivos de dados na tabela Iceberg.

Para `icebergS3`, é possível usar um parâmetro opcional `extra_credentials` para passar um `role_arn` para acesso baseado em funções no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

<div id="returned-value">
  ### Valor retornado
</div>

Uma tabela com a estrutura especificada para leitura de dados da tabela Iceberg especificada.

<div id="example">
  ### Exemplo
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
Atualmente, o ClickHouse suporta a leitura das versões v1 e v2 do formato Iceberg por meio das funções de tabela `icebergS3`, `icebergAzure`, `icebergHDFS` e `icebergLocal` e dos motores de tabela `IcebergS3`, `icebergAzure`, `IcebergHDFS` e `IcebergLocal`.
:::

<div id="defining-a-named-collection">
  ## Definindo uma coleção nomeada
</div>

Aqui está um exemplo de configuração de uma coleção nomeada para armazenar a URL e as credenciais:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## Usando um catálogo de dados
</div>

As tabelas Iceberg também podem ser usadas com vários catálogos de dados, como o [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), o [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) e o [Unity Catalog](https://www.unitycatalog.io/).

:::important
Ao usar um catálogo, a maioria dos usuários vai preferir usar o mecanismo de database `DataLakeCatalog`, que conecta o ClickHouse ao seu catálogo para descobrir suas tabelas. Você pode usar esse mecanismo de database em vez de criar manualmente tabelas individuais com o motor de tabela `IcebergS3`.
:::

Para isso, crie uma table com o mecanismo `IcebergS3` e forneça as configurações necessárias.

Por exemplo, usando o REST Catalog com armazenamento MinIO:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

Ou, usando o Glue Data Catalog da AWS com S3:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## Evolução de schema
</div>

No momento, com o CH, você pode ler tabelas Iceberg cujo schema mudou ao longo do tempo. Atualmente, oferecemos suporte à leitura de tabelas em que colunas foram adicionadas ou removidas, e cuja ordem foi alterada. Também é possível alterar uma coluna obrigatória para uma coluna em que NULL é permitido. Além disso, oferecemos suporte a conversões de tipo permitidas para tipos simples, a saber:  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) em que P&#39; &gt; P.

Atualmente, não é possível alterar estruturas aninhadas nem os tipos dos elementos dentro de arrays e maps.

<div id="partition-pruning">
  ## Poda de partições
</div>

O ClickHouse oferece suporte à poda de partições em consultas SELECT para tabelas Iceberg, o que ajuda a otimizar o desempenho das consultas ao ignorar arquivos de dados irrelevantes. Para habilitar a poda de partições, defina `use_iceberg_partition_pruning = 1`. Para mais informações sobre a poda de partições no Iceberg, acesse https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Viagem no tempo
</div>

O ClickHouse oferece suporte à viagem no tempo para tabelas Iceberg, permitindo consultar dados históricos com um `timestamp` específico ou um ID do snapshot.

<div id="deleted-rows">
  ## Processamento de tabelas com linhas excluídas
</div>

Atualmente, há suporte apenas para tabelas Iceberg com [position deletes](https://iceberg.apache.org/spec/#position-delete-files).

Os seguintes métodos de exclusão **não são suportados**:

* [Equality deletes](https://iceberg.apache.org/spec/#equality-delete-files)
* [Deletion vectors](https://iceberg.apache.org/spec/#deletion-vectors) (introduzido na v3)

<div id="basic-usage">
  ### Uso básico
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

Nota: Não é possível especificar os parâmetros `iceberg_timestamp_ms` e `iceberg_snapshot_id` na mesma consulta.

<div id="important-considerations">
  ### Considerações importantes
</div>

* **Snapshots** normalmente são criados quando:

* Novos dados são gravados na tabela

* Algum tipo de compactação de dados é realizada

* **Alterações de schema normalmente não criam snapshots** - Isso resulta em comportamentos importantes ao usar viagem no tempo com tabelas que passaram por evolução de schema.

<div id="example-scenarios">
  ### Cenários de exemplo
</div>

Todos os cenários foram escritos em Spark, porque o CH ainda não oferece suporte à gravação em tabelas Iceberg.

<div id="scenario-1">
  #### Cenário 1: Alterações de esquema sem novos snapshots
</div>

Considere esta sequência de operações:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Resultados da consulta em diferentes timestamps:

* Em ts1 &amp; ts2: aparecem apenas as duas colunas originais
* Em ts3: aparecem as três colunas, com NULL no preço da primeira linha

<div id="scenario-2">
  #### Cenário 2: Diferenças entre o schema histórico e o atual
</div>

Uma consulta de viagem no tempo no momento atual pode mostrar um schema diferente do da tabela atual:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

Isso acontece porque `ALTER TABLE` não cria um novo snapshot e, no caso da tabela atual, o Spark usa o valor de `schema_id` do arquivo de metadados mais recente, e não de um snapshot.

<div id="scenario-3">
  #### Cenário 3: Diferenças entre o schema histórico e o atual
</div>

A segunda é que, ao usar viagem no tempo, não é possível obter o estado da tabela antes de qualquer dado ter sido gravado nela:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

No ClickHouse, o comportamento é o mesmo do Spark. Você pode imaginar as consultas Select do Spark como consultas Select do ClickHouse, e tudo funcionará da mesma forma.

<div id="metadata-file-resolution">
  ## Resolução do arquivo de metadados
</div>

Ao usar a função de tabela `iceberg` no ClickHouse, o sistema precisa localizar o arquivo metadata.json correto que descreve a estrutura da tabela Iceberg. Veja como esse processo de resolução funciona:

<div id="candidate-search">
  ### Busca de Candidatos (em Ordem de Prioridade)
</div>

1. **Especificação Direta do Caminho**:
   *Se você definir `iceberg_metadata_file_path`, o sistema usará esse caminho exato, combinando-o com o caminho do diretório da tabela Iceberg.

* Quando essa configuração é fornecida, todas as outras configurações de resolução são ignoradas.

2. **Correspondência do UUID da Tabela**:
   *Se `iceberg_metadata_table_uuid` for especificado, o sistema irá:
   *Examinar apenas arquivos `.metadata.json` no diretório `metadata`
   *Filtrar arquivos que contenham um campo `table-uuid` correspondente ao UUID especificado (sem diferenciar maiúsculas de minúsculas)

3. **Busca Padrão**:
   *Se nenhuma das configurações acima for fornecida, todos os arquivos `.metadata.json` no diretório `metadata` se tornam candidatos

<div id="most-recent-file">
  ### Selecionando o Arquivo Mais Recente
</div>

Após identificar os arquivos candidatos usando as regras acima, o sistema determina qual deles é o mais recente:

* Se `iceberg_recent_metadata_file_by_last_updated_ms_field` estiver habilitado:

* Será selecionado o arquivo com o maior valor de `last-updated-ms`

* Caso contrário:

* Será selecionado o arquivo com o número de versão mais alto

* (A versão aparece como `V` em nomes de arquivo no formato `V.metadata.json` ou `V-uuid.metadata.json`)

**Observação**: Todas as configurações mencionadas são configurações de função de tabela (não globais nem em nível de consulta) e devem ser especificadas conforme mostrado abaixo:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Observação**: Embora os catálogos do Iceberg normalmente cuidem da resolução de metadados, a função de tabela `iceberg` no ClickHouse interpreta diretamente os arquivos armazenados no S3 como tabelas Iceberg, por isso é importante entender essas regras de resolução.

<div id="metadata-cache">
  ## Cache de metadados
</div>

O motor de tabela e a função de tabela `Iceberg` oferecem suporte a um cache de metadados que armazena informações de arquivos de manifesto, da lista de manifestos e do JSON de metadados. O cache é armazenado em memória. Esse recurso é controlado pela configuração `use_iceberg_metadata_files_cache`, que é ativada por padrão.

<div id="aliases">
  ## Aliases
</div>

A função de tabela `iceberg` agora é um alias de `icebergS3`.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor é `NULL`.
* `_time` — Horário da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se o horário for desconhecido, o valor é `NULL`.
* `_etag` — Etag do arquivo. Tipo: `LowCardinality(String)`. Se o etag for desconhecido, o valor é `NULL`.

<div id="writes-into-iceberg-table">
  ## Gravações em tabela Iceberg
</div>

A partir da versão 25.7, o ClickHouse suporta modificações nas tabelas Iceberg do usuário.

No momento, este é um recurso experimental, portanto, primeiro você precisa habilitá-lo:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### Criando tabela
</div>

Para criar sua própria tabela Iceberg vazia, use os mesmos comandos usados para leitura, mas especifique o esquema explicitamente.
A escrita oferece suporte a todos os formatos de dados da especificação Iceberg, como Parquet, Avro e ORC.

<div id="example">
  ### Exemplo
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Observação: para criar um arquivo de indicação de versão, ative a configuração `iceberg_use_version_hint`.
Se quiser compactar o arquivo metadata.json, especifique o nome do codec na configuração `iceberg_metadata_compression_method`.

<div id="writes-inserts">
  ### INSERT
</div>

Após criar uma nova tabela, você pode inserir dados usando a sintaxe padrão do ClickHouse.

<div id="example">
  ### Exemplo
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

O ClickHouse também oferece suporte à exclusão de linhas extras no formato merge-on-read.
Esta consulta criará um novo snapshot com position delete files.

<div id="example">
  ### Exemplo
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### Evolução de schema
</div>

ClickHouse permite adicionar, remover, modificar ou renomear colunas com tipos simples (que não sejam Tuple, Array nem map).

<div id="example">
  ### Exemplo
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### Compactação
</div>

O ClickHouse oferece suporte à compactação em tabelas Iceberg. Atualmente, ele pode mesclar arquivos de exclusão por posição aos arquivos de dados enquanto atualiza os metadados. Os IDs e timestamps de snapshots anteriores permanecem inalterados, portanto o recurso de viagem no tempo ainda pode ser usado com os mesmos valores.

Como usá-la:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### Expirar snapshots
</div>

As tabelas Iceberg acumulam snapshots a cada operação `INSERT`, `DELETE` ou `UPDATE`. Com o tempo, isso pode resultar em um grande número de snapshots e de arquivos de dados associados. O comando `expire_snapshots` remove snapshots antigos e limpa os arquivos de dados que não são mais referenciados por nenhum snapshot retido.

**Sintaxe:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

Por padrão, quais snapshots serão mantidos é definido pela [política de retenção](#iceberg-snapshot-retention-policy) (propriedades da tabela `min-snapshots-to-keep`, `max-snapshot-age-ms` e substituições por ref). Quando `snapshot_ids` é especificado, a política de retenção é ignorada e somente os snapshots listados são considerados para expiração.

**Argumentos:**

* `'timestamp'` (posicional) ou `expire_before = 'timestamp'` — uma string de data e hora (por exemplo, `'2024-06-01 00:00:00'`) interpretada no **fuso horário do servidor**. Funciona como uma trava de segurança: snapshots cujo `timestamp-ms` seja igual ou posterior a esse valor ficam protegidos contra expiração, mesmo que a política de retenção, de outra forma, os expirasse. Pode ser combinado com `snapshot_ids`; nesse caso, snapshots listados com timestamp igual ou mais recente que esse não são expirados.
* `retention_period = '<duration>'` — substitui `history.expire.max-snapshot-age-ms` no nível da tabela somente para esta invocação. Snapshots mais antigos do que essa duração (medida a partir de agora) tornam-se candidatos à expiração. O valor é uma string de duração composta por um ou mais pares `{number}{unit}` concatenados. Unidades compatíveis: `y` (365 dias), `w` (7 dias), `d` (24 horas), `h` (60 minutos), `m` (60 segundos), `s` (1 segundo), `ms` (1 milissegundo). As unidades podem ser combinadas, por exemplo, `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
* `retain_last = N` — substitui `history.expire.min-snapshots-to-keep` no nível da tabela somente para esta invocação. Pelo menos `N` snapshots são sempre mantidos, independentemente da idade.
* `snapshot_ids = [id1, id2, ...]` — expira exatamente os IDs de snapshot listados (exceto snapshots referenciados pelo snapshot atual, por branches ou por tags). Esse modo ignora completamente a política de retenção e não pode ser combinado com `retention_period` nem com `retain_last`.
* `dry_run = 1` — calcula o que seria expirado e retorna métricas sem gravar novos metadados nem excluir arquivos.

:::note
`retention_period` e `retain_last` substituem apenas os padrões de retenção no **nível da tabela**. Substituições de retenção por ref (branch/tag) configuradas nas propriedades da tabela Iceberg (por exemplo, `refs.<branch>.min-snapshots-to-keep`) nunca são substituídas — elas sempre entram em vigor conforme especificado nos metadados da tabela.
:::

**Exemplo:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**Saída:**

O comando retorna uma tabela com duas colunas (`metric_name String`, `metric_value Int64`), contendo uma linha por métrica. Os nomes das métricas seguem a [especificação do Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#output):

| metric&#95;name                       | Descrição                                                          |
| ------------------------------------- | ------------------------------------------------------------------ |
| `deleted_data_files_count`            | Número de arquivos de dados excluídos                              |
| `deleted_position_delete_files_count` | Número de arquivos de exclusão por posição excluídos               |
| `deleted_equality_delete_files_count` | Número de arquivos de exclusão por igualdade excluídos             |
| `deleted_manifest_files_count`        | Número de arquivos de manifesto excluídos                          |
| `deleted_manifest_lists_count`        | Número de arquivos de lista de manifestos excluídos                |
| `deleted_statistics_files_count`      | Número de arquivos de estatísticas excluídos (sempre 0 no momento) |
| `dry_run`                             | `1` para modo de simulação, `0` para execução normal               |

O comando executa as seguintes etapas:

1. Avalia a política de retenção (veja abaixo) para determinar quais snapshots devem ser preservados
2. Se um argumento de timestamp for fornecido, também protege todos os snapshots nesse timestamp ou mais recentes
3. Expira os snapshots que não são retidos pela política nem protegidos pelo limite de timestamp
4. Calcula quais arquivos estão associados exclusivamente a snapshots expirados
5. No modo normal: gera novos metadados sem os snapshots expirados
6. No modo normal: exclui fisicamente listas de manifestos, arquivos de manifesto e arquivos de dados inacessíveis
7. No modo `dry_run = 1`: pula as etapas 5 e 6 e retorna apenas as métricas calculadas

<div id="iceberg-snapshot-retention-policy">
  #### Política de retenção de snapshots
</div>

O comando `expire_snapshots` respeita a [política de retenção de snapshots do Iceberg](https://iceberg.apache.org/spec/#snapshot-retention-policy). A retenção é configurada por meio das propriedades da tabela Iceberg e de overrides por referência:

| Propriedade                            | Escopo | Padrão                                                                    | Descrição                                                                                                       |
| -------------------------------------- | ------ | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | Tabela | `iceberg_expire_default_min_snapshots_to_keep` (padrão `1`)               | Número mínimo de snapshots a manter na cadeia de ancestrais de cada branch                                      |
| `history.expire.max-snapshot-age-ms`   | Tabela | `iceberg_expire_default_max_snapshot_age_ms` (padrão `432000000`, 5 dias) | Idade máxima (em ms) dos snapshots a serem mantidos em uma branch                                               |
| `history.expire.max-ref-age-ms`        | Tabela | `iceberg_expire_default_max_ref_age_ms` (padrão `∞`)                      | Idade máxima (em ms) de uma referência de snapshot (branch ou tag) antes que a própria referência seja removida |

Cada referência de snapshot (`refs` nos metadados do Iceberg) pode sobrescrever essas configurações com campos específicos por referência: `min-snapshots-to-keep`, `max-snapshot-age-ms` e `max-ref-age-ms`.

**Avaliação da retenção:**

* **Para cada branch** (incluindo `main`): a cadeia de ancestrais é percorrida a partir do head da branch. Os snapshots são mantidos enquanto pelo menos uma destas condições for true:
  * O snapshot está entre os primeiros `min-snapshots-to-keep` da cadeia
  * A idade do snapshot está dentro de `max-snapshot-age-ms` (isto é, `now - timestamp-ms <= max-snapshot-age-ms`)
* **Para tags**: o snapshot marcado é mantido, a menos que a tag tenha excedido seu `max-ref-age-ms`; nesse caso, a referência da tag é removida
* **Referências diferentes de `main`** cuja idade excede `max-ref-age-ms` são removidas por completo (a branch `main` nunca é removida)
* **Referências órfãs** que apontam para snapshots inexistentes são removidas com um aviso
* **O snapshot atual é sempre preservado**, independentemente das configurações de retenção

**Privilégios necessários:**

O privilégio `ALTER TABLE EXECUTE` é necessário e é filho de `ALTER TABLE` na hierarquia de controle de acesso do ClickHouse. Você pode concedê-lo especificamente ou por meio do pai:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* Apenas tabelas no Iceberg format version 2 são compatíveis (snapshots v1 não garantem `manifest-list`, que é necessário para identificar com segurança os arquivos a serem limpos)
* O snapshot atual é sempre preservado, mesmo que seja mais antigo que o timestamp especificado
* Exige que a configuração `allow_insert_into_iceberg` esteja habilitada
* Exige que a configuração `allow_experimental_expire_snapshots` esteja habilitada
* O próprio mecanismo de autorização do catálogo (autenticação do REST catálogo, IAM do AWS Glue etc.) é aplicado independentemente quando o ClickHouse atualiza os metadados
  :::

<div id="iceberg-remove-orphan-files">
  ### Remover arquivos órfãos
</div>

Arquivos órfãos são arquivos no armazenamento que não são referenciados por nenhum snapshot nos metadados da tabela Iceberg. Eles se acumulam devido a gravações com falha, limpeza parcial após a compactação e operações interrompidas, causando crescimento indefinido do armazenamento. O comando `remove_orphan_files` identifica e remove esses arquivos órfãos.

**Sintaxe:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**Parâmetros:**

| Parâmetro    | Tipo                 | Padrão                                                                 | Descrição                                                                                                                                                                                                |
| ------------ | -------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `older_than` | `String` (timestamp) | Há 3 dias (configurável via `iceberg_orphan_files_older_than_seconds`) | Considera como candidatos a arquivos órfãos apenas os arquivos cuja última modificação seja anterior a esse timestamp. Medida de segurança para evitar a exclusão de arquivos de gravações em andamento. |
| `location`   | `String`             | Localização da tabela                                                  | Restringe a varredura a um subdiretório específico dentro da localização da tabela (por exemplo, `'data/'` ou `'metadata/'`).                                                                            |
| `dry_run`    | `UInt64`             | `0`                                                                    | Quando `1`, identifica arquivos órfãos e retorna o resumo do resultado sem excluir nada de fato.                                                                                                         |

**Exemplos:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**Saída:**

O comando retorna uma tabela com as colunas `metric_name` e `metric_value`, mostrando a contagem de arquivos excluídos (ou que seriam excluídos no modo `dry&#95;run`) por categoria. As categorias de arquivos são classificadas com base em heurísticas aproximadas, usando convenções de nomenclatura de arquivos; arquivos que não correspondem a nenhum padrão específico são contabilizados por padrão em `deleted_data_files_count`:

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**Configurações:**

| Configuração                              | Tipo     | Padrão            | Descrição                                                                 |
| ----------------------------------------- | -------- | ----------------- | ------------------------------------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`           | Configuração de controle para habilitar o recurso (experimental).         |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 dias) | Limite padrão de `older_than`, em segundos, quando o argumento é omitido. |

:::note

* **Requer a versão 2 do formato Iceberg (ou superior).** Tabelas da versão 1 são rejeitadas porque não têm ponteiros `manifest-list` em snapshots, necessários para determinar com segurança o conjunto de arquivos referenciados. Executar o comando em uma tabela v1 retorna um erro `BAD_ARGUMENTS`.
* Requer que as configurações `allow_insert_into_iceberg` e `allow_iceberg_remove_orphan_files` estejam habilitadas
* Recomenda-se executar `expire_snapshots` antes de `remove_orphan_files`, para que os arquivos referenciados exclusivamente por snapshots expirados sejam removidos primeiro
* Use `dry_run = 1` para visualizar os arquivos órfãos antes da exclusão
* O limite `older_than` evita a exclusão de arquivos de gravações em andamento — o limite padrão de 3 dias oferece uma margem de segurança generosa
  :::

<div id="see-also">
  ## Veja também
</div>

* [Motor Iceberg](/pt-BR/engines/table-engines/integrations/iceberg.md)
* [Função de tabela de cluster Iceberg](/pt-BR/sql-reference/table-functions/icebergCluster.md)