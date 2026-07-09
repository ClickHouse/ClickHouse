---
description: 'Este motor fornece uma integração de somente leitura com tabelas Apache Iceberg
  existentes no Amazon S3, Azure, HDFS e com tabelas armazenadas localmente.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Motor de tabela Iceberg'
doc_type: 'referência'
---

:::warning
Recomendamos usar a [Função de tabela Iceberg](/pt-BR/sql-reference/table-functions/iceberg.md) para trabalhar com dados Iceberg no ClickHouse. Atualmente, a Função de tabela Iceberg oferece funcionalidade suficiente, com uma interface parcial de somente leitura para tabelas Iceberg.

O Motor de tabela Iceberg está disponível, mas pode ter limitações. O ClickHouse não foi originalmente projetado para dar suporte a tabelas com esquemas alterados externamente, o que pode afetar a funcionalidade do Motor de tabela Iceberg. Como resultado, alguns recursos que funcionam com tabelas comuns podem não estar disponíveis ou podem não funcionar corretamente, especialmente ao usar o analisador antigo.

Para obter a melhor compatibilidade, sugerimos usar a Função de tabela Iceberg enquanto continuamos a aprimorar o suporte ao Motor de tabela Iceberg.
:::

Este motor fornece uma integração de somente leitura com tabelas Apache [Iceberg](https://iceberg.apache.org/) existentes no Amazon S3, Azure, HDFS e com tabelas armazenadas localmente.

<div id="create-table">
  ## Criar tabela
</div>

Observe que a tabela Iceberg já deve existir no armazenamento; este comando não recebe parâmetros de DDL para criar uma nova tabela.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Argumentos do motor
</div>

A descrição dos argumentos é a mesma que a dos argumentos dos motores `S3`, `AzureBlobStorage`, `HDFS` e `File`, respectivamente.
`format` indica o formato dos arquivos de dados na tabela Iceberg.

Para `IcebergS3`, é possível usar o parâmetro opcional `extra_credentials` para passar um `role_arn` para acesso baseado em função no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

Os parâmetros do motor podem ser especificados usando [Named Collections](../../../operations/named-collections.md)

<div id="example">
  ### Exemplo
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Usando coleções nomeadas:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## Aliases
</div>

O motor de tabela `Iceberg` detecta automaticamente o backend de armazenamento com base na configuração `disk` e direciona para `IcebergS3`, `IcebergAzure` ou `IcebergLocal`, conforme apropriado. Quando nenhum `disk` é especificado, o padrão é a implementação `IcebergS3`.

<div id="data-types">
  ## Tipos de dados
</div>

A tabela a seguir mostra como os tipos de dados do Iceberg são mapeados para os tipos de dados do ClickHouse durante a inferência de esquema (para leitura).

<div id="primitive-types">
  ### Tipos primitivos
</div>

| Tipo do Iceberg    | Tipo do ClickHouse     | Observações                                            |
| ------------------ | ---------------------- | ------------------------------------------------------ |
| `boolean`          | `Bool`                 |                                                        |
| `int`              | `Int32`                |                                                        |
| `long`, `bigint`   | `Int64`                |                                                        |
| `float`            | `Float32`              |                                                        |
| `double`           | `Float64`              |                                                        |
| `date`             | `Date32`               |                                                        |
| `time`             | `Int64`                | Microssegundos desde a meia-noite                      |
| `timestamp`        | `DateTime64(6)`        | Microssegundos, sem fuso horário                       |
| `timestamptz`      | `DateTime64(6, 'UTC')` | Microssegundos, fuso horário UTC                       |
| `timestamp_ns`     | `DateTime64(9)`        | Nanossegundos, sem fuso horário (apenas no Iceberg v3) |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | Nanossegundos, fuso horário UTC (apenas no Iceberg v3) |
| `string`, `binary` | `String`               |                                                        |
| `uuid`             | `UUID`                 |                                                        |
| `fixed(N)`         | `FixedString(N)`       |                                                        |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                                        |

<div id="complex-types">
  ### Tipos complexos
</div>

| Tipo do Iceberg | Tipo do ClickHouse |
| --------------- | ------------------ |
| `list`          | `Array`            |
| `map`           | `Map`              |
| `struct`        | `Tuple`            |

<div id="schema-evolution">
  ## Evolução de esquema
</div>

O ClickHouse oferece suporte à leitura de tabelas Iceberg cujo esquema evoluiu ao longo do tempo. Isso inclui tabelas em que colunas foram adicionadas, removidas ou reordenadas, bem como colunas alteradas de obrigatórias para Nullable. Além disso, as seguintes conversões de tipo são compatíveis:

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) onde P&#39; &gt; P.

Atualmente, não é possível alterar estruturas aninhadas nem os tipos dos elementos em arrays e maps.

Para ler uma tabela cujo esquema foi alterado após sua criação com inferência dinâmica de esquema, defina allow&#95;dynamic&#95;metadata&#95;for&#95;data&#95;lakes = true ao criar a tabela.

<div id="partition-pruning">
  ## Poda de partições
</div>

O ClickHouse oferece suporte à poda de partições durante consultas SELECT em tabelas Iceberg, o que ajuda a otimizar o desempenho das consultas ao ignorar arquivos de dados irrelevantes. Para habilitar a poda de partições, defina `use_iceberg_partition_pruning = 1`. Para mais informações sobre poda de partições no Iceberg, acesse https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Viagem no tempo
</div>

O ClickHouse oferece suporte a viagens no tempo para tabelas Iceberg, permitindo consultar dados históricos usando um timestamp específico ou um ID de snapshot.

<div id="deleted-rows">
  ## Processamento de tabelas com linhas excluídas
</div>

O ClickHouse oferece suporte à leitura de tabelas Iceberg que usam os seguintes métodos de exclusão:

* [Exclusões por posição](https://iceberg.apache.org/spec/#position-delete-files)
* [Exclusões por igualdade](https://iceberg.apache.org/spec/#equality-delete-files) (suportadas a partir da versão 25.8+)

O seguinte método de exclusão **não é suportado**:

* [Vetores de exclusão](https://iceberg.apache.org/spec/#deletion-vectors) (introduzido na v3)

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
  * Algum tipo de compactação de dados é realizado

* **Alterações de esquema normalmente não criam snapshots** - Isso resulta em comportamentos importantes ao usar viagem no tempo com tabelas que passaram por evolução de esquema.

<div id="example-scenarios">
  ### Cenários de exemplo
</div>

Todos os cenários usam Spark porque o CH ainda não oferece suporte à gravação em tabelas Iceberg.

<div id="scenario-1">
  #### Cenário 1: Alterações de esquema sem novos snapshots
</div>

Considere a seguinte sequência de operações:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

* Em ts1 &amp; ts2: Apenas as duas colunas originais aparecem
* Em ts3: As três colunas aparecem, com NULL no preço da primeira linha

<div id="scenario-2">
  #### Cenário 2: Diferenças entre o esquema histórico e o atual
</div>

Uma consulta de viagem no tempo no momento atual pode mostrar um esquema diferente do da tabela atual:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

Isso acontece porque `ALTER TABLE` não cria um novo snapshot; para a tabela atual, o Spark usa o valor de `schema_id` do arquivo de metadados mais recente, e não de um snapshot.

<div id="scenario-3">
  #### Cenário 3: Diferenças entre o esquema histórico e o atual
</div>

A segunda é que, ao usar viagem no tempo, não é possível obter o estado da tabela antes que qualquer dado tenha sido gravado nela:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

No ClickHouse, o comportamento é o mesmo do Spark. Você pode pensar nas consultas Select do Spark como consultas Select do ClickHouse, e tudo funcionará da mesma forma.

<div id="metadata-file-resolution">
  ## Resolução do arquivo de metadados
</div>

Ao usar o motor de tabela `Iceberg` no ClickHouse, o sistema precisa localizar o arquivo metadata.json correto que descreve a estrutura da tabela Iceberg. Veja como funciona esse processo de resolução:

<div id="candidate-search">
  ### Busca de candidatos
</div>

1. **Especificação direta do caminho**:

* Se você definir `iceberg_metadata_file_path`, o sistema usará esse caminho exato, combinando-o com o caminho do diretório da tabela Iceberg.
* Quando essa configuração é fornecida, todas as outras configurações de resolution são ignoradas.

2. **Correspondência de UUID da tabela**:

* Se `iceberg_metadata_table_uuid` for especificado, o sistema irá:
  * Considerar apenas os arquivos `.metadata.json` no diretório `metadata`
  * Filtrar os arquivos que contêm um campo `table-uuid` correspondente ao UUID especificado (sem diferenciar maiúsculas de minúsculas)

3. **Busca padrão**:

* Se nenhuma das configurações acima for fornecida, todos os arquivos `.metadata.json` no diretório `metadata` se tornam candidatos

<div id="most-recent-file">
  ### Selecionando o arquivo mais recente
</div>

Após identificar os arquivos candidatos usando as regras acima, o sistema determina qual deles é o mais recente:

* Se `iceberg_recent_metadata_file_by_last_updated_ms_field` estiver habilitado:
  * O arquivo com o maior valor de `last-updated-ms` é selecionado

* Caso contrário:
  * O arquivo com o número de versão mais alto é selecionado
  * (A versão aparece como `V` em nomes de arquivo no formato `V.metadata.json` ou `V-uuid.metadata.json`)

**Observação**: Todas as configurações mencionadas (salvo indicação explícita em contrário) são configurações no nível da engine e devem ser especificadas durante a criação da tabela, como mostrado abaixo:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Nota**: Embora os catálogos do Iceberg normalmente cuidem da resolução de metadados, o motor de tabela `Iceberg` no ClickHouse interpreta diretamente arquivos armazenados no S3 como tabelas Iceberg, por isso é importante entender essas regras de resolução.

<div id="data-cache">
  ## Cache de dados
</div>

O motor de tabela `Iceberg` e a função de tabela oferecem suporte a cache de dados, assim como os armazenamentos `S3`, `AzureBlobStorage` e `HDFS`. Veja [aqui](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="metadata-cache">
  ## Cache de metadados
</div>

O motor de tabela `Iceberg` e a função de tabela oferecem suporte a um cache de metadados que armazena informações de arquivos de manifesto, da lista de manifestos e do JSON de metadados. O cache é armazenado na memória. Esse recurso é controlado pela configuração `use_iceberg_metadata_files_cache`, que é habilitada por padrão.

<div id="async-metadata-prefetch">
  ## Pré-busca assíncrona de metadados
</div>

A pré-busca assíncrona de metadados pode ser habilitada na criação da tabela `Iceberg` definindo `iceberg_metadata_async_prefetch_period_ms`. Se esse valor for definido como 0 (padrão) ou se o cache de metadados não estiver habilitado, a pré-busca assíncrona será desabilitada.
Para habilitar esse recurso, deve ser fornecido um valor diferente de zero em milissegundos. Ele representa o intervalo entre os ciclos de pré-busca.

Se estiver habilitado, o servidor executará uma operação recorrente em segundo plano para listar o catálogo remoto e detectar uma nova versão dos metadados. Em seguida, ele a analisará e percorrerá recursivamente o snapshot, buscando os arquivos ativos de lista de manifestos e de manifesto.
Os arquivos já disponíveis no cache de metadados não serão baixados novamente. Ao final de cada ciclo de pré-busca, o snapshot de metadados mais recente estará disponível no cache de metadados.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

Para aproveitar ao máximo o prefetching assíncrono de metadados em operações de leitura, o parâmetro `iceberg_metadata_staleness_ms` deve ser especificado como parâmetro de consulta ou de sessão. Por padrão (0 - não especificado), no contexto de cada consulta, o servidor buscará os metadados mais recentes no catálogo remoto.
Ao especificar uma tolerância à obsolescência dos metadados, o servidor pode usar a versão em cache do snapshot de metadados sem consultar o catálogo remoto. Se houver uma versão dos metadados no cache e ela tiver sido baixada dentro da janela de obsolescência especificada, ela será usada para processar a consulta.
Caso contrário, a versão mais recente será buscada no catálogo remoto.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**Nota**: A prefetching assíncrona de metadados é executada em `ICEBERG_SCEDULE_POOL`, que é o pool de threads do lado do servidor para operações em segundo plano em tabelas `Iceberg` ativas. O tamanho desse pool de threads é controlado pelo parâmetro de configuração do servidor `iceberg_background_schedule_pool_size` (o padrão é 10).

**Nota**: Atualmente, espera-se que o tamanho do cache de metadados seja suficiente para armazenar integralmente o snapshot de metadados mais recente de todas as tabelas ativas, se a prefetching assíncrona estiver habilitada.

<div id="see-also">
  ## Veja também
</div>

* [função de tabela Iceberg](/pt-BR/sql-reference/table-functions/iceberg.md)