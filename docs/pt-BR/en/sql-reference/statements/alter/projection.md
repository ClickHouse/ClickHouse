---
description: 'Documentação sobre manipulação de projeções'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'Projeções'
doc_type: 'reference'
---

Esta página explica o que são projeções, como usá-las e as várias opções para manipulá-las.

<div id="overview">
  ## Visão geral das projeções
</div>

As projeções armazenam dados em um formato que otimiza a execução de consultas. Esse recurso é útil para:

* Executar consultas em uma coluna que não faz parte da chave primária
* Pré-agregar colunas, o que reduz tanto o processamento quanto a E/S

Você pode definir uma ou mais projeções para uma tabela e, durante a análise da consulta, o ClickHouse selecionará a projeção com a menor quantidade de dados a serem lidos, sem modificar a consulta fornecida pelo usuário.

:::note[Uso de disco]
As projeções criam internamente uma nova tabela oculta, o que significa que mais E/S e espaço em disco serão necessários.
Por exemplo, se a projeção definir uma chave primária diferente, todos os dados da tabela original serão duplicados.
:::

Você pode ver mais detalhes técnicos sobre como as projeções funcionam internamente nesta [página](/pt-BR/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

<div id="examples">
  ## Uso de projeções
</div>

<div id="example-filtering-without-using-primary-keys">
  ### Exemplo de filtragem sem usar chaves primárias
</div>

Criando a tabela:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

Usando `ALTER TABLE`, podemos adicionar a projeção a uma tabela existente:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

Inserção dos dados:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

A Projeção permitirá filtrar por `user_name` rapidamente, mesmo que, na `Table` original, `user_name` não tenha sido definido como `PRIMARY_KEY`.
No momento da consulta, o ClickHouse determina que menos dados serão processados se a projeção for usada, pois os dados estão ordenados por `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

Para verificar se uma consulta está usando a projeção, podemos analisar a tabela `system.query_log`. No campo `projections`, temos o nome da projeção usada ou ele fica vazio se nenhuma tiver sido usada:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### Exemplo de consulta com pré-agregação
</div>

Crie a tabela com a projeção `projection_visits_by_user`:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

Insira os dados:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

Execute uma primeira consulta com `GROUP BY` usando o campo `user_agent`.
Esta consulta não usará a projeção definida, pois a pré-agregação não é compatível.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

Para usar a projeção, você pode executar consultas que selecionem parte ou a totalidade dos campos da pré-agregação e do `GROUP BY`:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

Como mencionado anteriormente, você pode consultar a tabela `system.query_log` para verificar se uma projeção foi usada.
O campo `projections` mostra o nome da projeção usada.
Ele ficará vazio se nenhuma projeção tiver sido usada:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### Criar e usar índices de projeção
</div>

Criando um [índice de projeção](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>Criando uma projeção com o campo `_part_offset` explícito</summary>

  Como alternativa, é possível criar índices de projeção usando a sintaxe a seguir (não recomendado):

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

Inserindo alguns dados de exemplo:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

O campo `_part_offset` preserva seu valor durante merges e mutações, o que o torna valioso para indexação secundária. Podemos aproveitar isso em consultas:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### Exemplo de projeção com cláusula `WHERE`
</div>

As projeções podem incluir uma cláusula `WHERE` para armazenar apenas um subconjunto de linhas. Isso é útil quando as consultas filtram com frequência por um predicado conhecido — a projeção materializa apenas as linhas correspondentes, reduzindo o armazenamento e melhorando o desempenho das consultas.

Criando uma tabela e adicionando uma projeção filtrada:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

Inserção de dados:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

Quando a cláusula `WHERE` de uma consulta **implica** a cláusula `WHERE` da projeção (ou seja, todas as condições do filtro da projeção também estão presentes no filtro da consulta), o otimizador pode usar automaticamente a projeção quando determina que isso é vantajoso:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

A verificação de implicação é conservadora — usa correspondência exata de conjunções na forma canônica da expressão. Ela pode não identificar algumas oportunidades válidas de otimização (por exemplo, implicações de intervalo), mas nunca produzirá resultados incorretos.

<div id="manipulating-projections">
  ## Manipulação de projeções
</div>

As operações a seguir com [projeções](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#projections) estão disponíveis:

<div id="add-projection">
  ### ADD PROJECTION
</div>

Use a instrução abaixo para adicionar uma definição de projeção aos metadados da tabela:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
Quando uma projeção define uma cláusula `WHERE`, apenas as linhas que correspondem ao predicado são materializadas. O otimizador pode usar essa projeção quando o `WHERE` da consulta implicar, do ponto de vista lógico, o `WHERE` da projeção, e quando a projeção for vantajosa para o plano da consulta. Isso se aplica tanto a projeções normais quanto agregadas.
:::

<div id="with-settings">
  #### Cláusula `WITH SETTINGS`
</div>

`WITH SETTINGS` define **configurações no nível da projeção** que personalizam a forma como a projeção armazena dados (por exemplo, `index_granularity` ou `index_granularity_bytes`).
Elas correspondem diretamente às **configurações de tabela do MergeTree**, mas se aplicam **somente a esta projeção**.

Exemplo:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

As configurações da projeção substituem as configurações efetivas da tabela para essa projeção, sujeitas às regras de validação (por exemplo, substituições inválidas ou incompatíveis serão rejeitadas).

<div id="drop-projection">
  ### DROP PROJECTION
</div>

Use a instrução abaixo para remover a descrição de uma projeção dos metadados de uma tabela e excluir os arquivos da projeção do disco.
Isso é implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

Use a instrução abaixo para recriar a projeção `name` na partição `partition_name`.
Isso é implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

Use a instrução abaixo para excluir arquivos de projeção do disco sem remover a descrição.
Isso é implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

Os comandos `ADD`, `DROP` e `CLEAR` são leves no sentido de que apenas alteram metadados ou removem arquivos.
Além disso, eles são replicados e sincronizam os metadados da projeção via ClickHouse Keeper ou ZooKeeper.

:::note
A manipulação de projeções é suportada apenas para tabelas com motor [`*MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree.md) (incluindo variantes [replicadas](/pt-BR/engines/table-engines/mergetree-family/replication.md)).
:::

<div id="control-projections-merges">
  ### Controlando o comportamento de mesclagem das projeções
</div>

Quando você executa uma consulta, o ClickHouse escolhe entre ler da tabela original ou de uma de suas projeções.
A decisão de ler da tabela original ou de uma de suas projeções é tomada individualmente para cada parte da tabela.
Em geral, o ClickHouse procura ler o mínimo possível de dados e emprega alguns recursos para identificar a melhor parte para leitura, por exemplo, fazendo sampling da chave primária de uma parte.
Em alguns casos, partes da tabela de origem não têm partes de projeção correspondentes.
Isso pode acontecer, por exemplo, porque a criação de uma projeção para uma tabela em SQL é “lazy” por padrão — ela afeta apenas os dados inseridos posteriormente, mas mantém as partes existentes inalteradas.

Como uma das projeções já contém os valores agregados pré-computados, o ClickHouse tenta ler das partes de projeção correspondentes para evitar agregar novamente durante a execução da consulta. Se uma parte específica não tiver a parte de projeção correspondente, a execução da consulta recorre à parte original.

Mas o que acontece se as linhas da tabela original mudarem de forma não trivial devido a mesclagens em segundo plano não triviais de partes de dados?
Por exemplo, suponha que a tabela seja armazenada usando o mecanismo de tabela `ReplacingMergeTree`.
Se a mesma linha for detectada em várias partes de entrada durante a mesclagem, apenas a versão mais recente da linha (da parte inserida mais recentemente) será mantida, enquanto todas as versões anteriores serão descartadas.

Da mesma forma, se a tabela for armazenada usando o mecanismo de tabela `AggregatingMergeTree`, a operação de mesclagem poderá consolidar linhas iguais nas partes de entrada (com base nos valores da chave primária) em uma única linha para atualizar estados de agregação parciais.

Antes do ClickHouse v24.8, as partes de projeção ou ficavam silenciosamente dessincronizadas em relação aos dados principais, ou determinadas operações, como updates e deletes, simplesmente não podiam ser executadas, já que o banco de dados gerava automaticamente uma exceção se a tabela tivesse projeções.

Desde a v24.8, uma nova configuração no nível da tabela [`deduplicate_merge_projection_mode`](/pt-BR/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode) controla o comportamento caso as operações em segundo plano não triviais de mesclagem mencionadas acima ocorram em partes da tabela original.

Mutações de exclusão são outro exemplo de operações de mesclagem de partes que descartam linhas nas partes da tabela original. Desde a v24.7, também temos uma configuração para controlar o comportamento em relação às mutações de exclusão acionadas por exclusões leves: [`lightweight_mutation_projection_mode`](/pt-BR/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode).

Abaixo estão os possíveis valores de `deduplicate_merge_projection_mode` e `lightweight_mutation_projection_mode`:

* `throw` (padrão): Uma exceção é gerada, impedindo que as partes de projeção fiquem dessincronizadas.
* `drop`: As partes da tabela de projeção afetadas são descartadas. As consultas recorrerão à parte da tabela original para essas partes de projeção afetadas.
* `rebuild`: A parte de projeção afetada é reconstruída para permanecer consistente com os dados da parte da tabela original.

<div id="limitations">
  ## Limitações
</div>

Não é possível usar uma coluna `ALIAS` na cláusula `ORDER BY` da projeção. Por exemplo:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

As colunas `ALIAS` não são armazenadas fisicamente e são calculadas em tempo de consulta, portanto não estão disponíveis durante a etapa de gravação da projection part, quando a expressão de ordenação é avaliada.

Em vez disso, use colunas `MATERIALIZED` ou defina a expressão diretamente:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## Veja também
</div>

* [&quot;Controle de projeções durante mesclagens&quot; (post do blog)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;Projeções&quot; (guia)](/pt-BR/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;Visões materializadas versus projeções&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)