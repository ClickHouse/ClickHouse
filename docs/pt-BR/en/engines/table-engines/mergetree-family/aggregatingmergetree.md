---
description: 'Substitui todas as linhas com a mesma chave primária (ou, mais precisamente, com
  a mesma [chave de ordenação](../../../engines/table-engines/mergetree-family/mergetree.md))
  por uma única linha (dentro de uma única parte de dados) que armazena uma combinação
  de estados de funções de agregação.'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'Motor de tabela AggregatingMergeTree'
doc_type: 'reference'
---

Esse motor herda de [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree), alterando a lógica de mesclagem das partes de dados. O ClickHouse substitui todas as linhas com a mesma chave primária (ou, mais precisamente, com a mesma [chave de ordenação](../../../engines/table-engines/mergetree-family/mergetree.md)) por uma única linha (dentro de uma única parte de dados) que armazena uma combinação de estados de funções de agregação.

Você pode usar tabelas `AggregatingMergeTree` para agregação incremental de dados, inclusive em visões materializadas agregadas.

Veja no vídeo abaixo um exemplo de uso do AggregatingMergeTree e de funções de agregação:

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="Estados de agregação no ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

O motor processa todas as colunas com os seguintes tipos:

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

É apropriado usar `AggregatingMergeTree` se ele reduzir o número de linhas em várias ordens de grandeza.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

Para obter uma descrição dos parâmetros da requisição, consulte a [descrição da requisição](../../../sql-reference/statements/create/table.md).

**Cláusulas da consulta**

Ao criar uma tabela `AggregatingMergeTree`, são exigidas as mesmas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) da criação de uma tabela `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para criar uma tabela</summary>

  :::note
  Não use este método em novos projetos e, se possível, migre os projetos antigos para o método descrito acima.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  Todos os parâmetros têm o mesmo significado que em `MergeTree`.
</details>

<div id="select-and-insert">
  ## SELECT e INSERT
</div>

Para inserir dados, use a consulta [INSERT SELECT](../../../sql-reference/statements/insert-into.md) com funções de agregação `-State-`.
Ao selecionar dados de uma tabela `AggregatingMergeTree`, use a cláusula `GROUP BY` e as mesmas funções de agregação usadas na inserção dos dados, mas com o sufixo `-Merge`.

Nos resultados da consulta `SELECT`, os valores do tipo `AggregateFunction` têm uma representação binária específica da implementação em todos os formatos de saída do ClickHouse. Por exemplo, se você exportar dados no formato `TabSeparated` com uma consulta `SELECT`, essa exportação poderá ser importada novamente usando uma consulta `INSERT`.

<div id="example-of-an-aggregated-materialized-view">
  ## Exemplo de uma visão materializada agregada
</div>

O exemplo a seguir parte do pressuposto de que você tem um banco de dados chamado `test`. Crie-o, caso ainda não exista, usando o comando abaixo:

```sql
CREATE DATABASE test;
```

Agora, crie a tabela `test.visits` com os dados brutos:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

Em seguida, você precisa de uma tabela `AggregatingMergeTree` que armazenará `AggregationFunction`s para acompanhar o número total de visitas e o número de usuários únicos.

Crie uma visão materializada `AggregatingMergeTree` que observa a tabela `test.visits` e usa o tipo [`AggregateFunction`](/pt-BR/sql-reference/data-types/aggregatefunction):

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

Crie uma visão materializada que popula `test.agg_visits` a partir de `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

Insira dados na tabela `test.visits`:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

Os dados são inseridos tanto em `test.visits` quanto em `test.agg_visits`.

Para obter os dados agregados, execute uma consulta como `SELECT ... GROUP BY ...` na visão materializada `test.visits_mv`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

Adicione mais dois registros a `test.visits`, mas desta vez tente usar um `timestamp` diferente para um dos registros:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

Execute novamente a consulta `SELECT`, que retornará a seguinte saída:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

Em alguns casos, pode ser desejável evitar a pré-agregação de linhas no momento da inserção para transferir o custo da agregação do momento da inserção
para o momento da mesclagem. Normalmente, é necessário incluir as colunas que não fazem parte da agregação na cláusula `GROUP BY`
da definição da visão materializada para evitar um erro. No entanto, você pode usar a função [`initializeAggregation`](/pt-BR/sql-reference/functions/other-functions#initializeAggregation)
com a configuração `optimize_on_insert = 0` (ela é ativada por padrão) para fazer isso. Nesse caso, o uso de `GROUP BY`
deixa de ser necessário:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
Ao usar `initializeAggregation`, um estado de agregação é criado para cada linha individualmente, sem agrupamento.
Cada linha de origem produz uma linha na visão materializada, e a agregação propriamente dita acontece depois, quando o
`AggregatingMergeTree` mescla as partes. Isso só é válido se `optimize_on_insert = 0`.
:::

<div id="tuple-element-aggregation">
  ## Agregação de elementos de Tuple
</div>

Quando a configuração `allow_tuple_element_aggregation` está habilitada, as colunas `Tuple` são achatadas recursivamente para que cada elemento terminal participe da agregação de forma independente. Isso significa que as subcolunas `AggregateFunction` ou `SimpleAggregateFunction` dentro de um `Tuple` são agregadas de acordo com suas respectivas funções, como se fossem colunas de nível superior.

As subcolunas que pertencem a um `Tuple` na chave de ordenação são excluídas da agregação. As subcolunas não agregadas são tratadas como colunas comuns (o primeiro valor delas é mantido).

:::note
Essa configuração é imutável e deve ser especificada no momento da criação da tabela.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits` é agregado usando `sum` (100 + 200 = 300), enquanto `unique_users` é agregado usando `max` (max(5, 8) = 8).

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Usando combinadores de agregação no ClickHouse para arrays, maps e estados](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)