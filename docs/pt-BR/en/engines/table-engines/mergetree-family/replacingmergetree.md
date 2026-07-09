---
description: 'difere de MergeTree porque remove entradas duplicadas com o mesmo valor
  da chave de ordenação (seção da tabela `ORDER BY`, não `PRIMARY KEY`).'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'motor de tabela ReplacingMergeTree'
doc_type: 'reference'
---

O motor difere de [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree) porque remove entradas duplicadas com o mesmo valor da [chave de ordenação](../../../engines/table-engines/mergetree-family/mergetree.md) (seção da tabela `ORDER BY`, não `PRIMARY KEY`).

A desduplicação de dados ocorre apenas durante uma mesclagem. A mesclagem acontece em segundo plano, em um momento imprevisível, portanto você não pode planejá-la. Parte dos dados pode permanecer sem processamento. Embora seja possível executar uma mesclagem não agendada usando a consulta `OPTIMIZE`, não dependa disso, porque a consulta `OPTIMIZE` lerá e gravará uma grande quantidade de dados.

Assim, `ReplacingMergeTree` é adequado para eliminar dados duplicados em segundo plano a fim de economizar espaço, mas não garante a ausência de duplicatas.

:::note
Um guia detalhado sobre ReplacingMergeTree, incluindo melhores práticas e como otimizar o desempenho, está disponível [aqui](/pt-BR/guides/replacing-merge-tree).
:::

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obter uma descrição dos parâmetros da solicitação, consulte a [descrição da instrução](../../../sql-reference/statements/create/table.md).

:::note
A unicidade das linhas é determinada pela seção `ORDER BY` da tabela, não pela `PRIMARY KEY`.
:::

<div id="replacingmergetree-parameters">
  ## Parâmetros do ReplacingMergeTree
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — coluna com o número da versão. Tipo `UInt*`, `Date`, `DateTime` ou `DateTime64`. Parâmetro opcional.

Durante a mesclagem, o `ReplacingMergeTree` mantém apenas uma das linhas com a mesma chave de ordenação:

* A última na seleção, se `ver` não estiver definido. Uma seleção é um conjunto de linhas em um conjunto de partes que participam da mesclagem. A parte criada mais recentemente (a última inserção) será a última na seleção. Assim, após a desduplicação, permanecerá a última linha da inserção mais recente para cada chave de ordenação distinta.
* A que tiver a versão máxima, se `ver` for especificado. Se `ver` for o mesmo para várias linhas, será usada para elas a regra de &quot;se `ver` não estiver especificado&quot;, isto é, a linha inserida mais recentemente permanecerá.

Exemplo:

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — Nome de uma coluna usada durante uma mesclagem para determinar se os dados nesta linha representam o estado ou devem ser excluídos; `1` é uma linha &quot;excluída&quot;, `0` é uma linha de &quot;estado&quot;.

Tipo de dados da coluna — `UInt8`.

:::note
`is_deleted` só pode ser habilitado quando `ver` é usado.

Independentemente da operação nos dados, a versão deve ser incrementada. Se duas linhas inseridas tiverem o mesmo número de versão, a última linha inserida será mantida.

Por padrão, o ClickHouse manterá a última linha de uma chave, mesmo que essa linha seja uma linha de exclusão. Isso é feito para que quaisquer linhas futuras com versões inferiores possam
ser inseridas com segurança, e a linha de exclusão ainda assim seja aplicada.

Para remover permanentemente essas linhas de exclusão, habilite a configuração da tabela `allow_experimental_replacing_merge_with_cleanup` e então:

1. Defina as configurações da tabela `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`, `min_age_to_force_merge_on_partition_only` e `min_age_to_force_merge_seconds`. Se todas as partes em uma partição forem mais antigas que `min_age_to_force_merge_seconds`, o ClickHouse fará a mesclagem de
   todas elas em uma única parte e removerá quaisquer linhas de exclusão.

2. Execute manualmente `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`.
   :::

Exemplo:

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

<div id="query-clauses">
  ## Cláusulas de consulta
</div>

Ao criar uma tabela `ReplacingMergeTree`, são necessárias as mesmas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) exigidas ao criar uma tabela `MergeTree`.

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
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  Todos os parâmetros, exceto `ver`, têm o mesmo significado que os de `MergeTree`.

  * `ver` - coluna com a versão. Parâmetro opcional. Para ver uma descrição, consulte o texto acima.
</details>

<div id="query-time-de-duplication--final">
  ## Desduplicação em tempo de consulta &amp; FINAL
</div>

No momento da mesclagem, o ReplacingMergeTree identifica linhas duplicadas usando os valores das colunas `ORDER BY` (usadas para criar a tabela) como identificador único e mantém apenas a versão mais alta. No entanto, isso oferece apenas correção eventual — não garante que as linhas serão desduplicadas, e você não deve depender disso. Portanto, as consultas podem produzir respostas incorretas, já que atualizações e linhas excluídas podem ser consideradas nas consultas.

Para obter respostas corretas, os usuários precisam complementar as mesclagens em segundo plano com desduplicação em tempo de consulta e remoção de linhas excluídas. Isso pode ser feito usando o operador `FINAL`. Por exemplo, considere o exemplo a seguir:

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

Consultar sem `FINAL` resulta em uma contagem incorreta (o resultado exato varia conforme as mesclagens):

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Adicionar FINAL gera um resultado correto:

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Para mais detalhes sobre `FINAL`, inclusive sobre como otimizar seu desempenho, recomendamos a leitura do nosso [guia detalhado sobre ReplacingMergeTree](/pt-BR/guides/replacing-merge-tree).