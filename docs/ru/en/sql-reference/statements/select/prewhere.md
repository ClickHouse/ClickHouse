---
description: 'Документация по предложению PREWHERE'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'Предложение PREWHERE'
doc_type: 'справочник'
---

Prewhere — это оптимизация, которая позволяет применять фильтрацию эффективнее. Она включена по умолчанию, даже если предложение `PREWHERE` не указано явно. Она работает, автоматически перенося часть условия [WHERE](../../../sql-reference/statements/select/where.md) на этап prewhere. Назначение предложения `PREWHERE` — лишь управлять этой оптимизацией, если вы считаете, что можете сделать это лучше, чем по умолчанию.

При оптимизации prewhere сначала читаются только те столбцы, которые нужны для вычисления выражения prewhere. Затем читаются остальные столбцы, необходимые для выполнения оставшейся части запроса, но только для тех блоков, где выражение prewhere имеет значение `true` хотя бы для некоторых строк. Если есть много блоков, где выражение prewhere имеет значение `false` для всех строк, и для prewhere требуется меньше столбцов, чем для других частей запроса, это часто позволяет считывать с диска значительно меньше данных при выполнении запроса.

<div id="controlling-prewhere-manually">
  ## Ручное управление PREWHERE
</div>

Это предложение имеет тот же смысл, что и предложение `WHERE`. Разница заключается в том, какие данные считываются из таблицы. `PREWHERE` имеет смысл задавать вручную для условий фильтрации, которые затрагивают лишь небольшую часть столбцов в запросе, но при этом обеспечивают сильную фильтрацию данных. Это уменьшает объем данных, которые нужно прочитать.

В запросе можно одновременно указать `PREWHERE` и `WHERE`. В этом случае `PREWHERE` выполняется перед `WHERE`.

Если значение настройки [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) равно 0, эвристики, автоматически переносящие части выражений из `WHERE` в `PREWHERE`, отключаются.

Если в запросе используется модификатор [FINAL](/ru/sql-reference/statements/select/from#final-modifier), оптимизация `PREWHERE` не всегда корректна. Она включается, только если включены обе настройки: [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) и [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final).

:::note
Секция `PREWHERE` выполняется до `FINAL`, поэтому результаты запросов `FROM ... FINAL` могут искажаться при использовании `PREWHERE` с полями, не входящими в секцию `ORDER BY` таблицы.
:::

<div id="limitations">
  ## Ограничения
</div>

`PREWHERE` поддерживается только в таблицах семейства [*MergeTree](../../../engines/table-engines/mergetree-family/index.md).

<div id="example">
  ## Пример
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```