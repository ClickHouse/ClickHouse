---
description: 'Documentação da instrução ALTER TABLE ... MODIFY QUERY'
sidebar_label: 'VIEW'
sidebar_position: 50
slug: /sql-reference/statements/alter/view
title: 'Instrução ALTER TABLE ... MODIFY QUERY'
doc_type: 'reference'
---

Você pode modificar a consulta `SELECT` especificada quando uma [visão materializada](/pt-BR/sql-reference/statements/create/view#materialized-view) foi criada com a instrução `ALTER TABLE ... MODIFY QUERY`, sem interromper o processo de ingestão.

Este comando se destina a alterar uma visão materializada criada com a cláusula `TO [db.]name`. Ele não altera a estrutura da tabela de armazenamento subjacente nem a definição das colunas da visão materializada. Por isso, a aplicação desse comando é muito limitada para visões materializadas criadas sem a cláusula `TO [db.]name`.

**Exemplo com tabela TO**

```sql
CREATE TABLE events (ts DateTime, event_type String)
ENGINE = MergeTree ORDER BY (event_type, ts);

CREATE TABLE events_by_day (ts DateTime, event_type String, events_cnt UInt64)
ENGINE = SummingMergeTree ORDER BY (event_type, ts);

CREATE MATERIALIZED VIEW mv TO events_by_day AS
SELECT toStartOfDay(ts) ts, event_type, count() events_cnt
FROM events
GROUP BY ts, event_type;

INSERT INTO events
SELECT DATE '2020-01-01' + interval number * 900 second,
       ['imp', 'click'][number%2+1]
FROM numbers(100);

SELECT ts, event_type, sum(events_cnt)
FROM events_by_day
GROUP BY ts, event_type
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─sum(events_cnt)─┐
│ 2020-01-01 00:00:00 │ click      │              48 │
│ 2020-01-01 00:00:00 │ imp        │              48 │
│ 2020-01-02 00:00:00 │ click      │               2 │
│ 2020-01-02 00:00:00 │ imp        │               2 │
└─────────────────────┴────────────┴─────────────────┘

-- Let's add the new measurement `cost`
-- and the new dimension `browser`.

ALTER TABLE events
  ADD COLUMN browser String,
  ADD COLUMN cost Float64;

-- Column do not have to match in a materialized view and TO
-- (destination table), so the next alter does not break insertion.

ALTER TABLE events_by_day
    ADD COLUMN cost Float64,
    ADD COLUMN browser String after event_type,
    MODIFY ORDER BY (event_type, ts, browser);

INSERT INTO events
SELECT Date '2020-01-02' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

-- New columns `browser` and `cost` are empty because we did not change Materialized View yet.

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬─cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │    0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │    0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │    0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │    0 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │    0 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │    0 │
└─────────────────────┴────────────┴─────────┴────────────┴──────┘

ALTER TABLE mv MODIFY QUERY
  SELECT toStartOfDay(ts) ts, event_type, browser,
  count() events_cnt,
  sum(cost) cost
  FROM events
  GROUP BY ts, event_type, browser;

INSERT INTO events
SELECT Date '2020-01-03' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬──cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │     0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │     0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │     0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │     0 │
│ 2020-01-03 00:00:00 │ click      │ firefox │         16 │  6.84 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ click      │ safary  │         16 │  9.82 │
│ 2020-01-03 00:00:00 │ click      │ chrome  │         16 │  5.63 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ imp        │ firefox │         16 │ 15.14 │
│ 2020-01-03 00:00:00 │ imp        │ safary  │         16 │  6.14 │
│ 2020-01-03 00:00:00 │ imp        │ chrome  │         16 │  7.89 │
│ 2020-01-04 00:00:00 │ click      │ safary  │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ click      │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ chrome  │          1 │   0.1 │
└─────────────────────┴────────────┴─────────┴────────────┴───────┘

-- !!! During `MODIFY ORDER BY` PRIMARY KEY was implicitly introduced.

SHOW CREATE TABLE events_by_day FORMAT TSVRaw

CREATE TABLE test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `browser` String,
    `events_cnt` UInt64,
    `cost` Float64
)
ENGINE = SummingMergeTree
PRIMARY KEY (event_type, ts)
ORDER BY (event_type, ts, browser)

-- !!! The columns' definition is unchanged but it does not matter, we are not querying
-- MATERIALIZED VIEW, we are querying TO (storage) table.
-- SELECT section is updated.

SHOW CREATE TABLE mv FORMAT TSVRaw;

CREATE MATERIALIZED VIEW test.mv TO test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `events_cnt` UInt64
) AS
SELECT
    toStartOfDay(ts) AS ts,
    event_type,
    browser,
    count() AS events_cnt,
    sum(cost) AS cost
FROM test.events
GROUP BY
    ts,
    event_type,
    browser
```

**Exemplo sem a tabela TO**

A aplicação é muito limitada, porque você só pode alterar a cláusula `SELECT` sem adicionar novas colunas.

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table;
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```

```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```

```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

<div id="alter-table--modify-refresh-statement">
  ## Instrução ALTER TABLE ... MODIFY REFRESH
</div>

`ALTER TABLE ... MODIFY REFRESH` altera os parâmetros de atualização de uma [view materializada atualizável](../create/view.md#refreshable-materialized-view), incluindo o agendamento, as dependências, a aleatorização e as [configurações de atualização](../create/view.md#refresh-settings).

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

O agendamento (`EVERY` ou `AFTER`) é obrigatório: a instrução substitui *todos* os parâmetros de atualização de uma só vez. Qualquer cláusula não especificada — `RANDOMIZE FOR`, `DEPENDS ON` ou `SETTINGS` — é removida ou redefinida para os valores padrão. Para alterar apenas as configurações de atualização, repita o agendamento atual.

```sql
-- Change the schedule.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change retry settings (schedule must be repeated).
ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Add or keep a dependency.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

Limitações:

* `ALTER TABLE ... MODIFY SETTING` não é compatível com visões materializadas; as configurações de atualização só podem ser alteradas por meio de `MODIFY REFRESH`.
* Não há suporte para adicionar nem remover `APPEND`.
* A configuração de atualização `all_replicas` não pode ser alterada após a criação da visão.

A lista completa das configurações de atualização está documentada em [Configurações de atualização](../create/view.md#refresh-settings). O status da atualização, incluindo as configurações atualmente aplicadas, pode ser visto em [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md).