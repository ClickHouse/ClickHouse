---
description: 'The BigQuery engine allows reading from and writing to a table in Google
  BigQuery.'
sidebar_label: 'BigQuery'
sidebar_position: 39
slug: /engines/table-engines/integrations/bigquery
title: 'BigQuery table engine'
doc_type: 'reference'
---

# BigQuery table engine {#bigquery-table-engine}

The `BigQuery` engine allows reading from and writing to a table in [Google BigQuery](https://cloud.google.com/bigquery), including public datasets.

Reading uses the BigQuery REST API (`tabledata.list`), so only native tables can be read (views, materialized views and external tables cannot). Writing uses streaming inserts (`tabledata.insertAll`), which requires billing to be enabled for the project.

Writes are not atomic: a large `INSERT` is sent in batches (at most 500 rows per request, and also split to stay under BigQuery's 10 MB request-size limit), and if a later batch is rejected after earlier batches were accepted, the already-accepted rows stay committed in BigQuery while the query reports an error. Each row carries a stable `insertId` (derived from the query id and the row's ordinal position) so that BigQuery best-effort deduplicates retried rows; because the `insertId` depends on the ordinal position, re-running the same `INSERT` deduplicates only when it presents the rows in the same order (for example single-threaded, with `max_threads = 1` and `max_insert_threads = 1`). See the [`bigquery` table function limitations](../../../sql-reference/table-functions/bigquery.md#limitations) for details.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
[(
    name1 [type1],
    name2 [type2],
    ...
)]
ENGINE = BigQuery(project, dataset, table[, access_token][, key = value, ...])
```

The column list is optional: when omitted, the structure is inferred from the BigQuery table schema. When specified, the columns can be a subset of the BigQuery columns, and each column must be declared with the exact type the BigQuery schema maps to (see the [data type mapping](../../../sql-reference/table-functions/bigquery.md#data-type-mapping)). A `NULLABLE` `RECORD` is mapped to `Nullable(Tuple(...))` so `NULL` records round-trip losslessly; creating such a table (whether the structure is inferred or declared explicitly) requires the `enable_nullable_tuple_type` setting, as for any `Nullable(Tuple)` column. When declaring columns explicitly, a `RECORD` field may instead be declared as a plain `Tuple(...)` to avoid the setting, at the cost of coercing a whole-record `NULL` to a default tuple; the engine accepts a declared type that differs from the inferred type only by `Nullable` wrappers around a `Tuple`.

**Engine parameters**

- `project` — The Google Cloud project that owns the dataset.
- `dataset` — The dataset name.
- `table` — The table name.
- `access_token` — An OAuth 2.0 access token (optional positional argument).

The parameters can also be passed as a [named collection](/operations/named-collections) with `key = value` overrides. See the [`bigquery` table function](../../../sql-reference/table-functions/bigquery.md#arguments) for the full list of keys and the description of the [authentication methods](../../../sql-reference/table-functions/bigquery.md#authentication). Exactly one authentication method must be provided; for a permanent table a `service_account_key` or a `refresh_token` is preferable to an `access_token`, because access tokens expire within an hour.

## Usage example {#usage-example}

```sql
CREATE TABLE shakespeare
ENGINE = BigQuery('bigquery-public-data', 'samples', 'shakespeare',
                  service_account_key = '{"type": "service_account", ...}');

SELECT word, word_count FROM shakespeare ORDER BY word_count DESC LIMIT 3;

CREATE TABLE events (id Int64, payload Nullable(String))
ENGINE = BigQuery('my-project', 'my_dataset', 'events',
                  service_account_key = '{"type": "service_account", ...}');

INSERT INTO events VALUES (1, 'started');
```

## Related {#related}

- [`bigquery` table function](../../../sql-reference/table-functions/bigquery.md)
