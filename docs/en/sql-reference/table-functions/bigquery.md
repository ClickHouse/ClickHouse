---
description: 'Allows reading from and writing to a table in Google BigQuery, with
  automatic schema inference.'
sidebar_label: 'bigquery'
sidebar_position: 22
slug: /sql-reference/table-functions/bigquery
title: 'bigquery'
doc_type: 'reference'
---

# bigquery table function {#bigquery-table-function}

Allows `SELECT` and `INSERT` queries to be performed on a table in [Google BigQuery](https://cloud.google.com/bigquery), including public datasets. The table structure is inferred from the BigQuery table schema automatically.

Reading uses the BigQuery REST API (`tabledata.list`), so only native tables can be read (views, materialized views and external tables cannot). Writing uses streaming inserts (`tabledata.insertAll`), which requires billing to be enabled for the project.

## Syntax {#syntax}

```sql
bigquery(project, dataset, table[, access_token][, key = value, ...])
bigquery(named_collection[, key = value, ...])
```

## Arguments {#arguments}

| Argument       | Description                                                                                     |
|----------------|-------------------------------------------------------------------------------------------------|
| `project`      | The Google Cloud project that owns the dataset. For public datasets this is the project of the dataset, for example `bigquery-public-data`. |
| `dataset`      | The dataset name.                                                                               |
| `table`        | The table name.                                                                                 |
| `access_token` | An OAuth 2.0 access token (optional positional argument, see [Authentication](#authentication)). |

The following arguments can be specified in the `key = value` form (or as keys of a named collection):

| Key                   | Description                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------|
| `access_token`        | An OAuth 2.0 access token.                                                              |
| `service_account_key` | The content of a Google service account key file in JSON format.                        |
| `client_id`           | OAuth 2.0 client id (used together with `client_secret` and `refresh_token`).           |
| `client_secret`       | OAuth 2.0 client secret.                                                                |
| `refresh_token`       | OAuth 2.0 refresh token.                                                                |
| `billing_project`     | Optional project to attribute quota and billing to (sent as the `X-Goog-User-Project` header). |
| `base_url`            | The API endpoint, `https://bigquery.googleapis.com` by default. Can be changed for tests and emulators. |
| `token_url`           | The OAuth token endpoint override for tests and emulators. By default, the `token_uri` of the service account key or `https://oauth2.googleapis.com/token`. |

## Authentication {#authentication}

Exactly one authentication method must be provided. BigQuery does not allow anonymous access, so credentials are required even for public datasets.

1. **Access token**. Any valid OAuth 2.0 access token, for example, from `gcloud auth print-access-token`. Tokens expire quickly (typically after one hour), so this method is best for interactive use.
2. **Service account key** (recommended for servers). Pass the content of a key file created in Google Cloud IAM with the `service_account_key` argument. ClickHouse signs a JWT with the key and exchanges it for an access token, refreshing it automatically.
3. **Refresh token**. Pass `client_id`, `client_secret` and `refresh_token`, for example, taken from `~/.config/gcloud/application_default_credentials.json` after `gcloud auth application-default login`.

Store credentials in a [named collection](/operations/named-collections) to avoid specifying them in each query.

## Data type mapping {#data-type-mapping}

| BigQuery type            | ClickHouse type                 |
|--------------------------|---------------------------------|
| `STRING`                 | [String](../../sql-reference/data-types/string.md) |
| `BYTES`                  | [String](../../sql-reference/data-types/string.md) (raw bytes) |
| `INTEGER` / `INT64`      | [Int64](../../sql-reference/data-types/int-uint.md) |
| `FLOAT` / `FLOAT64`      | [Float64](../../sql-reference/data-types/float.md) |
| `BOOLEAN` / `BOOL`       | [Bool](../../sql-reference/data-types/boolean.md) |
| `TIMESTAMP`              | [DateTime64(6, 'UTC')](../../sql-reference/data-types/datetime64.md) |
| `DATE`                   | [Date32](../../sql-reference/data-types/date32.md) |
| `TIME`                   | [Time64(6)](../../sql-reference/data-types/time64.md) |
| `DATETIME`               | [DateTime64(6, 'UTC')](../../sql-reference/data-types/datetime64.md) |
| `NUMERIC` / `DECIMAL`    | [Decimal(38, 9)](../../sql-reference/data-types/decimal.md), or `Decimal(P, S)` when parameterized |
| `BIGNUMERIC`             | [Decimal(76, 38)](../../sql-reference/data-types/decimal.md), or `Decimal(P, S)` when parameterized |
| `GEOGRAPHY`              | [String](../../sql-reference/data-types/string.md) (WKT) |
| `JSON`                   | [String](../../sql-reference/data-types/string.md) |
| `INTERVAL`               | [String](../../sql-reference/data-types/string.md) |
| `RANGE`                  | [String](../../sql-reference/data-types/string.md) (read-only) |
| `RECORD` / `STRUCT`      | [Tuple](../../sql-reference/data-types/tuple.md), or [Nullable](../../sql-reference/data-types/nullable.md)(`Tuple`) in `NULLABLE` mode |
| `REPEATED` mode          | [Array](../../sql-reference/data-types/array.md) of the element type, with a [Nullable](../../sql-reference/data-types/nullable.md) element (including `Nullable(Tuple(...))` for a `RECORD` element) |
| `NULLABLE` mode          | [Nullable](../../sql-reference/data-types/nullable.md) |

Notes:

- BigQuery `DATETIME` has no time zone; it is mapped to `DateTime64(6, 'UTC')` so that the displayed value does not depend on the server time zone.
- A `NULLABLE` `RECORD` is mapped to `Nullable(Tuple(...))`, so a whole-record `NULL` is preserved as `NULL` instead of collapsing to a `Tuple` of default values. A `NULL` (or empty) array becomes an empty array, because `Array` cannot be inside `Nullable` in ClickHouse. The elements of a `REPEATED` field use a `Nullable` element type (including `Nullable(Tuple(...))` for `RECORD` elements), so a `NULL` array element is preserved rather than coerced to a default.
- Reading and writing `Nullable(Tuple(...))` columns through the `bigquery` table function works without extra settings. Creating a persistent `BigQuery`-engine table that contains such a column (whether the structure is inferred or declared explicitly) requires the `enable_nullable_tuple_type` setting, as for any `Nullable(Tuple)` column. When declaring columns explicitly, a `RECORD` field may instead be declared as a plain `Tuple(...)` to avoid the setting, at the cost of coercing a whole-record `NULL` to a default tuple; the engine accepts a declared type that differs from the inferred type only by `Nullable` wrappers placed directly around a `Tuple`.
- `BIGNUMERIC` values with more than 38 digits in the integer part do not fit into `Decimal(76, 38)` and produce an error.
- `TIMESTAMP` and `DATE` values outside of the range of `DateTime64`/`Date32` (years 1900-2299) are not supported.
- `RANGE` columns are read-only. `tabledata.insertAll` expects a `RANGE<T>` value as a structured `{start, end}` object, which cannot be reconstructed from the `String` mapping, so inserting into a `RANGE` column raises an error.
- `INT64` values are sent to `tabledata.insertAll` as decimal strings, because the API parses JSON numbers as doubles and would otherwise corrupt values outside `[-2^53 + 1, 2^53 - 1]`.

## Examples {#examples}

Read a public dataset using a token from `gcloud`:

```sql
SELECT word, sum(word_count) AS c
FROM bigquery('bigquery-public-data', 'samples', 'shakespeare', '<access token>')
GROUP BY word
ORDER BY c DESC
LIMIT 5;
```

Read a private table using a service account key file:

```sql
SELECT count()
FROM bigquery('my-project', 'my_dataset', 'my_table',
              service_account_key = '{"type": "service_account", "private_key": "...", "client_email": "...", ...}');
```

Insert data (streaming insert, requires billing to be enabled):

```sql
INSERT INTO FUNCTION bigquery('my-project', 'my_dataset', 'my_table', '<access token>')
SELECT number AS id, toString(number) AS name FROM numbers(10);
```

Use a named collection:

```xml
<clickhouse>
    <named_collections>
        <my_bigquery>
            <project>my-project</project>
            <dataset>my_dataset</dataset>
            <service_account_key><![CDATA[{"type": "service_account", ...}]]></service_account_key>
        </my_bigquery>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM bigquery(my_bigquery, table = 'my_table');
```

## Limitations {#limitations}

- Only native BigQuery tables can be read. Views and external tables require running a BigQuery query job, which this function does not do.
- `RANGE` columns can be read (as `String`) but not written: inserting into a `RANGE` column raises an error.
- Predicates and limits are not pushed down: the whole table (only the selected columns) is downloaded. Use column selection to reduce the transferred data.
- Rows written with streaming inserts land in the BigQuery streaming buffer and may take a while to become visible to subsequent reads.
- Writes are not atomic. A large `INSERT` is sent to `tabledata.insertAll` in batches of 500 rows, and BigQuery commits each request independently. If a later batch is rejected after earlier batches have been accepted, the query reports an error but the already-accepted rows remain committed in BigQuery. To limit duplication, each row is sent with a stable `insertId` derived from the query id and the row's position, which BigQuery uses for best-effort deduplication within its streaming-insert window — so a transport-level retry, or re-running the same `INSERT` with the same `query_id` over the same input, does not re-insert the already-committed rows.

## Related {#related}

- [`BigQuery` table engine](../../engines/table-engines/integrations/bigquery.md)
