#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/BigQuery/StorageBigQuery.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class TableFunctionBigQuery : public ITableFunction
{
public:
    static constexpr auto name = "bigquery";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "BigQuery"; }

    String getUsedNamedCollectionName() const override { return configuration ? configuration->named_collection_name : String{}; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<BigQueryConfiguration> configuration;
    /// Shared with the resulting storage so that an OAuth access token minted during schema inference
    /// (analysis) is reused during execution instead of being re-requested from the token endpoint.
    std::shared_ptr<BigQueryTokenProvider> token_provider;
    /// The remote schema is fetched over the network once during analysis. It is cached here and handed
    /// to the storage so that execution reuses the same schema snapshot (no second `tables.get`, and no
    /// mismatch if the BigQuery table changes between analysis and execution).
    mutable std::optional<BigQueryFields> fetched_fields;
};

StoragePtr TableFunctionBigQuery::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    std::optional<BigQueryFields> prefetched_fields;
    ColumnsDescription columns;
    if (cached_columns.empty())
    {
        columns = getActualTableStructure(context, is_insert_query);
        prefetched_fields = fetched_fields;
    }
    else
    {
        columns = std::move(cached_columns);
        /// A cache hit bypasses schema inference, so `fetched_fields` may be empty here; in that case the
        /// storage re-establishes the snapshot from the live schema on the first read or write, validating
        /// the cached columns against it — the same reload contract as a `BigQuery` engine table after
        /// `ATTACH` or a server restart (see the limitations in the documentation below).
        prefetched_fields = fetched_fields;
    }

    auto storage = std::make_shared<StorageBigQuery>(
        StorageID(getDatabaseName(), table_name), *configuration, columns, ConstraintsDescription(), String{}, context,
        token_provider, std::move(prefetched_fields));
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionBigQuery::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (!fetched_fields)
        fetched_fields = StorageBigQuery::fetchTableSchema(*configuration, context, token_provider);
    return columnsDescriptionFromBigQuerySchema(*fetched_fields);
}

void TableFunctionBigQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'bigquery' must have arguments");

    configuration = std::make_shared<BigQueryConfiguration>(BigQueryConfiguration::fromArguments(func_args.arguments->children, context));
    /// The token provider only stores the configuration here; no network request is made until a token
    /// is actually needed (during schema inference or execution).
    token_provider = std::make_shared<BigQueryTokenProvider>(*configuration);
}

}

void registerTableFunctionBigQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionBigQuery>(
    {
        .description = R"DOCS_MD(
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

The `project`, `dataset`, `table` and `access_token` arguments can also be given in the `key = value` form; positional arguments fill these slots in this order, and specifying an argument both positionally and as a key (or the same key twice) is an error.

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

Store credentials in a [named collection](/operations/named-collections) to avoid specifying them in each query. A permanent table created from a named collection (with the `BigQuery` table engine or `CREATE TABLE ... AS bigquery(...)`) is registered as a dependency of the collection, so `DROP NAMED COLLECTION` is blocked while the table exists.

## Data type mapping {#data-type-mapping}

| BigQuery type            | ClickHouse type                 |
|--------------------------|---------------------------------|
| `STRING`                 | [String](/sql-reference/data-types/string) |
| `BYTES`                  | [String](/sql-reference/data-types/string) (raw bytes) |
| `INTEGER` / `INT64`      | [Int64](/sql-reference/data-types/int-uint) |
| `FLOAT` / `FLOAT64`      | [Float64](/sql-reference/data-types/float) |
| `BOOLEAN` / `BOOL`       | [Bool](/sql-reference/data-types/boolean) |
| `TIMESTAMP`              | [DateTime64(6, 'UTC')](/sql-reference/data-types/datetime64) |
| `DATE`                   | [Date32](/sql-reference/data-types/date32) |
| `TIME`                   | [Time64(6)](/sql-reference/data-types/time64) |
| `DATETIME`               | [DateTime64(6, 'UTC')](/sql-reference/data-types/datetime64) |
| `NUMERIC` / `DECIMAL`    | [Decimal(38, 9)](/sql-reference/data-types/decimal), or `Decimal(P, S)` when parameterized |
| `BIGNUMERIC`             | [Decimal(76, 38)](/sql-reference/data-types/decimal), or `Decimal(P, S)` when parameterized |
| `GEOGRAPHY`              | [Geometry](/sql-reference/data-types/geo#geometry) (parsed from WKT) |
| `JSON`                   | [String](/sql-reference/data-types/string) |
| `INTERVAL`               | [String](/sql-reference/data-types/string) |
| `RANGE`                  | [String](/sql-reference/data-types/string) (read-only) |
| `RECORD` / `STRUCT`      | [Tuple](/sql-reference/data-types/tuple), or [Nullable](/sql-reference/data-types/nullable)(`Tuple`) in `NULLABLE` mode |
| `REPEATED` mode          | [Array](/sql-reference/data-types/array) of the element type, with a non-`Nullable` element (`Array(Tuple(...))` for a `RECORD` element), because a BigQuery array cannot contain `NULL` elements |
| `NULLABLE` mode          | [Nullable](/sql-reference/data-types/nullable) (except `GEOGRAPHY`, whose `Geometry` type holds a `NULL` by itself) |

Notes:

- BigQuery `DATETIME` has no time zone; it is mapped to `DateTime64(6, 'UTC')` so that the displayed value does not depend on the server time zone.
- A `NULLABLE` `RECORD` is mapped to `Nullable(Tuple(...))`, so a whole-record `NULL` is preserved as `NULL` instead of collapsing to a `Tuple` of default values. A `NULL` (or empty) array becomes an empty array, because `Array` cannot be inside `Nullable` in ClickHouse. A BigQuery array cannot contain `NULL` elements (`ARRAY<T>` is equivalent to `ARRAY<T NOT NULL>`), so the element type of a `REPEATED` field is not `Nullable` (`Array(T)`, or `Array(Tuple(...))` for a `RECORD` element); a `NULL` element in a `tabledata.list` response is rejected as malformed input.
- Reading and writing `Nullable(Tuple(...))` columns through the `bigquery` table function works without extra settings. Creating a persistent `BigQuery`-engine table that contains such a column (whether the structure is inferred or declared explicitly) requires the `enable_nullable_tuple_type` setting, as for any `Nullable(Tuple)` column. When declaring columns explicitly, a `RECORD` field may instead be declared as a plain `Tuple(...)` to avoid the setting, at the cost of coercing a whole-record `NULL` to a default tuple; the only accepted difference from the inferred type is dropping a `Nullable` that wraps a `RECORD`'s `Tuple`, and only at that same record — the nullability cannot be moved to a different (inner or outer) record.
- `GEOGRAPHY` is mapped to [Geometry](/sql-reference/data-types/geo#geometry). BigQuery transfers a `GEOGRAPHY` value as [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) text, which is parsed into the matching alternative of `Geometry` (a `Variant` of `Point`, `MultiPoint`, `Ring`, `LineString`, `MultiLineString`, `Polygon` and `MultiPolygon`) on read, and serialized back to WKT on write. A `GEOMETRYCOLLECTION` and an empty geometry (such as `POINT EMPTY`) have no `Geometry` counterpart, so reading a row that contains such a value raises an error. Because `Variant` holds a `NULL` by itself, a `NULLABLE` `GEOGRAPHY` field is mapped to `Geometry` and not to `Nullable(Geometry)`, and `NULL` still round-trips.
- `JSON` is mapped to `String` rather than to the [JSON](/sql-reference/data-types/newjson) data type, because the ClickHouse `JSON` type accepts only an object (`{...}`) at the top level, while a BigQuery `JSON` value can be any JSON value — a scalar, an array, or `null` — so a table containing such values could not be read. In addition, `JSON` cannot be wrapped in `Nullable`, so an SQL `NULL` in a `NULLABLE` column would not be preserved. The `String` mapping is lossless; top-level objects can be converted with `CAST(value AS JSON)`.
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
- A `GEOGRAPHY` value that is a `GEOMETRYCOLLECTION` or an empty geometry cannot be represented by the `Geometry` type, so reading a row containing one raises an error. Writing a `NULL` `Geometry` into a `REQUIRED` `GEOGRAPHY` field, or as an element of a `REPEATED` `GEOGRAPHY` field, is rejected, because BigQuery accepts no `NULL` there.
- Predicates are not pushed down: `tabledata.list` only lists the rows of a table and has no filtering parameter at all (it takes pagination, column selection and format options), and filtering would require running a BigQuery query job, which this function does not do. A `WHERE` condition is therefore applied in ClickHouse after the rows have been downloaded; use column selection to reduce the transferred data.
- A `LIMIT`, on the other hand, does reduce the amount of data read. Pages are requested lazily, with `maxResults` set to `max_block_size`, and no further page is requested once the query has enough rows. For a trivial `LIMIT n` (no `WHERE`, `GROUP BY`, `ORDER BY`, and `n` below `max_block_size`) ClickHouse lowers `max_block_size` to `n`, so exactly one request for exactly `n` rows is made; otherwise the read stops at the first page boundary past the limit, overshooting it by less than one page.
- The read is pinned to the schema seen at query analysis time by passing the explicit list of columns to `tabledata.list`. For a very wide read whose column list would exceed the request URL length limit (for example `SELECT *` from a table with thousands of columns), the query is rejected rather than read without a pin (an unpinned read could be misaligned by a concurrent schema change); select fewer columns so the list fits. The same URL length limit is checked before every paginated request (each page carries an opaque `pageToken`), so a read whose later pages would not fit the limit is rejected with the same error instead of failing part way through.
- If the BigQuery table is altered after its schema has been read, the query is rejected instead of silently returning or writing mismatched data: the live schema is re-fetched and compared with the analyzed one right before a read, and again before an `INSERT` streams its first row. The remaining window (a schema change between that check and the requests that follow it) cannot be closed, because the schema and the data are fetched by separate REST requests.
- The comparison is against the schema snapshot the query was analyzed with, which is taken when the table function resolves its structure or, for a persistent table (a `BigQuery` engine table, or a table created with `CREATE TABLE ... AS bigquery(...)`, which persists its columns the same way), on its first read or write after `CREATE`, `ATTACH`, or a server restart. Table metadata persists the mapped ClickHouse columns, not the BigQuery schema, so a schema change made while the table was detached (or the server was down) is adopted by the next query rather than rejected: the declared columns are still validated against the live schema, and the rows are decoded with it, so a change that keeps the mapped ClickHouse types (`STRING` to `BYTES`, for example) is read with the new type's rules under the same column type.
- Rows written with streaming inserts land in the BigQuery streaming buffer and may take a while to become visible to subsequent reads.
- A large `INSERT` is sent to `tabledata.insertAll` in batches: at most 500 rows per request, and also split so that each request stays under BigQuery's 10 MB request-size limit (a single row larger than that limit is rejected with a clear error).
- Writes are not atomic, and a single `tabledata.insertAll` request may itself partially succeed: BigQuery can commit some rows of a request while rejecting the others with `insertErrors`. Requests are also committed independently of each other, so a later batch may be rejected after earlier batches have been accepted. In both cases the query reports an error, but the already-committed rows remain in BigQuery. To limit duplication, each row is sent with a stable `insertId` derived from the query id and the row's ordinal position in the stream, which BigQuery uses for best-effort deduplication within its streaming-insert window. A `query_id` longer than BigQuery's 128-character `insertId` limit is hashed to a fixed-length prefix, which stays stable for that `query_id`. Because the `insertId` depends on the ordinal position, deduplication is reliable only when the rerun produces the rows in the same order: a transport-level retry of a batch is always safe, and re-running the same `INSERT` with the same `query_id` deduplicates only if it presents the rows in the same order (for example a single-threaded insert, or an otherwise deterministic ordering — set `max_threads = 1` and `max_insert_threads = 1` for a parallel `INSERT ... SELECT` whose chunk order could otherwise change between attempts).

## Related {#related}

- [`BigQuery` table engine](/engines/table-engines/integrations/bigquery)
)DOCS_MD",
        .category = FunctionDocumentation::Category::TableFunction
    });
}

}
