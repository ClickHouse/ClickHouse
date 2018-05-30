# Roadmap

## Q1 2018

### New fuctionality

- Support for `UPDATE` and `DELETE`.

- Multidimensional and nested arrays.

   It can look something like this:

```sql
CREATE TABLE t
(
    x Array(Array(String)),
    z Nested(
        x Array(String),
        y Nested(...))
)
ENGINE = MergeTree ORDER BY x
```

- External MySQL and ODBC tables.

   External tables can be integrated into ClickHouse using external dictionaries. This new functionality is a convenient alternative to connecting external tables.

```sql
SELECT ...
FROM mysql('host:port', 'db', 'table', 'user', 'password')`
```

### Improvements

- Effective data copying between ClickHouse clusters.

   Now you can copy data with the remote() function. For example: `INSERT INTO t SELECT * FROM remote(...) `.

   This operation will have improved performance.

- O_DIRECT for merges.

   This will improve the performance of the OS cache and "hot" queries.

## Q2 2018

### New functionality

- UPDATE/DELETE conform to the EU GDPR.

- Protobuf and Parquet input and output formats.

- Creating dictionaries using DDL queries.

   Currently, dictionaries that are part of the database schema are defined in external XML files. This is inconvenient and counter-intuitive. The new approach should fix it.

- Integration with LDAP.

- WITH ROLLUP and WITH CUBE for GROUP BY.

- Custom encoding and compression for each column individually.

   As of now, ClickHouse supports LZ4 and ZSTD compression of columns, and compression settings are global (see the article [Compression in ClickHouse](https://www.altinity.com/blog/2017/11/21/compression-in-clickhouse)). Per-column compression and encoding will provide more efficient data storage, which in turn will speed up queries.

- Storing data on multiple disks on the same server.

   This functionality will make it easier to extend the disk space, since different disk systems can be used for different databases or tables. Currently, users are forced to use symbolic links if the databases and tables must be stored on a different disk.

### Improvements

Many improvements and fixes are planned for the query execution system. For example:

- Using an index for `in (subquery)`.

   The index is not used right now, which reduces performance.

- Passing predicates from `where` to subqueries, and passing predicates to views.

   The predicates must be passed, since the view is changed by the subquery. Performance is still low for view filters, and views can't use the primary key of the original table, which makes views useless for large tables.

- Optimizing branching operations (ternary operator, if, multiIf).

   ClickHouse currently performs all branches, even if they aren't necessary.

- Using a primary key for GROUP BY and ORDER BY.

   This will speed up certain types of queries with partially sorted data.

## Q3-Q4 2018

We don't have any set plans yet, but the main projects will be:

- Resource pools for executing queries.

   This will make load management more efficient.

- ANSI SQL JOIN syntax.

   Improve ClickHouse compatibility with many SQL tools.

