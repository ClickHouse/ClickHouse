# Roadmap

## Q1 2018

### New functionality
- Initial support for `UPDATE` and `DELETE`.
- Multi-dimensional and nested arrays.
    
    It may look like this:
    
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

    External tables can be integrated to ClickHouse using external dictionaries. This will be an alternative and a more convenient way to do so.

```sql
SELECT ... 
FROM mysql('host:port', 'db', 'table', 'user', 'password')`
```

### Improvements

- Efficient data copy between ClickHouse clusters.

    Currently, it is possible to copy data using remote() function, e.g.: `
INSERT INTO t SELECT * FROM remote(...) `.

    The performance of this will be improved by proper distributed execution.

- O_DIRECT for merges.

    Should improve OS cache performance and correspondingly query performance for 'hot' queries.


## Q2 2018

### New functionality

- UPDATE/DELETE in order to comply with Europe GDPR.
- Protobuf and Parquet input/output formats.
- Create dictionaries by DDL queries.

    Currently, it is inconvenient and confusing that dictionaries are defined in external XML files while being a part of DB schema. The new approach will fix that.

- LDAP integration.
- WITH ROLLUP and WITH CUBE for GROUP BY.
- Custom encoding/compression for columns.

    Currently, ClickHouse support LZ4 and ZSTD compressions for columns, and compressions settings are global (see our article [Compression in ClickHouse](https://www.altinity.com/blog/2017/11/21/compression-in-clickhouse)) for more details). Column level encoding (e.g. delta encoding) and compression will allow more efficient data storage and therefore faster queries.

- Store data at multiple disk volumes of a single server.

    That will make it easier to extend disk system as well as use different disk systems for different DBs or tables. Currently, users have to use symlinks if DB/table needs to be stored in another volume.

### Improvements

A lot of enhancements and fixes are planned for query execution. In particular:

- Using index for ‘in (subquery)’.

    Currently, index is not used for such queries resulting in lower performance.

- Predicate pushdown from ‘where’ into subqueries and Predicate pushdown for views.

    These two are related since view is replaced by subquery. Currently, performance of filter conditions for views is significantly degraded, views can not use primary key of the underlying table, that makes views on big tables pretty much useless.

- Short-circuit expressions evaluation (ternary operator, if, multiIf).

    Currently, ClickHouse evaluates all branches even if the first one needs to be returned due to logical condition result.

- Using primary key for GROUP BY and ORDER BY.

    This may speed up certain types of queries since data is already partially pre-sorted.

## Q3-Q4 2018

Longer term plans are not yet finalized. There are two major projects on the list so far.

- Resource pools for query execution.

    That will allow managing workloads more efficiently.

- ANSI SQL JOIN syntax.

    That will make ClickHouse more friendly for numerous SQL tools.
