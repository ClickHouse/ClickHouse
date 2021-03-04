---
toc_priority: 53
toc_title: null function
---

# null {#null-function}

Accepts an inserted data of the specified structure and immediately drops it away. The function is used for convenience writing tests and demonstrations.

**Syntax** 

``` sql
null('structure')
```

**Parameter** 

-   `structure` — A list of columns and column types. [String](../../sql-reference/data-types/string.md).

**Returned value**

A table with the specified structure, which is dropped right after the query execution.

**Example**

Query with the `null` function:

``` sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```
can replace three queries:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

See also: format **Null**.

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/null/) <!--hide-->
