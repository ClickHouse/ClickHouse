---
slug: /en/sql-reference/table-functions/generate
sidebar_position: 47
sidebar_label: generateRandom
---

# generateRandom

Generates random data with given schema.
Allows to populate test tables with data.
Not all types are supported.

``` sql
generateRandom('name TypeName[, name TypeName]...', [, 'random_seed'[, 'max_string_length'[, 'max_array_length']]])
```

**Arguments**

-   `name` — Name of corresponding column.
-   `TypeName` — Type of corresponding column.
-   `max_array_length` — Maximum elements for all generated arrays or maps. Defaults to `10`.
-   `max_string_length` — Maximum string length for all generated strings. Defaults to `10`.
-   `random_seed` — Specify random seed manually to produce stable results. If NULL — seed is randomly generated.

**Returned Value**

A table object with requested schema.

## Usage Example

``` sql
SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2) LIMIT 3;
```

``` text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

```sql
CREATE TABLE random (a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)) engine=Memory;
INSERT INTO random SELECT * FROM generateRandom() LIMIT 2;
SELECT * FROM random;
```

```text
┌─a────────────────────────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ []                           │   68091.8197 │ ('2037-10-02 12:44:23.368','039ecab7-81c2-45ee-208c-844e5c6c5652') │
│ [8,-83,0,-22,65,9,-30,28,64] │ -186233.4909 │ ('2062-01-11 00:06:04.124','69563ea1-5ad1-f870-16d8-67061da0df25') │
└──────────────────────────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

## Related content
- Blog: [Generating random data in ClickHouse](https://clickhouse.com/blog/generating-random-test-distribution-data-for-clickhouse)
