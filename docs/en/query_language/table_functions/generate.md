# generate

Generates random data with given schema.
Allows to populate test tables with data.
Supports all data types that can be stored in table except LowCardinality, AggregateFunction.

```sql
generate('name TypeName[, name TypeName]...', 'limit'[, 'max_array_length'[, 'max_string_length'[, 'random_seed']]]);
```

**Parameters**

- `name` — Name of corresponding column.
- `TypeName` — Type of corresponding column.
- `limit` — Number of rows to generate.
- `max_array_length` — Maximum array length for all generated arrays. Defaults to `10`.
- `max_string_length` — Maximum string length for all generated strings. Defaults to `10`.
- `random_seed` — Specify random seed manually to produce stable results. Defaults to `0` — seed is randomly generated.

**Returned Value**

A table object with requested schema.

## Usage Example


```sql
SELECT * FROM generate('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 3, 2, 10, 1);
```
```text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/generate/) <!--hide-->
