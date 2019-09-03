
# hdfs

Creates a table from a file in hdfs.

```
hdfs(URI, format, structure)
```

**Input parameters**

- `URI` — The relative URI to the file in HDFS.
- `format` —  The [format](../../interfaces/formats.md#formats) of the file.
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Example**

Table from `hdfs://hdfs1:9000/test` and selection of the first two rows from it:

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/hdfs/) <!--hide-->
