---
description: 'Documentation for INTO OUTFILE Clause'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'INTO OUTFILE Clause'
---

# INTO OUTFILE Clause

`INTO OUTFILE` clause redirects the result of a `SELECT` query to a file on the **client** side.

Compressed files are supported. Compression type is detected by the extension of the file name (mode `'auto'` is used by default). Or it can be explicitly specified in a `COMPRESSION` clause. The compression level for a certain compression type can be specified in a `LEVEL` clause.

**Syntax**

```sql
SELECT <expr_list> INTO OUTFILE file_name [APPEND | TRUNCATE] [AND STDOUT] [PARTITION BY <partition_by_expr_list>] [COMPRESSION type [LEVEL level]]
```

`file_name` and `type` are string literals. Supported compression types are: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level` is a numeric literal. Positive integers in following ranges are supported: `1-12` for `lz4` type, `1-22` for `zstd` type and `1-9` for other compression types.

## PARTITION BY {#partition-by}

If specified result of `SELECT` query is partitioned using `partition_by_expr_list`, each partition goes to a separate file. `file_name` used as a template to get actual filename for each partition.  Each placeholder `{a}` in `file_name` will be substituted with `ToString(a)` value of column `a`. Column can be referred by expression name or alias. 


Template rules:
- All expression from `partition_by_expr_list` must be used in the template.
- Symbols `{` and `}` in`file_name` that does not belong to any placeholder must be escaped by backslash `\` (escaping only affect `{` and `}`)
- if there is only one key in `<partition_by_expr_list>`, it also can be referred to in the template using `{_partition_id}` placeholder
- values are substituted as is, so clients may want to escape partition key, that can have bad character for theirs OS filename

When using `PARTITION BY` and open/writing to a file error occurs, there is not rollback. ClickHouse can not check in advance which files will be used, so user may want to use `APPEND` or `TRUNCATE` options, or prefix `file_name` with empty directory.


`PARTITION BY` does not support Extremes and Totals.

## Implementation Details {#implementation-details}

- This functionality is available in the [command-line client](../../../interfaces/cli.md) and [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Thus a query sent via [HTTP interface](../../../interfaces/http.md) will fail.
- The query will fail if a file with the same file name already exists.
- The default [output format](../../../interfaces/formats.md) is `TabSeparated` (like in the command-line client batch mode). Use [FORMAT](format.md) clause to change it.
- If `AND STDOUT` is mentioned in the query then the output that is written to the file is also displayed on standard output. If used with compression, the plaintext is displayed on standard output.
- If `APPEND` is mentioned in the query then the output is appended to an existing file. If compression is used, append cannot be used.
- When writing to a file that already exists, `APPEND` or `TRUNCATE` must be used.
- Compression & format guessing with `PARTITION BY` use template rather than actual file name


**Example**

Execute the following queries using [command-line client](../../../interfaces/cli.md):

```bash
$ clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
$ zcat select.gz 
1,"ABC"
```

```bash
clickhouse-client --query="SELECT * FROM VALUES('a UInt8, b UInt8, c UInt8', (1, 1, 40), (1, 2, 41), (2, 1, 42), (2, 2, 43))
INTO OUTFILE 'name_{a}_{alias}' TRUNCATE PARTITION BY a, b as alias FORMAT CSVWithNames"
$ ls name*
name_1_1  name_1_2  name_2_1  name_2_2
$ cat name*
"a","b","c"
1,1,40
"a","b","c"
1,2,41
"a","b","c"
2,1,42
"a","b","c"
2,2,43
```


```bash
$ clickhouse-client --query="SELECT * FROM VALUES('a UInt8, b UInt8', (0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6))
INTO OUTFILE 'name_{_partition_id}' TRUNCATE PARTITION BY number % 2"
$ ls name*
name_0  name_1
$ cat name_0
0   1
0   2
2   5
2   6
$ cat name_1
1   3
1   4
```
