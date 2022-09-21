---
sidebar_label: INTO OUTFILE
---

# INTO OUTFILE Clause

`INTO OUTFILE` clause redirects the result of a `SELECT` query to a file on the **client** side.

Compressed files are supported. Compression type is detected by the extension of the file name (mode `'auto'` is used by default). Or it can be explicitly specified in a `COMPRESSION` clause.

**Syntax**

```sql
SELECT <expr_list> INTO OUTFILE file_name [COMPRESSION type]
```

`file_name` and `type` are string literals. Supported compression types are: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

## Implementation Details

-   This functionality is available in the [command-line client](../../../interfaces/cli.md) and [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Thus a query sent via [HTTP interface](../../../interfaces/http.md) will fail.
-   The query will fail if a file with the same file name already exists.
-   The default [output format](../../../interfaces/formats.md) is `TabSeparated` (like in the command-line client batch mode). Use [FORMAT](format.md) clause to change it.

**Example**

Execute the following query using [command-line client](../../../interfaces/cli.md):

```bash
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

Result:

```text
1,"ABC"
```
