---
toc_title: INTO OUTFILE
---

# INTO OUTFILE Clause {#into-outfile-clause}

`INTO OUTFILE` clause redirects the result of a `SELECT` query to a file on the **client** side. File compression is detected by the extension of the file name or it can be explicitly specified in a `COMPRESSION` clause.

**Syntax**

```sql
SELECT <expr_list> INTO OUTFILE filename [COMPRESSION type]
```

`filename` and `type` are string literals. Supported compression types are: `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

## Implementation Details {#implementation-details}

-   This functionality is available in the [command-line client](../../../interfaces/cli.md) and [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Thus a query sent via [HTTP interface](../../../interfaces/http.md) will fail.
-   The query will fail if a file with the same file name already exists.
-   The default [output format](../../../interfaces/formats.md) is `TabSeparated` (like in the command-line client batch mode). Use [FORMAT](format.md) clause to change it.

**Example**

Execute a query in the [command-line client](../../../interfaces/cli.md):

```bash
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

Result:
```text
1,"ABC"
```
