# clickhouse-odbc-bridge

Simple HTTP-server which works like a proxy for ODBC driver. The main motivation
was possible segfaults or another faults in ODBC implementations, which can
crash whole clickhouse-server process.

This tool works via HTTP, not via pipes, shared memory, or TCP because:
- It's simplier to implement
- It's simplier to debug
- jdbc-bridge can be implemented in the same way

## Usage

`clickhouse-server` use this tool inside odbc table function and StorageODBC.
However it can be used as standalone tool from command line with the following
parameters in POST-request URL:
- `connection_string` -- ODBC connection string.
- `columns` -- columns in ClickHouse NamesAndTypesList format, name in backticks,
  type as string. Name and type are space separated, rows separated with
  newline.
- `max_block_size` -- optional parameter, sets maximum size of single block.
Query is send in post body. Response is returned in RowBinary format.

## Example:

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "columns=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```
