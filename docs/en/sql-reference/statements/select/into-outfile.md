---
toc_title: INTO OUTFILE
---

# INTO OUTFILE Clause {#into-outfile-clause}

Add the `INTO OUTFILE filename` clause (where filename is a string literal) to `SELECT query` to redirect its output to the specified file on the client-side.

## Implementation Details {#implementation-details}

-   This functionality is available in the [command-line client](../../../interfaces/cli.md) and [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Thus a query sent via [HTTP interface](../../../interfaces/http.md) will fail.
-   The query will fail if a file with the same filename already exists.
-   The default [output format](../../../interfaces/formats.md) is `TabSeparated` (like in the command-line client batch mode).
