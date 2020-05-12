### INTO OUTFILE Clause {#into-outfile-clause}

Add the `INTO OUTFILE filename` clause (where filename is a string literal) to redirect query output to the specified file.
In contrast to MySQL, the file is created on the client side. The query will fail if a file with the same filename already exists.
This functionality is available in the command-line client and clickhouse-local (a query sent via HTTP interface will fail).

The default output format is TabSeparated (the same as in the command-line client batch mode).
