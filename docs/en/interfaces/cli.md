# Command-line client

To work from the command line, you can use ` clickhouse-client`:

```bash
$ clickhouse-client
ClickHouse client version 0.0.26176.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.26176.

:)
```

The client supports command-line options and configuration files. For more information, see "[Configuring](#interfaces_cli_configuration)".

## Usage

The client can be used in interactive and non-interactive (batch) mode.
To use batch mode, specify the 'query' parameter, or send data to 'stdin' (it verifies that 'stdin' is not a terminal), or both.
Similar to the HTTP interface, when using the 'query' parameter and sending data to 'stdin', the request is a concatenation of the 'query' parameter, a line feed, and the data in 'stdin'. This is convenient for large INSERT queries.

Example of using the client to insert data:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

By default, you can only process a single query in batch mode. To make multiple queries from a "script," use the --multiquery parameter. This works for all queries except INSERT. Query results are output consecutively without additional separators.
Similarly, to process a large number of queries, you can run 'clickhouse-client' for each query. Note that it may take tens of milliseconds to launch the 'clickhouse-client' program.

In interactive mode, you get a command line where you can enter queries.

If 'multiline' is not specified (the default):To run the query, press Enter. The semicolon is not necessary at the end of the query. To enter a multiline query, enter a backslash `\` before the line feed. After you press Enter, you will be asked to enter the next line of the query.

If multiline is specified:To run a query, end it with a semicolon and press Enter. If the semicolon was omitted at the end of the entered line, you will be asked to enter the next line of the query.

Only a single query is run, so everything after the semicolon is ignored.

You can specify `\G` instead of or after the semicolon. This indicates Vertical format. In this format, each value is printed on a separate line, which is convenient for wide tables. This unusual feature was added for compatibility with the MySQL CLI.

The command line is based on 'readline' (and 'history' or 'libedit', or without a library, depending on the build). In other words, it uses the familiar keyboard shortcuts and keeps a history.
The history is written to `~/.clickhouse-client-history`.

By default, the format used is PrettyCompact. You can change the format in the FORMAT clause of the query, or by specifying `\G` at the end of the query, using the `--format` or `--vertical` argument in the command line, or using the client configuration file.

To exit the client, press Ctrl+D (or Ctrl+C), or enter one of the following instead of a query:"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж", "q", "й", "q", "Q", ":q", "й", "Й", "Жй"

When processing a query, the client shows:

1. Progress, which is updated no more than 10 times per second (by default). For quick queries, the progress might not have time to be displayed.
2. The formatted query after parsing, for debugging.
3. The result in the specified format.
4. The number of lines in the result, the time passed, and the average speed of query processing.

You can cancel a long query by pressing Ctrl+C. However, you will still need to wait a little for the server to abort the request. It is not possible to cancel a query at certain stages. If you don't wait and press Ctrl+C a second time, the client will exit.

The command-line client allows passing external data (external temporary tables) for querying. For more information, see the section "External data for query processing".

<a name="interfaces_cli_configuration"></a>

## Configuring

You can pass parameters to `clickhouse-client` (all parameters have a default value) using:

- From the Command Line

   Command-line options override the default values and settings in configuration files.

- Configuration files.

   Settings in the configuration files override the default values.

### Command line options

- `--host, -h` -– The server name, 'localhost' by default.  You can use either the name or the IPv4 or IPv6 address.
- `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
- `--user, -u` – The username. Default value: default.
- `--password` – The password. Default value: empty string.
- `--query, -q` – The query to process when using non-interactive mode.
- `--database, -d` – Select the current default database. Default value: the current database from the server settings ('default' by default).
- `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
- `--multiquery, -n` – If specified, allow processing multiple queries separated by commas. Only works in non-interactive mode.
- `--format, -f` – Use the specified default format to output the result.
- `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as '--format=Vertical'. In this format, each value is printed on a separate line, which is helpful when displaying wide tables.
- `--time, -t` – If specified, print the query execution time to 'stderr' in non-interactive mode.
- `--stacktrace` – If specified, also print the stack trace if an exception occurs.
- `-config-file` – The name of the configuration file.

### Configuration files

`clickhouse-client`  uses the first existing file of the following:

- Defined in the `-config-file` parameter.
- `./clickhouse-client.xml`
- `\~/.clickhouse-client/config.xml`
- `/etc/clickhouse-client/config.xml`

Example of a config file:

```xml
<config>
    <user>username</user>
    <password>password</password>
</config>
```

