---
sidebar_position: 17
sidebar_label: Command-Line Client
---

# Command-line Client {#command-line-client}

ClickHouse provides a native command-line client: `clickhouse-client`. The client supports command-line options and configuration files. For more information, see [Configuring](#interfaces_cli_configuration).

[Install](../getting-started/install.md) it from the `clickhouse-client` package and run it with the command `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 20.13.1.5273 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 20.13.1 revision 54442.

:)
```

Different client and server versions are compatible with one another, but some features may not be available in older clients. We recommend using the same version of the client as the server app. When you try to use a client of the older version, then the server, `clickhouse-client` displays the message:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## Usage {#cli_usage}

The client can be used in interactive and non-interactive (batch) mode. To use batch mode, specify the ‘query’ parameter, or send data to ‘stdin’ (it verifies that ‘stdin’ is not a terminal), or both. Similar to the HTTP interface, when using the ‘query’ parameter and sending data to ‘stdin’, the request is a concatenation of the ‘query’ parameter, a line feed, and the data in ‘stdin’. This is convenient for large INSERT queries.

Example of using the client to insert data:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

In batch mode, the default data format is TabSeparated. You can set the format in the FORMAT clause of the query.

By default, you can only process a single query in batch mode. To make multiple queries from a “script,” use the `--multiquery` parameter. This works for all queries except INSERT. Query results are output consecutively without additional separators. Similarly, to process a large number of queries, you can run ‘clickhouse-client’ for each query. Note that it may take tens of milliseconds to launch the ‘clickhouse-client’ program.

In interactive mode, you get a command line where you can enter queries.

If ‘multiline’ is not specified (the default): To run the query, press Enter. The semicolon is not necessary at the end of the query. To enter a multiline query, enter a backslash `\` before the line feed. After you press Enter, you will be asked to enter the next line of the query.

If multiline is specified: To run a query, end it with a semicolon and press Enter. If the semicolon was omitted at the end of the entered line, you will be asked to enter the next line of the query.

Only a single query is run, so everything after the semicolon is ignored.

You can specify `\G` instead of or after the semicolon. This indicates Vertical format. In this format, each value is printed on a separate line, which is convenient for wide tables. This unusual feature was added for compatibility with the MySQL CLI.

The command line is based on ‘replxx’ (similar to ‘readline’). In other words, it uses the familiar keyboard shortcuts and keeps a history. The history is written to `~/.clickhouse-client-history`.

By default, the format used is PrettyCompact. You can change the format in the FORMAT clause of the query, or by specifying `\G` at the end of the query, using the `--format` or `--vertical` argument in the command line, or using the client configuration file.

To exit the client, press Ctrl+D, or enter one of the following instead of a query: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

When processing a query, the client shows:

1.  Progress, which is updated no more than 10 times per second (by default). For quick queries, the progress might not have time to be displayed.
2.  The formatted query after parsing, for debugging.
3.  The result in the specified format.
4.  The number of lines in the result, the time passed, and the average speed of query processing.

You can cancel a long query by pressing Ctrl+C. However, you will still need to wait for a little for the server to abort the request. It is not possible to cancel a query at certain stages. If you do not wait and press Ctrl+C a second time, the client will exit.

The command-line client allows passing external data (external temporary tables) for querying. For more information, see the section “External data for query processing”.

### Queries with Parameters {#cli-queries-with-parameters}

You can create a query with parameters and pass values to them from client application. This allows to avoid formatting query with specific dynamic values on client side. For example:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### Query Syntax {#cli-queries-with-parameters-syntax}

Format a query as usual, then place the values that you want to pass from the app parameters to the query in braces in the following format:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [Data type](../sql-reference/data-types/index.md) of the app parameter value. For example, a data structure like `(integer, ('string', integer))` can have the `Tuple(UInt8, Tuple(String, UInt8))` data type (you can also use another [integer](../sql-reference/data-types/int-uint.md) types). It's also possible to pass table, database, column names as a parameter, in that case you would need to use `Identifier` as a data type.

#### Example {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --query "SELECT {col:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

## Configuring {#interfaces_cli_configuration}

You can pass parameters to `clickhouse-client` (all parameters have a default value) using:

-   From the Command Line

    Command-line options override the default values and settings in configuration files.

-   Configuration files.

    Settings in the configuration files override the default values.

### Command Line Options {#command-line-options}

-   `--host, -h` – The server name, ‘localhost’ by default. You can use either the name or the IPv4 or IPv6 address.
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode. You must specify either `query` or `queries-file` option.
-   `--queries-file, -qf` – file path with queries to execute. You must specify either `query` or `queries-file` option.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ by default).
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the [Vertical format](../interfaces/formats.md#vertical) by default to output the result. This is the same as `–format=Vertical`. In this format, each value is printed on a separate line, which is helpful when displaying wide tables.
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ in non-interactive mode.
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection (TLS). You might need to configure your CA certificates in the [configuration file](#configuration_files). The available configuration settings are the same as for [server-side TLS configuration](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl).
-   `--history_file` — Path to a file containing command history.
-   `--param_<name>` — Value for a [query with parameters](#cli-queries-with-parameters).
-   `--hardware-utilization` — Print hardware utilization information in progress bar.
-   `--print-profile-events` – Print `ProfileEvents` packets.
-   `--profile-events-delay-ms` – Delay between printing `ProfileEvents` packets (-1 - print only totals, 0 - print every single packet).

Since version 20.5, `clickhouse-client` has automatic syntax highlighting (always enabled).

### Configuration Files {#configuration_files}

`clickhouse-client` uses the first existing file of the following:

-   Defined in the `--config-file` parameter.
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

Example of a config file:

```xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>true</secure>
    <openSSL>
      <client>
        <caConfig>/etc/ssl/cert.pem</caConfig>
      </client>
    </openSSL>
</config>
```

### Query ID Format {#query-id-format}

In interactive mode `clickhouse-client` shows query ID for every query. By default, the ID is formatted like this:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

A custom format may be specified in a configuration file inside a `query_id_formats` tag. `{query_id}` placeholder in the format string is replaced with the ID of a query. Several format strings are allowed inside the tag.
This feature can be used to generate URLs to facilitate profiling of queries.

**Example**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

If the configuration above is applied, the ID of a query is shown in the following format:

``` text
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

