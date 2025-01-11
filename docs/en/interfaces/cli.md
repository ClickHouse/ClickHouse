---
slug: /en/interfaces/cli
sidebar_position: 17
sidebar_label: Command-Line Client
title: Command-Line Client
---
import ConnectionDetails from '@site/docs/en/_snippets/_gather_your_details_native.md';

## clickhouse-client

ClickHouse provides a native command-line client: `clickhouse-client`. The client supports [command-line options](#command-line-options) and [configuration files](#configuration_files).

[Install](../getting-started/install.md) it from the `clickhouse-client` package and run it with the command `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 20.13.1.5273 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 20.13.1.

:)
```

Different client and server versions are compatible with one another, but some features may not be available in older clients. We recommend using the same version of the client as the server app. When you try to use a client of the older version, then the server, `clickhouse-client` displays the message:

```response
ClickHouse client version is older than ClickHouse server.
It may lack support for new features.
```

## Usage {#cli_usage}

The client can be used in interactive and non-interactive (batch) mode.

### Gather your connection details
<ConnectionDetails />

### Interactive

To connect to your ClickHouse Cloud service or any ClickHouse server using TLS and passwords, specify port `9440` (or `--secure`) and provide your username and password:

```bash
clickhouse-client --host <HOSTNAME> \
                  --port 9440 \
                  --user <USERNAME> \
                  --password <PASSWORD>
```

To connect to a self-managed ClickHouse server you will need the details for that server.  Whether or not TLS is used, port numbers, and passwords are all configurable.  Use the above example for ClickHouse Cloud as a starting point.


### Batch

To use batch mode, specify the ‘query’ parameter, or send data to ‘stdin’ (it verifies that ‘stdin’ is not a terminal), or both. Similar to the HTTP interface, when using the ‘query’ parameter and sending data to ‘stdin’, the request is a concatenation of the ‘query’ parameter, a line feed, and the data in ‘stdin’. This is convenient for large INSERT queries.

Examples of using the client to insert data:

#### Inserting a CSV file into a remote ClickHouse service

This example is appropriate for ClickHouse Cloud, or any ClickHouse server using TLS and a password. In this example a sample dataset CSV file, `cell_towers.csv` is inserted into an existing table `cell_towers` in the `default` database:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

:::note
To concentrate on the query syntax, the rest of the examples leave off the connection details (`--host`, `--port`, etc.).  Add them in when you try the commands.
:::

#### Three different ways of inserting data

``` bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

### Notes

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
4.  The number of lines in the result, the time passed, and the average speed of query processing. All data amounts refer to uncompressed data.

You can cancel a long query by pressing Ctrl+C. However, you will still need to wait for a little for the server to abort the request. It is not possible to cancel a query at certain stages. If you do not wait and press Ctrl+C a second time, the client will exit.

The command-line client allows passing external data (external temporary tables) for querying. For more information, see the section “External data for query processing”.

### Queries with Parameters {#cli-queries-with-parameters}

You can create a query with parameters and pass values to them from client application. This allows to avoid formatting query with specific dynamic values on client side. For example:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

It is also possible to set parameters from within an interactive session:
``` bash
$ clickhouse-client -nq "
  SET param_parName='[1, 2]';
  SELECT {parName:Array(UInt16)}"
```

#### Query Syntax {#cli-queries-with-parameters-syntax}

Format a query as usual, then place the values that you want to pass from the app parameters to the query in braces in the following format:

``` sql
{<name>:<data type>}
```

- `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
- `data type` — [Data type](../sql-reference/data-types/index.md) of the app parameter value. For example, a data structure like `(integer, ('string', integer))` can have the `Tuple(UInt8, Tuple(String, UInt8))` data type (you can also use another [integer](../sql-reference/data-types/int-uint.md) types). It's also possible to pass table, database, column names as a parameter, in that case you would need to use `Identifier` as a data type.

#### Example {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```


## Aliases {#cli_aliases}

- `\l` - SHOW DATABASES
- `\d` - SHOW TABLES
- `\c <DATABASE>` - USE DATABASE
- `.` - repeat the last query


## Shortkeys {#shortkeys_aliases}

- `Alt (Option) + Shift + e` - open editor with current query. It is possible to set up an environment variable - `EDITOR`, by default vim is used.
- `Alt (Option) + #` - comment line.
- `Ctrl + r` - fuzzy history search.

:::tip
To configure the correct work of meta key (Option) on MacOS:

iTerm2: Go to Preferences -> Profile -> Keys -> Left Option key and click Esc+
:::

The full list with all available shortkeys - [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).


## Connection string {#connection_string}

clickhouse-client alternatively supports connecting to clickhouse server using a connection string similar to [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). It has the following syntax:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

Where

- `user` - (optional) is a user name,
- `password` - (optional) is a user password. If `:` is specified and the password is blank, the client will prompt for the user's password.
- `hosts_and_ports` - (optional) is a list of hosts and optional ports `host[:port] [, host:[port]], ...`,
- `database` - (optional) is the database name,
- `query_parameters` - (optional) is a list of key-value pairs `param1=value1[,&param2=value2], ...`. For some parameters, no value is required. Parameter names and values are case-sensitive.

If no user is specified, `default` user without password will be used.
If no host is specified, the `localhost` will be used (localhost).
If no port is specified is not specified, `9000` will be used as port.
If no database is specified, the `default` database will be used.

If the user name, password or database was specified in the connection string, it cannot be specified using `--user`, `--password` or `--database` (and vice versa).

The host component can either be a host name and IP address. Put an IPv6 address in square brackets to specify it:

```text
clickhouse://[2001:db8::1234]
```

URI allows multiple hosts to be connected to. Connection strings can contain multiple hosts. ClickHouse-client will try to connect to these hosts in order (i.e. from left to right). After the connection is established, no attempt to connect to the remaining hosts is made.

The connection string must be specified as the first argument of clickhouse-client. The connection string can be combined with arbitrary other [command-line-options](#command-line-options) except `--host/-h` and `--port`.

The following keys are allowed for component `query_parameter`:

- `secure` or shorthanded `s` - no value. If specified, client will connect to the server over a secure connection (TLS). See `secure` in [command-line-options](#command-line-options)

### Percent encoding {#connection_string_uri_percent_encoding}

Non-US ASCII, spaces and special characters in the `user`, `password`, `hosts`, `database` and `query parameters` must be [percent-encoded](https://en.wikipedia.org/wiki/URL_encoding).

### Examples {#connection_string_examples}

Connect to localhost using port 9000 and execute the query `SELECT 1`.

``` bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Connect to localhost using user `john` with password `secret`, host `127.0.0.1` and port `9000`

``` bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Connect to localhost using default user, host with IPV6 address `[::1]` and port `9000`.

``` bash
clickhouse-client clickhouse://[::1]:9000
```

Connect to localhost using port 9000 in multiline mode.

``` bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Connect to localhost using port 9000 with the user `default`.

``` bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Connect to localhost using port 9000 to `my_database` database.

``` bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Connect to localhost using port 9000 to `my_database` database specified in the connection string and a secure connection using shorthanded 's' URI parameter.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Connect to default host using default port, default user, and default database.

``` bash
clickhouse-client clickhouse:
```

Connect to the default host using the default port, using user `my_user` and no password.

``` bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Connect to localhost using email as the user name. `@` symbol is percent encoded to `%40`.

``` bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Connect to one of provides hosts: `192.168.1.15`, `192.168.1.25`.

``` bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

## Configuration Files {#configuration_files}

`clickhouse-client` uses the first existing file of the following:

- Defined in the `--config-file` parameter.
- `./clickhouse-client.xml`, `.yaml`, `.yml`
- `~/.clickhouse-client/config.xml`, `.yaml`, `.yml`
- `/etc/clickhouse-client/config.xml`, `.yaml`, `.yml`

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

Or the same config in a YAML format:

```yaml
user: username
password: 'password'
secure: true
openSSL:
  client:
    caConfig: '/etc/ssl/cert.pem'
```

### Connection credentials {#connection-credentials}

If you frequently connect to the same ClickHouse server, you can save the connection details including credentials in
the configuration file like this:

```xml
<config>
    <connections_credentials>
        <name>production</name>
        <hostname>127.0.0.1</hostname>
        <port>9000</port>
        <secure>1</secure>
        <user>default</user>
        <password></password>
        <database></database>

        <!-- You can use colors and macros, see clickhouse-client.xml for more documentation. -->
        <prompt>\e[31m[PRODUCTION]\e[0m {user}@{display_name}</prompt>
    </connections_credentials>
</config>
```

## Query ID Format {#query-id-format}

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

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

## Command-Line Options {#command-line-options}

All command-line options can be specified directly on the command line or as defaults in the [configuration file](#configuration_files).

### General Options {#command-line-options-general}

**`-c [ -C, --config, --config-file ] <path-to-file>`**

The location of the configuration file for the client, if it is not at one of the default locations. See [Configuration Files](#configuration_files).

**`--help`**

Print usage summary and exit. Combine with `--verbose` to display all possible options including query settings.

**`--history_file <path-to-file>`**

 Path to a file containing the command history.

**`--history_max_entries`**

Maximum number of entries in the history file.

Default value: 1000000 (1 million)

**`--prompt <prompt>`**

Specify a custom prompt.

Default value: The `display_name` of the server.

**`--verbose`**

Increase output verbosity.

**`-V [ --version ]`**

Print version and exit.

### Connection Options {#command-line-options-connection}

**`--connection <name>`**

The name of preconfigured connection details from the configuration file. See [Connection credentials](#connection-credentials).

**`-d [ --database ] <database>`**

Select the database to default to for this connection.

Default value: the current database from the server settings (`default` by default).

**`-h [ --host ] <host>`**

The hostname of the ClickHouse server to connect to. Can either be a hostname or an IPv4 or IPv6 address. Multiple hosts can be passed via multiple arguments.

Default value: localhost

**`--jwt <value>`**

Use JSON Web Token (JWT) for authentication.

Server JWT authorization is only available in ClickHouse Cloud.

**`--no-warnings`**

Disable showing warnings from `system.warnings` when the client connects to the server.

**`--password <password>`**

The password of the database user. You can also specify the password for a connection in the configuration file. If you do not specify the password, the client will ask for it.

**`--port <port>`**

The port the server is accepting connections on. The default ports are 9440 (TLS) and 9000 (no TLS).

Note: The client uses the native protocol and not HTTP(S).

Default value: 9440 if `--secure` is specified, 9000 otherwise. Always defaults to 9440 if the hostname ends in `.clickhouse.cloud`.

**`-s [ --secure ]`**

Whether to use TLS.

Enabled automatically when connecting to port 9440 (the default secure port) or ClickHouse Cloud.

You might need to configure your CA certificates in the [configuration file](#configuration_files). The available configuration settings are the same as for [server-side TLS configuration](../operations/server-configuration-parameters/settings.md#openssl).

**`--ssh-key-file <path-to-file>`**

File containing the SSH private key for authenticate with the server.

**`--ssh-key-passphrase <value>`**

Passphrase for the SSH private key specified in `--ssh-key-file`.

**`-u [ --user ] <username>`**

The database user to connect as.

Default value: default

Instead of the `--host`, `--port`, `--user` and `--password` options, the client also supports [connection strings](#connection_string).

### Query Options {#command-line-options-query}

**`--param_<name>=<value>`**

Substitution value for a parameter of a [query with parameters](#cli-queries-with-parameters).

**`-q [ --query ] <query>`**

The query to run in batch mode. Can be specified multiple times (`--query "SELECT 1" --query "SELECT 2"`) or once with multiple comma-separated queries (`--query "SELECT 1; SELECT 2;"`). In the latter case, `INSERT` queries with formats other than `VALUES` must be separated by empty lines.

A single query can also be specified without a parameter:
```bash
$ clickhouse-client "SELECT 1"
1
```

Cannot be used together with `--queries-file`.

**`--queries-file <path-to-file>`**

Path to a file containing queries. `--queries-file` can be specified multiple times, e.g. `--queries-file  queries1.sql --queries-file  queries2.sql`.

Cannot be used together with `--query`.

**`-m [ --multiline ]`**

If specified, allow multiline queries (do not send the query on Enter). Queries will be sent only when they are ended with a semicolon.

### Query Settings {#command-line-options-query-settings}

Query settings can be specified as command-line options in the client, for example:
```bash
$ clickhouse-client --max_threads 1
```

See [Core Settings](../operations/settings/settings.md) for a list of settings.

### Formatting Options {#command-line-options-formatting}

**`-f [ --format ] <format>`**

Use the specified format to output the result.

See [Formats for Input and Output Data](formats.md) for a list of supported formats.

Default value: TabSeparated

**`--pager <command>`**

Pipe all output into this command. Typically `less` (e.g., `less -S` to display wide result sets) or similar.

**`-E [ --vertical ]`**

Use the [Vertical format](../interfaces/formats.md#vertical) to output the result. This is the same as `–-format Vertical`. In this format, each value is printed on a separate line, which is helpful when displaying wide tables.

### Execution Details {#command-line-options-execution-details}

**`--enable-progress-table-toggle`**

Enable toggling of the progress table by pressing the control key (Space). Only applicable in interactive mode with progress table printing enabled.

Default value: enabled

**`--hardware-utilization`**

Print hardware utilization information in progress bar.

**`--memory-usage`**

If specified, print memory usage to `stderr` in non-interactive mode.

Possible values:
- `none` - do not print memory usage
- `default` - print number of bytes
- `readable` - print memory usage in human-readable format

**`--print-profile-events`**

Print `ProfileEvents` packets.

**`--progress`**

Print progress of query execution.

Possible values:
- `tty|on|1|true|yes` - outputs to the terminal in interactive mode
- `err` - outputs to `stderr` in non-interactive mode
- `off|0|false|no` - disables progress printing

Default value: `tty` in interactive mode, `off` in non-interactive (batch) mode.

**`--progress-table`**

Print a progress table with changing metrics during query execution.

Possible values:
- `tty|on|1|true|yes` - outputs to the terminal in interactive mode
- `err` - outputs to `stderr` non-interactive mode
- `off|0|false|no` - disables the progress table

Default value: `tty` in interactive mode, `off` in non-interactive (batch) mode.

**`--stacktrace`**

Print stack traces of exceptions.

**`-t [ --time ]`**

Print query execution time to `stderr` in non-interactive mode (for benchmarks).