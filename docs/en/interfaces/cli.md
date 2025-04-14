---
description: 'Documentation for the ClickHouse command-line client interface'
sidebar_label: 'ClickHouse Client'
sidebar_position: 17
slug: /interfaces/cli
title: 'ClickHouse Client'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png'

ClickHouse provides a native command-line client for executing SQL queries directly against a ClickHouse server. It supports both interactive mode (for live query execution) and batch mode (for scripting and automation). Query results can be displayed in the terminal or exported to a file, with support for all ClickHouse output [formats](formats.md), such as Pretty, CSV, JSON, and more.

The client provides real-time feedback on query execution with a progress bar and the number of rows read, bytes processed and query execution time. It supports both [command-line options](#command-line-options) and [configuration files](#configuration_files).


## Install {#install}

To download ClickHouse, run:

```bash
curl https://clickhouse.com/ | sh
```

To also install it, run:
```bash
sudo ./clickhouse install
```

See [Install ClickHouse](../getting-started/install.md) for more installation options.

Different client and server versions are compatible with one another, but some features may not be available in older clients. We recommend using the same version for client and server.


## Run {#run}

:::note
If you only downloaded but did not install ClickHouse, use `./clickhouse client` instead of `clickhouse-client`.
:::

To connect to a ClickHouse server, run:

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

Specify additional connection details as necessary:

**`--port <port>`** - The port ClickHouse server is accepting connections on. The default ports are 9440 (TLS) and 9000 (no TLS). Note that ClickHouse Client uses the native protocol and not HTTP(S).

**`-s [ --secure ]`** - Whether to use TLS (usually autodetected).

**`-u [ --user ] <username>`** - The database user to connect as. Connects as the `default` user by default.

**`--password <password>`** - The password of the database user. You can also specify the password for a connection in the configuration file. If you do not specify the password, the client will ask for it.

**`-c [ --config ] <path-to-file>`** - The location of the configuration file for ClickHouse Client, if it is not at one of the default locations. See [Configuration Files](#configuration_files).

**`--connection <name>`** - The name of preconfigured connection details from the configuration file.

For a complete list of command-line options, see [Command Line Options](#command-line-options).


### Connecting to ClickHouse Cloud {#connecting-cloud}

The details for your ClickHouse Cloud service are available in the ClickHouse Cloud console. Select the service that you want to connect to and click **Connect**:

<Image img={cloud_connect_button}
  size="md"
  alt="ClickHouse Cloud service connect button"
/>

<br/><br/>

Choose **Native**, and the details are shown with an example `clickhouse-client` command:

<Image img={connection_details_native}
  size="md"
  alt="ClickHouse Cloud Native TCP connection details"
/>


### Storing connections in a configuration file {#connection-credentials}

You can store connection details for one or more ClickHouse servers in a [configuration file](#configuration_files).

The format looks like this:
```xml
<config>
    <connections_credentials>
        <name>default</name>
        <hostname>hostname</hostname>
        <port>9440</port>
        <secure>1</secure>
        <user>default</user>
        <password>password</password>
    </connections_credentials>
</config>
```

See the [section on configuration files](#configuration_files) for more information.

:::note
To concentrate on the query syntax, the rest of the examples leave off the connection details (`--host`, `--port`, etc.). Remember to add them when you use the commands.
:::

## Batch mode {#batch-mode}

Instead of using ClickHouse Client interactively, you can run it in batch mode.

You can specify a single query like this:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

You can also use the `--query` command-line option:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

You can provide a query on `stdin`:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

Inserting data:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

When `--query` is specified, any input is appended to the request after a line feed.

**Inserting a CSV file into a remote ClickHouse service**

This example is inserting a sample dataset CSV file, `cell_towers.csv` into an existing table `cell_towers` in the `default` database:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

**More examples of inserting data**

```bash
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


## Notes {#notes}

In interactive mode, the default output format is `PrettyCompact`. You can change the format in the `FORMAT` clause of the query or by specifying the `--format` command-line option. To use the Vertical format, you can use `--vertical` or specify `\G` at the end of the query. In this format, each value is printed on a separate line, which is convenient for wide tables.

In batch mode, the default data [format](formats.md) is `TabSeparated`. You can set the format in the `FORMAT` clause of the query.

In interactive mode, by default whatever was entered is run when you press `Enter`. A semicolon is not necessary at the end of the query.

You can start the client with the `-m, --multiline` parameter. To enter a multiline query, enter a backslash `\` before the line feed. After you press `Enter`, you will be asked to enter the next line of the query. To run the query, end it with a semicolon and press `Enter`.

ClickHouse Client is based on `replxx` (similar to `readline`) so it uses familiar keyboard shortcuts and keeps a history. The history is written to `~/.clickhouse-client-history` by default.

To exit the client, press `Ctrl+D`, or enter one of the following instead of a query: `exit`, `quit`, `logout`, `exit;`, `quit;`, `logout;`, `q`, `Q`, `:q`.

When processing a query, the client shows:

1.  Progress, which is updated no more than 10 times per second by default. For quick queries, the progress might not have time to be displayed.
2.  The formatted query after parsing, for debugging.
3.  The result in the specified format.
4.  The number of lines in the result, the time passed, and the average speed of query processing. All data amounts refer to uncompressed data.

You can cancel a long query by pressing `Ctrl+C`. However, you will still need to wait for a little for the server to abort the request. It is not possible to cancel a query at certain stages. If you do not wait and press `Ctrl+C` a second time, the client will exit.

ClickHouse Client allows passing external data (external temporary tables) for querying. For more information, see the section [External data for query processing](../engines/table-engines/special/external-data.md).


## Queries with parameters {#cli-queries-with-parameters}

You can specify parameters in a query and pass values to it with command-line options. This avoids formatting a query with specific dynamic values on the client side. For example:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

It is also possible to set parameters from within an interactive session:
```bash
$ clickhouse-client --query "SET param_parName='[1, 2]'; SELECT {parName:Array(UInt16)}"
```

### Query Syntax {#cli-queries-with-parameters-syntax}

In the query, place the values that you want to fill using command-line parameters in braces in the following format:

```sql
{<name>:<data type>}
```

- `name` — Placeholder identifier. The corresponding command-line option is `--param_<name> = value`.
- `data type` — [Data type](../sql-reference/data-types/index.md) of the parameter. For example, a data structure like `(integer, ('string', integer))` can have the `Tuple(UInt8, Tuple(String, UInt8))` data type (you can also use other [integer](../sql-reference/data-types/int-uint.md) types). It is also possible to pass the table name, database name, and column names as parameters, in that case you would need to use `Identifier` as the data type.

### Examples {#cli-queries-with-parameters-examples}

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```


## Aliases {#cli_aliases}

- `\l` - SHOW DATABASES
- `\d` - SHOW TABLES
- `\c <DATABASE>` - USE DATABASE
- `.` - repeat the last query


## Keyboard shortcuts {#keyboard_shortcuts}

- `Alt (Option) + Shift + e` - open editor with the current query. It is possible to specify the editor to use with the environment variable `EDITOR`. By default, `vim` is used.
- `Alt (Option) + #` - comment line.
- `Ctrl + r` - fuzzy history search.

The full list with all available keyboard shortcuts is available at [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
To configure the correct work of the meta key (Option) on MacOS:

iTerm2: Go to Preferences -> Profile -> Keys -> Left Option key and click Esc+
:::


## Connection string {#connection_string}

ClickHouse Client alternatively supports connecting to a ClickHouse server using a connection string similar to [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). It has the following syntax:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

**Components**

- `user` - (optional) Database username. Default: `default`.
- `password` - (optional) Database user password. If `:` is specified and the password is blank, the client will prompt for the user's password.
- `hosts_and_ports` - (optional) List of hosts and optional ports `host[:port] [, host:[port]], ...`. Default: `localhost:9000`.
- `database` - (optional) Database name. Default: `default`.
- `query_parameters` - (optional) List of key-value pairs `param1=value1[,&param2=value2], ...`. For some parameters, no value is required. Parameter names and values are case-sensitive.

If the username, password or database was specified in the connection string, it cannot be specified using `--user`, `--password` or `--database` (and vice versa).

The host component can either be a hostname or an IPv4 or IPv6 address. IPv6 addresses should be in square brackets:

```text
clickhouse://[2001:db8::1234]
```

Connection strings can contain multiple hosts. ClickHouse Client will try to connect to these hosts in order (from left to right). After the connection is established, no attempt to connect to the remaining hosts is made.

The connection string must be specified as the first argument of `clickHouse-client`. The connection string can be combined with arbitrary other [command-line options](#command-line-options) except `--host` and `--port`.

The following keys are allowed for `query_parameters`:

- `secure` or shorthanded `s`. If specified, the client will connect to the server over a secure connection (TLS). See `--secure` in the [command-line options](#command-line-options).

**Percent encoding**

Non-US ASCII, spaces and special characters in the `user`, `password`, `hosts`, `database` and `query parameters` must be [percent-encoded](https://en.wikipedia.org/wiki/URL_encoding).

### Examples {#connection_string_examples}

Connect to `localhost` on port 9000 and execute the query `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Connect to `localhost` as user `john` with password `secret`, host `127.0.0.1` and port `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Connect to `localhost` as the `default` user, host with IPV6 address `[::1]` and port `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

Connect to `localhost` on port 9000 in multiline mode.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Connect to `localhost` using port 9000 as the user `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Connect to `localhost` on port 9000 and default to the `my_database` database.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Connect to `localhost` on port 9000 and default to the `my_database` database specified in the connection string and a secure connection using the shorthanded `s` parameter.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Connect to the default host using the default port, the default user, and the default database.

```bash
clickhouse-client clickhouse:
```

Connect to the default host using the default port, as the user `my_user` and no password.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Connect to `localhost` using the email as the user name. `@` symbol is percent encoded to `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Connect to one of two hosts: `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```


## Query ID format {#query-id-format}

In interactive mode ClickHouse Client shows the query ID for every query. By default, the ID is formatted like this:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

A custom format may be specified in a configuration file inside a `query_id_formats` tag. The `{query_id}` placeholder in the format string is replaced with the query ID. Several format strings are allowed inside the tag.
This feature can be used to generate URLs to facilitate profiling of queries.

**Example**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

With the configuration above, the ID of a query is shown in the following format:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```


## Configuration Files {#configuration_files}

ClickHouse Client uses the first existing file of the following:

- A file that is defined with the `-c [ -C, --config, --config-file ]` parameter.
- `./clickhouse-client.[xml|yaml|yml]`
- `~/.clickhouse-client/config.[xml|yaml|yml]`
- `/etc/clickhouse-client/config.[xml|yaml|yml]`

See the sample configuration file in the ClickHouse repository: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

Example XML syntax:

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

The same configuration in YAML format:

```yaml
user: username
password: 'password'
secure: true
openSSL:
  client:
    caConfig: '/etc/ssl/cert.pem'
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

The query to run in batch mode. Can be specified multiple times (`--query "SELECT 1" --query "SELECT 2"`) or once with multiple semicolon-separated queries (`--query "SELECT 1; SELECT 2;"`). In the latter case, `INSERT` queries with formats other than `VALUES` must be separated by empty lines.

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

See [Settings](../operations/settings/settings.md) for a list of settings.

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
