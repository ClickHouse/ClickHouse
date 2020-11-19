---
toc_priority: 46
toc_title: Troubleshooting
---

# Troubleshooting {#troubleshooting}

-   [Installation](#troubleshooting-installation-errors)
-   [Connecting to the server](#troubleshooting-accepts-no-connections)
-   [Query processing](#troubleshooting-does-not-process-queries)
-   [Efficiency of query processing](#troubleshooting-too-slow)

## Installation {#troubleshooting-installation-errors}

### You Cannot Get Deb Packages from ClickHouse Repository with Apt-get {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   Check firewall settings.
-   If you cannot access the repository for any reason, download packages as described in the [Getting started](../getting-started/index.md) article and install them manually using the `sudo dpkg -i <packages>` command. You will also need the `tzdata` package.

## Connecting to the Server {#troubleshooting-accepts-no-connections}

Possible issues:

-   The server is not running.
-   Unexpected or wrong configuration parameters.

### Server Is Not Running {#server-is-not-running}

**Check if server is runnnig**

Command:

``` bash
$ sudo service clickhouse-server status
```

If the server is not running, start it with the command:

``` bash
$ sudo service clickhouse-server start
```

**Check logs**

The main log of `clickhouse-server` is in `/var/log/clickhouse-server/clickhouse-server.log` by default.

If the server started successfully, you should see the strings:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

If `clickhouse-server` start failed with a configuration error, you should see the `<Error>` string with an error description. For example:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

If you don’t see an error at the end of the file, look through the entire file starting from the string:

``` text
<Information> Application: starting up.
```

If you try to start a second instance of `clickhouse-server` on the server, you see the following log:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**See system.d logs**

If you don’t find any useful information in `clickhouse-server` logs or there aren’t any logs, you can view `system.d` logs using the command:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Start clickhouse-server in interactive mode**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

This command starts the server as an interactive app with standard parameters of the autostart script. In this mode `clickhouse-server` prints all the event messages in the console.

### Configuration Parameters {#configuration-parameters}

Check:

-   Docker settings.

    If you run ClickHouse in Docker in an IPv6 network, make sure that `network=host` is set.

-   Endpoint settings.

    Check [listen\_host](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) and [tcp\_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) settings.

    ClickHouse server accepts localhost connections only by default.

-   HTTP protocol settings.

    Check protocol settings for the HTTP API.

-   Secure connection settings.

    Check:

    -   The [tcp\_port\_secure](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) setting.
    -   Settings for [SSL certificates](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    Use proper parameters while connecting. For example, use the `port_secure` parameter with `clickhouse_client`.

-   User settings.

    You might be using the wrong user name or password.

## Query Processing {#troubleshooting-does-not-process-queries}

If ClickHouse is not able to process the query, it sends an error description to the client. In the `clickhouse-client` you get a description of the error in the console. If you are using the HTTP interface, ClickHouse sends the error description in the response body. For example:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

If you start `clickhouse-client` with the `stack-trace` parameter, ClickHouse returns the server stack trace with the description of an error.

You might see a message about a broken connection. In this case, you can repeat the query. If the connection breaks every time you perform the query, check the server logs for errors.

## Efficiency of Query Processing {#troubleshooting-too-slow}

If you see that ClickHouse is working too slowly, you need to profile the load on the server resources and network for your queries.

You can use the clickhouse-benchmark utility to profile queries. It shows the number of queries processed per second, the number of rows processed per second, and percentiles of query processing times.
