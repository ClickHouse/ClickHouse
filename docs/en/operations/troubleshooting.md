# Troubleshooting

Known problems:

- [Installation errors](#troubleshooting-installation-errors).
- [The server does not accept the connections](#troubleshooting-accepts-no-connections).
- [ClickHouse does not process queries](#troubleshooting-does-not-process-queries).
- [ClickHouse processes queries too slow](#troubleshooting-too-slow).

## Installation errors {#troubleshooting-installation-errors}

### You can not get deb-packages from ClickHouse repository with apt-get

- Check firewall settings.
- If you can not access the repository by any reason, download packages as described in the [Getting started](../getting_started/index.md) article and install them manually with `sudo dpkg -i <packages>` command. Also, you need `tzdata` package.


## The server does not accept the connections {#troubleshooting-accepts-no-connections}

Possible reasons:

- The server is not running.
- Unexpected or wrong configuration parameters.

### The server is not running

**Check if server is runnnig**

Command:

```
sudo service clickhouse-server status
```

If the server is not running, start it with the command:

```
sudo service clickhouse-server start
```

**See logs**

The main log of `clickhouse-server` is in `/var/log/clickhouse-server.log`.

In case of successful start you should see the strings:

    - `starting up` — Server started to run.
    - `Ready for connections` — Server runs and ready for connections.

If `clickhouse-server` start failed by the configuration error you should see the `<Error>` string with an error description. For example:

```
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

If you don't see an error at the end of file look through all the file from the string:

```
<Information> Application: starting up.
```

If you try to start the second instance of `clickhouse-server` at the server you see the following log:

```
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

If there is no any useful information in `clickhouse-server` logs or there is no any logs, you can see `system.d` logs by the command:

```
sudo journalctl -u clickhouse-server
```

**Start clickhouse-server in interactive mode**

```
sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

This command starts the server as an interactive app with standard parameters of autostart script. In this mode `clickhouse-server` prints all the event messages into the console.

### Configuration parameters

Check:

- Docker settings.

    If you run ClickHouse in Docker in IPv6 network, check that `network=host` is set.

- Endpoint settings.

    Check [listen_host](server_settings/settings.md#server_settings-listen_host) and [tcp_port](server_settings/settings.md#server_settings-tcp_port) settings.

    ClickHouse server accepts localhost connections only by default.

- HTTP protocol settings.

    Check protocol settings for HTTP API.

- Secure connection settings.

    Check setting `tcp_port_secure` for secure connection. Use proper parameters while connecting. For example, use parameter `port_secure` with `clickhouse_client`. Check also settings for ssl-sertificates.

- User settings

    You may use the wrong user name or password for it.

## ClickHouse does not process queries {#troubleshooting-does-not-process-queries}

If ClickHouse can not process the query, it sends the description of an error to the client. In the `clickhouse-client` you get a description of an error in console. If you use HTTP interface, ClickHouse sends error description in response body. For example,

```
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there is no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

If you start `clickhouse-client` with `stack-trace` parameter, ClickHouse returns server stack trace with the description of an error.

It is possible that you see the message of connection broken. In this case, you can repeat query. If connection brakes any time you perform the query you should check the server logs for errors.

## ClickHouse processes queries too slow {#troubleshooting-too-slow}

If you see that ClickHouse works too slow, you need to profile the load of the server resources and network for your queries.

You can use clickhouse-benchmark utility to profile queries. It shows the number of queries processed in a second, the number of rows processed in a second and percentiles of query processing times.
