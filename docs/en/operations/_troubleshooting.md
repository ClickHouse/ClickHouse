
[//]: # (This file is included in FAQ > Troubleshooting)

- [Installation](#troubleshooting-installation-errors)
- [Connecting to the server](#troubleshooting-accepts-no-connections)
- [Query processing](#troubleshooting-does-not-process-queries)
- [Efficiency of query processing](#troubleshooting-too-slow)

## Installation {#troubleshooting-installation-errors}

### You Cannot Get Deb Packages from ClickHouse Repository with Apt-get {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

- Check firewall settings.
- If you cannot access the repository for any reason, download packages as described in the [install guide](../getting-started/install.md) article and install them manually using the `sudo dpkg -i <packages>` command. You will also need the `tzdata` package.

### You Cannot Update Deb Packages from ClickHouse Repository with Apt-get {#you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get}

- The issue may be happened when the GPG key is changed.

Please use the manual from the [setup](../getting-started/install.md#setup-the-debian-repository) page to update the repository configuration.


### You Get Different Warnings with `apt-get update` {#you-get-different-warnings-with-apt-get-update}

- The completed warning messages are as one of following:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

To resolve the above issue, please use the following script:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

### You Can't Get Packages With Yum Because Of Wrong Signature {#you-cant-get-packages-with-yum-because-of-wrong-signature}

Possible issue: the cache is wrong, maybe it's broken after updated GPG key in 2022-09.

The solution is to clean out the cache and lib directory for yum:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

After that follow the [install guide](../getting-started/install.md#from-rpm-packages)

### You Can't Run Docker Container {#you-cant-run-docker-container}

You are running a simple `docker run clickhouse/clickhouse-server` and it crashes with a stack trace similar to following:

```bash
$ docker run -it clickhouse/clickhouse-server
........
2024.11.06 21:04:48.912036 [ 1 ] {} <Information> SentryWriter: Sending crash reports is disabled
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

The reason is an old docker daemon with version lower than `20.10.10`. A way to fix it either upgrading it, or running `docker run [--privileged | --security-opt seccomp=unconfined]`. The latter has security implications.

## Connecting to the Server {#troubleshooting-accepts-no-connections}

Possible issues:

- The server is not running.
- Unexpected or wrong configuration parameters.

### Server Is Not Running {#server-is-not-running}

**Check if server is running**

Command:

```bash
$ sudo service clickhouse-server status
```

If the server is not running, start it with the command:

```bash
$ sudo service clickhouse-server start
```

**Check logs**

The main log of `clickhouse-server` is in `/var/log/clickhouse-server/clickhouse-server.log` by default.

If the server started successfully, you should see the strings:

- `<Information> Application: starting up.` — Server started.
- `<Information> Application: Ready for connections.` — Server is running and ready for connections.

If `clickhouse-server` start failed with a configuration error, you should see the `<Error>` string with an error description. For example:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

If you do not see an error at the end of the file, look through the entire file starting from the string:

```text
<Information> Application: starting up.
```

If you try to start a second instance of `clickhouse-server` on the server, you see the following log:

```text
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

If you do not find any useful information in `clickhouse-server` logs or there aren't any logs, you can view `system.d` logs using the command:

```bash
$ sudo journalctl -u clickhouse-server
```

**Start clickhouse-server in interactive mode**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

This command starts the server as an interactive app with standard parameters of the autostart script. In this mode `clickhouse-server` prints all the event messages in the console.

### Configuration Parameters {#configuration-parameters}

Check:

- Docker settings.

    If you run ClickHouse in Docker in an IPv6 network, make sure that `network=host` is set.

- Endpoint settings.

    Check [listen_host](../operations/server-configuration-parameters/settings.md#listen_host) and [tcp_port](../operations/server-configuration-parameters/settings.md#tcp_port) settings.

    ClickHouse server accepts localhost connections only by default.

- HTTP protocol settings.

    Check protocol settings for the HTTP API.

- Secure connection settings.

    Check:

    - The [tcp_port_secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure) setting.
    - Settings for [SSL certificates](../operations/server-configuration-parameters/settings.md#openssl).

    Use proper parameters while connecting. For example, use the `port_secure` parameter with `clickhouse_client`.

- User settings.

    You might be using the wrong user name or password.

## Query Processing {#troubleshooting-does-not-process-queries}

If ClickHouse is not able to process the query, it sends an error description to the client. In the `clickhouse-client` you get a description of the error in the console. If you are using the HTTP interface, ClickHouse sends the error description in the response body. For example:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

If you start `clickhouse-client` with the `stack-trace` parameter, ClickHouse returns the server stack trace with the description of an error.

You might see a message about a broken connection. In this case, you can repeat the query. If the connection breaks every time you perform the query, check the server logs for errors.

## Efficiency of Query Processing {#troubleshooting-too-slow}

If you see that ClickHouse is working too slowly, you need to profile the load on the server resources and network for your queries.

You can use the clickhouse-benchmark utility to profile queries. It shows the number of queries processed per second, the number of rows processed per second, and percentiles of query processing times.
