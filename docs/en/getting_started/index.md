# Getting started

## System requirements

This is not a cross-platform system. It requires Linux Ubuntu Precise (12.04) or newer, with x86_64 architecture and support for the SSE 4.2 instruction set.
To check for SSE 4.2:

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

We recommend using Ubuntu Trusty, Ubuntu Xenial, or Ubuntu Precise.
The terminal must use UTF-8 encoding (the default in Ubuntu).

## Installation

For testing and development, the system can be installed on a single server or on a desktop computer.

### Installing from packages Debian/Ubuntu

In `/etc/apt/sources.list` (or in a separate `/etc/apt/sources.list.d/clickhouse.list` file), add the repository:

```text
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

If you want to use the most recent test version, replace 'stable' with 'testing'.

Then run:

```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

You can also download and install packages manually from here:
<https://repo.yandex.ru/clickhouse/deb/stable/main/>

ClickHouse contains access restriction settings. They are located in the 'users.xml' file (next to 'config.xml').
By default, access is allowed from anywhere for the 'default' user, without a password. See 'user/default/networks'.
For more information, see the section "Configuration files".

### Installing from sources

To compile, follow the instructions: build.md

You can compile packages and install them.
You can also use programs without installing packages.

```text
Client: dbms/src/Client/
Server: dbms/src/Server/
```

For the server, create a catalog with data, such as:

```text
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

(Configurable in the server config.)
Run 'chown' for the desired user.

Note the path to logs in the server config (src/dbms/src/Server/config.xml).

### Other installation methods

Docker image: <https://hub.docker.com/r/yandex/clickhouse-server/>

RPM packages for CentOS or RHEL: <https://github.com/Altinity/clickhouse-rpm-install>

Gentoo overlay: <https://github.com/kmeaw/clickhouse-overlay>

## Launch

To start the server (as a daemon), run:

```bash
sudo service clickhouse-server start
```

See the logs in the `/var/log/clickhouse-server/ directory.`

If the server doesn't start, check the configurations in the file `/etc/clickhouse-server/config.xml.`

You can also launch the server from the console:

```bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

In this case, the log will be printed to the console, which is convenient during development.
If the configuration file is in the current directory, you don't need to specify the '--config-file' parameter. By default, it uses './config.xml'.

You can use the command-line client to connect to the server:

```bash
clickhouse-client
```

The default parameters indicate connecting with localhost:9000 on behalf of the user 'default' without a password.
The client can be used for connecting to a remote server.
Example:

```bash
clickhouse-client --host=example.com
```

For more information, see the section "Command-line client".

Checking the system:

```bash
milovidov@hostname:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**Congratulations, the system works!**

To continue experimenting, you can try to download from the test data sets.
