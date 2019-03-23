# Getting Started

## System Requirements

ClickHouse can run on any Linux, FreeBSD or Mac OS X with x86\_64 CPU architecture.

Though pre-built binaries are typically compiled to leverage SSE 4.2 instruction set, so unless otherwise stated usage of CPU that supports it becomes an additional system requirement. Here's the command to check if current CPU has support for SSE 4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## Installation

### From DEB Packages

Yandex ClickHouse team recommends using official pre-compiled `deb` packages for Debian or Ubuntu.

To install official packages add the Yandex repository in `/etc/apt/sources.list` or in a separate `/etc/apt/sources.list.d/clickhouse.list` file:

```
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

If you want to use the most recent version, replace `stable` with `testing` (this is not recommended for production environments).

Then run these commands to actually install packages:

```bash
sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

You can also download and install packages manually from here: <https://repo.yandex.ru/clickhouse/deb/stable/main/>.

### From RPM Packages

Yandex does not run ClickHouse on `rpm` based Linux distributions and `rpm` packages are not as thoroughly tested. So use them at your own risk, but there are many other companies that do successfully run them in production without any major issues.

For CentOS, RHEL or Fedora there are the following options:

* Packages from <https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/> are generated from official `deb` packages by Yandex and have byte-identical binaries.
* Packages from <https://github.com/Altinity/clickhouse-rpm-install> are built by independent company Altinity, but are used widely without any complaints.
* Or you can use Docker (see below).

### From Docker Image

To run ClickHouse inside Docker follow the guide on [Docker Hub](https://hub.docker.com/r/yandex/clickhouse-server/). Those images use official `deb` packages inside.

### From Sources

To manually compile ClickHouse, follow the instructions for [Linux](../development/build.md) or [Mac OS X](../development/build_osx.md).

You can compile packages and install them or use programs without installing packages. Also by building manually you can disable SSE 4.2 requirement or build for AArch64 CPUs.

```
Client: dbms/programs/clickhouse-client
Server: dbms/programs/clickhouse-server
```

You'll need to create a data and metadata folders and `chown` them for the desired user. Their paths can be changed in server config (src/dbms/programs/server/config.xml), by default they are:
```
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

On Gentoo you can just use `emerge clickhouse` to install ClickHouse from sources.

## Launch

To start the server as a daemon, run:

``` bash
$ sudo service clickhouse-server start
```

See the logs in the `/var/log/clickhouse-server/` directory.

If the server doesn't start, check the configurations in the file `/etc/clickhouse-server/config.xml`.

You can also manually launch the server from the console:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

In this case, the log will be printed to the console, which is convenient during development.
If the configuration file is in the current directory, you don't need to specify the `--config-file` parameter. By default, it uses `./config.xml`.

ClickHouse supports access restriction settings. They are located in the `users.xml` file (next to `config.xml`).
By default, access is allowed from anywhere for the `default` user, without a password. See `user/default/networks`.
For more information, see the section ["Configuration Files"](../operations/configuration_files.md).

After launching server, you can use the command-line client to connect to it:

``` bash
$ clickhouse-client
```

By default it connects to `localhost:9000` on behalf of the user `default` without a password. It can also be used to connect to a remote server using `--host` argument.

The terminal must use UTF-8 encoding.
For more information, see the section ["Command-line client"](../interfaces/cli.md).

Example:
``` bash
$ ./clickhouse-client
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

To continue experimenting, you can download one of test data sets or go through [tutorial](https://clickhouse.yandex/tutorial.html).


[Original article](https://clickhouse.yandex/docs/en/getting_started/) <!--hide-->
