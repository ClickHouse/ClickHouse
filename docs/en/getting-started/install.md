---
toc_priority: 11
toc_title: Installation
---

# Installation {#installation}

## System Requirements {#system-requirements}

ClickHouse can run on any Linux, FreeBSD, or Mac OS X with x86_64, AArch64, or PowerPC64LE CPU architecture.

Official pre-built binaries are typically compiled for x86_64 and leverage SSE 4.2 instruction set, so unless otherwise stated usage of CPU that supports it becomes an additional system requirement. Here’s the command to check if current CPU has support for SSE 4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

To run ClickHouse on processors that do not support SSE 4.2 or have AArch64 or PowerPC64LE architecture, you should [build ClickHouse from sources](#from-sources) with proper configuration adjustments.

## Available Installation Options {#available-installation-options}

### From DEB Packages {#install-from-deb-packages}

It is recommended to use official pre-compiled `deb` packages for Debian or Ubuntu. Run these commands to install packages:

``` bash
{% include 'install/deb.sh' %}
```

If you want to use the most recent version, replace `stable` with `testing` (this is recommended for your testing environments).

You can also download and install packages manually from [here](https://repo.clickhouse.tech/deb/stable/main/).

#### Packages {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` and installs the default server configuration.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` and other client-related tools. and installs client configuration files.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### From RPM Packages {#from-rpm-packages}

It is recommended to use official pre-compiled `rpm` packages for CentOS, RedHat, and all other rpm-based Linux distributions.

First, you need to add the official repository:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

If you want to use the most recent version, replace `stable` with `testing` (this is recommended for your testing environments). `prestable` is sometimes also available.

Then run these commands to install packages:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

You can also download and install packages manually from [here](https://repo.clickhouse.tech/rpm/stable/x86_64).

### From Tgz Archives {#from-tgz-archives}

It is recommended to use official pre-compiled `tgz` archives for all Linux distributions, where installation of `deb` or `rpm` packages is not possible.

The required version can be downloaded with `curl` or `wget` from repository https://repo.clickhouse.tech/tgz/.
After that downloaded archives should be unpacked and installed with installation scripts. Example for the latest version:

``` bash
export LATEST_VERSION=`curl https://api.github.com/repos/ClickHouse/ClickHouse/tags 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1`
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-client-$LATEST_VERSION.tgz

tar -xzvf clickhouse-common-static-$LATEST_VERSION.tgz
sudo clickhouse-common-static-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-common-static-dbg-$LATEST_VERSION.tgz
sudo clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-server-$LATEST_VERSION.tgz
sudo clickhouse-server-$LATEST_VERSION/install/doinst.sh
sudo /etc/init.d/clickhouse-server start

tar -xzvf clickhouse-client-$LATEST_VERSION.tgz
sudo clickhouse-client-$LATEST_VERSION/install/doinst.sh
```

For production environments, it’s recommended to use the latest `stable`-version. You can find its number on GitHub page https://github.com/ClickHouse/ClickHouse/tags with postfix `-stable`.

### From Docker Image {#from-docker-image}

To run ClickHouse inside Docker follow the guide on [Docker Hub](https://hub.docker.com/r/yandex/clickhouse-server/). Those images use official `deb` packages inside.

### From Precompiled Binaries for Non-Standard Environments {#from-binaries-non-linux}

For non-Linux operating systems and for AArch64 CPU arhitecture, ClickHouse builds are provided as a cross-compiled binary from the latest commit of the `master` branch (with a few hours delay).

-   [macOS](https://builds.clickhouse.tech/master/macos/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/macos/clickhouse' && chmod a+x ./clickhouse`
-   [FreeBSD](https://builds.clickhouse.tech/master/freebsd/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/freebsd/clickhouse' && chmod a+x ./clickhouse`
-   [AArch64](https://builds.clickhouse.tech/master/aarch64/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/aarch64/clickhouse' && chmod a+x ./clickhouse`

After downloading, you can use the `clickhouse client` to connect to the server, or `clickhouse local` to process local data. To run `clickhouse server`, you have to additionally download [server](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.xml) and [users](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/users.xml) configuration files from GitHub.

These builds are not recommended for use in production environments because they are less thoroughly tested, but you can do so on your own risk. They also have only a subset of ClickHouse features available.

### From Sources {#from-sources}

To manually compile ClickHouse, follow the instructions for [Linux](../development/build.md) or [Mac OS X](../development/build-osx.md).

You can compile packages and install them or use programs without installing packages. Also by building manually you can disable SSE 4.2 requirement or build for AArch64 CPUs.

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

You’ll need to create a data and metadata folders and `chown` them for the desired user. Their paths can be changed in server config (src/programs/server/config.xml), by default they are:

      /var/lib/clickhouse/data/default/
      /var/lib/clickhouse/metadata/default/

On Gentoo, you can just use `emerge clickhouse` to install ClickHouse from sources.

## Launch {#launch}

To start the server as a daemon, run:

``` bash
$ sudo service clickhouse-server start
```

If you don’t have `service` command, run as

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

See the logs in the `/var/log/clickhouse-server/` directory.

If the server doesn’t start, check the configurations in the file `/etc/clickhouse-server/config.xml`.

You can also manually launch the server from the console:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

In this case, the log will be printed to the console, which is convenient during development.
If the configuration file is in the current directory, you don’t need to specify the `--config-file` parameter. By default, it uses `./config.xml`.

ClickHouse supports access restriction settings. They are located in the `users.xml` file (next to `config.xml`).
By default, access is allowed from anywhere for the `default` user, without a password. See `user/default/networks`.
For more information, see the section [“Configuration Files”](../operations/configuration-files.md).

After launching server, you can use the command-line client to connect to it:

``` bash
$ clickhouse-client
```

By default, it connects to `localhost:9000` on behalf of the user `default` without a password. It can also be used to connect to a remote server using `--host` argument.

The terminal must use UTF-8 encoding.
For more information, see the section [“Command-line client”](../interfaces/cli.md).

Example:

```
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

To continue experimenting, you can download one of the test data sets or go through [tutorial](https://clickhouse.tech/tutorial.html).

[Original article](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
