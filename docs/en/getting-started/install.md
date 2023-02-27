---
sidebar_label: Install
keywords: [clickhouse, install, getting started, quick start]
description: Install ClickHouse
slug: /en/install
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

# Install ClickHouse

You have three options for getting up and running with ClickHouse:

- **[ClickHouse Cloud](https://clickhouse.com/cloud/):** The official ClickHouse as a service, - built by, maintained and supported by the creators of ClickHouse
- **[Self-managed ClickHouse](#self-managed-install):** ClickHouse can run on any Linux, FreeBSD, or macOS with x86-64, ARM, or PowerPC64LE CPU architecture
- **[Docker Image](https://hub.docker.com/r/clickhouse/clickhouse-server/):** Read the guide with the official image in Docker Hub

## ClickHouse Cloud

The quickest and easiest way to get up and running with ClickHouse is to create a new service in [ClickHouse Cloud](https://clickhouse.cloud/).

## Self-Managed Install

:::tip
For production installs of a specific release version see the [installation options](#available-installation-options) down below.
:::

<Tabs>
<TabItem value="linux" label="Linux" default>

1. The simplest way to download ClickHouse locally is to run the following command. If your operating system is supported, an appropriate ClickHouse binary will be downloaded and made runnable:

  ```bash
  curl https://clickhouse.com/ | sh
  ```

1. Run the `install` command, which defines a collection of useful symlinks along with the files and folders used by ClickHouse - all of which you can see in the output of the install script:

  ```bash
  sudo ./clickhouse install
  ```

1. At the end of the install script, you are prompted for a password for the `default` user. Feel free to enter a password, or you can optionally leave it blank:

  ```response
  Creating log directory /var/log/clickhouse-server.
  Creating data directory /var/lib/clickhouse.
  Creating pid directory /var/run/clickhouse-server.
   chown -R clickhouse:clickhouse '/var/log/clickhouse-server'
   chown -R clickhouse:clickhouse '/var/run/clickhouse-server'
   chown  clickhouse:clickhouse '/var/lib/clickhouse'
  Enter password for default user:
  ```
  You should see the following output:

  ```response
   ClickHouse has been successfully installed.

   Start clickhouse-server with:
    sudo clickhouse start

   Start clickhouse-client with:
    clickhouse-client
  ```

1. Run the following command to start the ClickHouse server:
    ```bash
    sudo clickhouse start
    ```

</TabItem>
<TabItem value="macos" label="macOS">

1. The simplest way to download ClickHouse locally is to run the following command. If your operating system is supported, an appropriate ClickHouse binary will be downloaded and made runnable:
  ```bash
  curl https://clickhouse.com/ | sh
  ```

1. Run the ClickHouse server:

  ```bash
  ./clickhouse server
  ```

1. Open a new terminal and use the **clickhouse-client** to connect to your service:

  ```bash
  ./clickhouse client
  ```

  ```response
  ./clickhouse client
  ClickHouse client version 23.2.1.1501 (official build).
  Connecting to localhost:9000 as user default.
  Connected to ClickHouse server version 23.2.1 revision 54461.

  local-host :)
  ```

  You are ready to start sending DDL and SQL commands to ClickHouse!

</TabItem>
</Tabs>


:::tip
The [Quick Start](/docs/en/quick-start.mdx/#step-1-get-clickhouse) walks through the steps to download and run ClickHouse, connect to it, and insert data.
:::

## Available Installation Options {#available-installation-options}

### From DEB Packages {#install-from-deb-packages}

It is recommended to use official pre-compiled `deb` packages for Debian or Ubuntu. Run these commands to install packages:

#### Setup the Debian repository
``` bash
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
```

#### Install ClickHouse server and client
```bash
sudo apt-get install -y clickhouse-server clickhouse-client
```

#### Start ClickHouse server

```bash
sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you've set up a password.
```

<details>
<summary>Deprecated Method for installing deb-packages</summary>

``` bash
sudo apt-get install apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

echo "deb https://repo.clickhouse.com/deb/stable/ main/" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you set up a password.
```

</details>

<details>
<summary>Migration Method for installing the deb-packages</summary>

```bash
sudo apt-key del E0C56BD4
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you set up a password.
```

</details>

You can replace `stable` with `lts` to use different [release kinds](/docs/en/faq/operations/production.md) based on your needs.

You can also download and install packages manually from [here](https://packages.clickhouse.com/deb/pool/main/c/).

#### Install standalone ClickHouse Keeper

:::tip
If you are going to run ClickHouse Keeper on the same server as ClickHouse server you
do not need to install ClickHouse Keeper as it is included with ClickHouse server.  This command is only needed on standalone ClickHouse Keeper servers.
:::

```bash
sudo apt-get install -y clickhouse-keeper
```

#### Enable and start ClickHouse Keeper

```bash
sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper
```

#### Packages {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` and installs the default server configuration.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` and other client-related tools. and installs client configuration files.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.
-   `clickhouse-keeper` - Used to install ClickHouse Keeper on dedicated ClickHouse Keeper nodes.  If you are running ClickHouse Keeper on the same server as ClickHouse server, then you do not need to install this package. Installs ClickHouse Keeper and the default ClickHouse Keeper configuration files.

:::info
If you need to install specific version of ClickHouse you have to install all packages with the same version:
`sudo apt-get install clickhouse-server=21.8.5.7 clickhouse-client=21.8.5.7 clickhouse-common-static=21.8.5.7`
:::

### From RPM Packages {#from-rpm-packages}

It is recommended to use official pre-compiled `rpm` packages for CentOS, RedHat, and all other rpm-based Linux distributions.

#### Setup the RPM repository
First, you need to add the official repository:

``` bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
```

#### Install ClickHouse server and client

```bash
sudo yum install -y clickhouse-server clickhouse-client
```

#### Start ClickHouse server

```bash
sudo systemctl enable clickhouse-server
sudo systemctl start clickhouse-server
sudo systemctl status clickhouse-server
clickhouse-client # or "clickhouse-client --password" if you set up a password.
```

#### Install standalone ClickHouse Keeper

:::tip
If you are going to run ClickHouse Keeper on the same server as ClickHouse server you
do not need to install ClickHouse Keeper as it is included with ClickHouse server.  This command is only needed on standalone ClickHouse Keeper servers.
:::

```bash
sudo yum install -y clickhouse-keeper
```

#### Enable and start ClickHouse Keeper

```bash
sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper
```

<details markdown="1">

<summary>Deprecated Method for installing rpm-packages</summary>

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.com/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.com/rpm/clickhouse.repo
sudo yum install clickhouse-server clickhouse-client

sudo /etc/init.d/clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you set up a password.
```

</details>

You can replace `stable` with `lts` to use different [release kinds](/docs/en/faq/operations/production.md) based on your needs.

Then run these commands to install packages:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

You can also download and install packages manually from [here](https://packages.clickhouse.com/rpm/stable).

### From Tgz Archives {#from-tgz-archives}

It is recommended to use official pre-compiled `tgz` archives for all Linux distributions, where installation of `deb` or `rpm` packages is not possible.

The required version can be downloaded with `curl` or `wget` from repository https://packages.clickhouse.com/tgz/.
After that downloaded archives should be unpacked and installed with installation scripts. Example for the latest stable version:

``` bash
LATEST_VERSION=$(curl -s https://packages.clickhouse.com/tgz/stable/ | \
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -V -r | head -n 1)
export LATEST_VERSION

case $(uname -m) in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) echo "Unknown architecture $(uname -m)"; exit 1 ;;
esac

for PKG in clickhouse-common-static clickhouse-common-static-dbg clickhouse-server clickhouse-client clickhouse-keeper
do
  curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION-${ARCH}.tgz" \
    || curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION.tgz"
done

tar -xzvf "clickhouse-common-static-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-server-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-server-$LATEST_VERSION.tgz"
sudo "clickhouse-server-$LATEST_VERSION/install/doinst.sh" configure
sudo /etc/init.d/clickhouse-server start

tar -xzvf "clickhouse-client-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-client-$LATEST_VERSION.tgz"
sudo "clickhouse-client-$LATEST_VERSION/install/doinst.sh"
```

<details markdown="1">

<summary>Deprecated Method for installing tgz archives</summary>

``` bash
export LATEST_VERSION=$(curl -s https://repo.clickhouse.com/tgz/stable/ | \
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -V -r | head -n 1)
curl -O https://repo.clickhouse.com/tgz/stable/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.com/tgz/stable/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.com/tgz/stable/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.com/tgz/stable/clickhouse-client-$LATEST_VERSION.tgz

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
</details>

For production environments, it’s recommended to use the latest `stable`-version. You can find its number on GitHub page https://github.com/ClickHouse/ClickHouse/tags with postfix `-stable`.

### From Docker Image {#from-docker-image}

To run ClickHouse inside Docker follow the guide on [Docker Hub](https://hub.docker.com/r/clickhouse/clickhouse-server/). Those images use official `deb` packages inside.

### From Sources {#from-sources}

To manually compile ClickHouse, follow the instructions for [Linux](/docs/en/development/build.md) or [macOS](/docs/en/development/build-osx.md).

You can compile packages and install them or use programs without installing packages.

      Client: <build_directory>/programs/clickhouse-client
      Server: <build_directory>/programs/clickhouse-server

You’ll need to create data and metadata folders manually and `chown` them for the desired user. Their paths can be changed in server config (src/programs/server/config.xml), by default they are:

      /var/lib/clickhouse/data/default/
      /var/lib/clickhouse/metadata/default/

On Gentoo, you can just use `emerge clickhouse` to install ClickHouse from sources.

### From CI checks pre-built binaries
ClickHouse binaries are built for each [commit](/docs/en/development/build.md#you-dont-have-to-build-clickhouse).

## Launch {#launch}

To start the server as a daemon, run:

``` bash
$ sudo clickhouse start
```

There are also other ways to run ClickHouse:

``` bash
$ sudo service clickhouse-server start
```

If you do not have `service` command, run as

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

If you have `systemctl` command, run as

``` bash
$ sudo systemctl start clickhouse-server.service
```

See the logs in the `/var/log/clickhouse-server/` directory.

If the server does not start, check the configurations in the file `/etc/clickhouse-server/config.xml`.

You can also manually launch the server from the console:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

In this case, the log will be printed to the console, which is convenient during development.
If the configuration file is in the current directory, you do not need to specify the `--config-file` parameter. By default, it uses `./config.xml`.

ClickHouse supports access restriction settings. They are located in the `users.xml` file (next to `config.xml`).
By default, access is allowed from anywhere for the `default` user, without a password. See `user/default/networks`.
For more information, see the section [“Configuration Files”](/docs/en/operations/configuration-files.md).

After launching server, you can use the command-line client to connect to it:

``` bash
$ clickhouse-client
```

By default, it connects to `localhost:9000` on behalf of the user `default` without a password. It can also be used to connect to a remote server using `--host` argument.

The terminal must use UTF-8 encoding.
For more information, see the section [“Command-line client”](/docs/en/interfaces/cli.md).

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

To continue experimenting, you can download one of the test data sets or go through [tutorial](/docs/en/tutorial.md).

## Recommendations for Self-Managed ClickHouse

ClickHouse can run on any Linux, FreeBSD, or macOS with x86-64, ARM, or PowerPC64LE CPU architecture.

ClickHouse uses all hardware resources available to process data.

ClickHouse tends to work more efficiently with a large number of cores at a lower clock rate than with fewer cores at a higher clock rate.

We recommend using a minimum of 4GB of RAM to perform non-trivial queries. The ClickHouse server can run with a much smaller amount of RAM, but queries will then frequently abort.

The required volume of RAM generally depends on:

-   The complexity of queries.
-   The amount of data that is processed in queries.

To calculate the required volume of RAM, you may estimate the size of temporary data for [GROUP BY](/docs/en/sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](/docs/en/sql-reference/statements/select/distinct.md#select-distinct), [JOIN](/docs/en/sql-reference/statements/select/join.md#select-join) and other operations you use.

To reduce memory consumption, ClickHouse can swap temporary data to external storage. See [GROUP BY in External Memory](/docs/en/sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) for details.

We recommend to disable the operating system's swap file in production environments.

The ClickHouse binary requires at least 2.5 GB of disk space for installation.

The volume of storage required for your data may be calculated separately based on

-   an estimation of the data volume.

    You can take a sample of the data and get the average size of a row from it. Then multiply the value by the number of rows you plan to store.

-   The data compression coefficient.

    To estimate the data compression coefficient, load a sample of your data into ClickHouse, and compare the actual size of the data with the size of the table stored. For example, clickstream data is usually compressed by 6-10 times.

To calculate the final volume of data to be stored, apply the compression coefficient to the estimated data volume. If you plan to store data in several replicas, then multiply the estimated volume by the number of replicas.

For distributed ClickHouse deployments (clustering), we recommend at least 10G class network connectivity.

Network bandwidth is critical for processing distributed queries with a large amount of intermediate data. Besides, network speed affects replication processes.
