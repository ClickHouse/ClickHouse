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

You have four options for getting up and running with ClickHouse:

- **[ClickHouse Cloud](https://clickhouse.com/cloud/):** The official ClickHouse as a service, - built by, maintained and supported by the creators of ClickHouse
- **[Quick Install](#quick-install):** an easy-to-download binary for testing and developing with ClickHouse
- **[Production Deployments](#available-installation-options):** ClickHouse can run on any Linux, FreeBSD, or macOS with x86-64, modern ARM (ARMv8.2-A up), or PowerPC64LE CPU architecture
- **[Docker Image](https://hub.docker.com/r/clickhouse/clickhouse-server/):** use the official Docker image in Docker Hub

## ClickHouse Cloud

The quickest and easiest way to get up and running with ClickHouse is to create a new service in [ClickHouse Cloud](https://clickhouse.cloud/).

## Quick Install

:::tip
For production installs of a specific release version see the [installation options](#available-installation-options) down below.
:::

On Linux, macOS and FreeBSD:

1. If you are just getting started and want to see what ClickHouse can do, the simplest way to download ClickHouse locally is to run the
   following command. It downloads a single binary for your operating system that can be used to run the ClickHouse server,
   `clickhouse-client`, `clickhouse-local`, ClickHouse Keeper, and other tools:

   ```bash
   curl https://clickhouse.com/ | sh
   ```

2. Run the following command to start [clickhouse-local](../operations/utilities/clickhouse-local.md):

   ```bash
   ./clickhouse
   ```

   `clickhouse-local` allows you to process local and remote files using ClickHouse's powerful SQL and without a need for configuration. Table
   data is stored in a temporary location, meaning that after a restart of `clickhouse-local` previously created tables are no longer
   available.

   As an alternative, you can start the ClickHouse server with this command ...

    ```bash
    ./clickhouse server
    ```

   ... and open a new terminal to connect to the server with `clickhouse-client`:

    ```bash
    ./clickhouse client
    ```

    ```response
    ./clickhouse client
    ClickHouse client version 24.5.1.117 (official build).
    Connecting to localhost:9000 as user default.
    Connected to ClickHouse server version 24.5.1.

    local-host :)
    ```

   Table data is stored in the current directory and still available after a restart of ClickHouse server. If necessary, you can pass
   `-C config.xml` as an additional command line argument to `./clickhouse server` and provide further configuration in a configuration
   file. All available configuration settings are documented [here](../operations/settings/settings.md) and in an [example configuration file
   template](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.xml).

   You are ready to start sending SQL commands to ClickHouse!

:::tip
The [Quick Start](/docs/en/quick-start.mdx) walks through the steps for creating tables and inserting data.
:::

## Production Deployments {#available-installation-options}

For production deployments of ClickHouse, choose from one of the following install options.

### From DEB Packages {#install-from-deb-packages}

It is recommended to use official pre-compiled `deb` packages for Debian or Ubuntu. Run these commands to install packages:

#### Setup the Debian repository
``` bash
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
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
<summary>Old distributions method for installing the deb-packages</summary>

```bash
sudo apt-get install apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you set up a password.
```

</details>

You can replace `stable` with `lts` to use different [release kinds](/knowledgebase/production) based on your needs.

You can also download and install packages manually from [here](https://packages.clickhouse.com/deb/pool/main/c/).

#### Install standalone ClickHouse Keeper

:::tip
In production environment we [strongly recommend](/docs/en/operations/tips.md#L143-L144) running ClickHouse Keeper on dedicated nodes.
In test environments, if you decide to run ClickHouse Server and ClickHouse Keeper on the same server,  you do not need to install ClickHouse Keeper as it is included with ClickHouse server.
This command is only needed on standalone ClickHouse Keeper servers.
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

- `clickhouse-common-static` — Installs ClickHouse compiled binary files.
- `clickhouse-server` — Creates a symbolic link for `clickhouse-server` and installs the default server configuration.
- `clickhouse-client` — Creates a symbolic link for `clickhouse-client` and other client-related tools. and installs client configuration files.
- `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.
- `clickhouse-keeper` - Used to install ClickHouse Keeper on dedicated ClickHouse Keeper nodes.  If you are running ClickHouse Keeper on the same server as ClickHouse server, then you do not need to install this package. Installs ClickHouse Keeper and the default ClickHouse Keeper configuration files.

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

For systems with `zypper` package manager (openSUSE, SLES):

``` bash
sudo zypper addrepo -r https://packages.clickhouse.com/rpm/clickhouse.repo -g
sudo zypper --gpg-auto-import-keys refresh clickhouse-stable
```

Later any `yum install` can be replaced by `zypper install`. To specify a particular version, add `-$VERSION` to the end of the package name, e.g. `clickhouse-client-22.2.2.22`.

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
In production environment we [strongly recommend](/docs/en/operations/tips.md#L143-L144) running ClickHouse Keeper on dedicated nodes.
In test environments, if you decide to run ClickHouse Server and ClickHouse Keeper on the same server,  you do not need to install ClickHouse Keeper as it is included with ClickHouse server.
This command is only needed on standalone ClickHouse Keeper servers.
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

You can replace `stable` with `lts` to use different [release kinds](/knowledgebase/production) based on your needs.

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
LATEST_VERSION=$(curl -s https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/utils/list-versions/version_date.tsv | \
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

For production environments, it’s recommended to use the latest `stable`-version. You can find its number on GitHub page https://github.com/ClickHouse/ClickHouse/tags with postfix `-stable`.

### From Docker Image {#from-docker-image}

To run ClickHouse inside Docker follow the guide on [Docker Hub](https://hub.docker.com/r/clickhouse/clickhouse-server/). Those images use official `deb` packages inside.

## Non-Production Deployments (Advanced)

### Compile From Source {#from-sources}

To manually compile ClickHouse, follow the instructions for [Linux](/docs/en/development/build.md) or [macOS](/docs/en/development/build-osx.md).

You can compile packages and install them or use programs without installing packages.

      Client: <build_directory>/programs/clickhouse-client
      Server: <build_directory>/programs/clickhouse-server

You’ll need to create data and metadata folders manually and `chown` them for the desired user. Their paths can be changed in server config (src/programs/server/config.xml), by default they are:

      /var/lib/clickhouse/data/default/
      /var/lib/clickhouse/metadata/default/

On Gentoo, you can just use `emerge clickhouse` to install ClickHouse from sources.

### Install a CI-generated Binary

ClickHouse's continuous integration (CI) infrastructure produces specialized builds for each commit in the [ClickHouse
repository](https://github.com/clickhouse/clickhouse/), e.g. [sanitized](https://github.com/google/sanitizers) builds, unoptimized (Debug)
builds, cross-compiled builds etc. While such builds are normally only useful during development, they can in certain situations also be
interesting for users.

:::note
Since ClickHouse's CI is evolving over time, the exact steps to download CI-generated builds may vary.
Also, CI may delete too old build artifacts, making them unavailable for download.
:::

For example, to download a aarch64 binary for ClickHouse v23.4, follow these steps:

- Find the GitHub pull request for release v23.4: [Release pull request for branch 23.4](https://github.com/ClickHouse/ClickHouse/pull/49238)
- Click "Commits", then click a commit similar to "Update autogenerated version to 23.4.2.1 and contributors" for the particular version you like to install.
- Click the green check / yellow dot / red cross to open the list of CI checks.
- Click "Details" next to "Builds" in the list, it will open a page similar to [this page](https://s3.amazonaws.com/clickhouse-test-reports/46793/b460eb70bf29b19eadd19a1f959b15d186705394/clickhouse_build_check/report.html)
- Find the rows with compiler = "clang-*-aarch64" - there are multiple rows.
- Download the artifacts for these builds.

To download binaries for very old x86-64 systems without [SSE3](https://en.wikipedia.org/wiki/SSE3) support or old ARM systems without
[ARMv8.1-A](https://en.wikipedia.org/wiki/AArch64#ARMv8.1-A) support, open a [pull
request](https://github.com/ClickHouse/ClickHouse/commits/master) and find CI check "BuilderBinAmd64Compat", respectively
"BuilderBinAarch64V80Compat". Then click "Details", open the "Build" fold, scroll to the end, find message "Notice: Build URLs
https://s3.amazonaws.com/clickhouse/builds/PRs/.../.../binary_aarch64_v80compat/clickhouse". You can then click the link to download the
build.

### macOS-only: Install with Homebrew

To install ClickHouse on macOS using [homebrew](https://brew.sh/), please see the ClickHouse [community homebrew formula](https://formulae.brew.sh/cask/clickhouse).

## Launch {#launch}

To start the server as a daemon, run:

``` bash
$ clickhouse start
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

- The complexity of queries.
- The amount of data that is processed in queries.

To calculate the required volume of RAM, you may estimate the size of temporary data for [GROUP BY](/docs/en/sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](/docs/en/sql-reference/statements/select/distinct.md#select-distinct), [JOIN](/docs/en/sql-reference/statements/select/join.md#select-join) and other operations you use.

To reduce memory consumption, ClickHouse can swap temporary data to external storage. See [GROUP BY in External Memory](/docs/en/sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) for details.

We recommend to disable the operating system's swap file in production environments.

The ClickHouse binary requires at least 2.5 GB of disk space for installation.

The volume of storage required for your data may be calculated separately based on

- an estimation of the data volume.

    You can take a sample of the data and get the average size of a row from it. Then multiply the value by the number of rows you plan to store.

- The data compression coefficient.

    To estimate the data compression coefficient, load a sample of your data into ClickHouse, and compare the actual size of the data with the size of the table stored. For example, clickstream data is usually compressed by 6-10 times.

To calculate the final volume of data to be stored, apply the compression coefficient to the estimated data volume. If you plan to store data in several replicas, then multiply the estimated volume by the number of replicas.

For distributed ClickHouse deployments (clustering), we recommend at least 10G class network connectivity.

Network bandwidth is critical for processing distributed queries with a large amount of intermediate data. Besides, network speed affects replication processes.
