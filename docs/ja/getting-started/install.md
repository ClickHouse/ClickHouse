---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 11
toc_title: "\u8A2D\u7F6E"
---

# 設置 {#installation}

## システム要件 {#system-requirements}

ClickHouseは、x86\_64、AArch64、またはPowerPC64LE CPUアーキテクチャを持つLinux、FreeBSD、またはMac OS X上で実行できます。

公式の事前ビルドされたバイナリは、通常、x86\_64用にコンパイルされ、SSE4.2命令セットを利用するため、特に明記されていない限り、それをサポートす このコマンドをチェックが現在のCPU支援のためのSSE4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

走ClickHouseにプロセッサーをサポートしていないSSE4.2てAArch64はPowerPC64LE建築き [ソースからClickHouseを構築](#from-sources) 適切な構成調節を使って。

## 利用できる設置選択 {#available-installation-options}

### DEBパッケージから {#install-from-deb-packages}

公式の事前コンパイルを使用することをお勧めします `deb` DebianまたはUbuntu用のパッケージ。 走りこれらのコマンド置パッケージ:

``` bash
{% include 'install/deb.sh' %}
```

最新のバージョンを使用する場合は、置き換えます `stable` と `testing` (これはテスト環境に推奨されます)。

でもダウンロードとインストールパッケージ動 [ここに](https://repo.clickhouse.tech/deb/stable/main/).

#### パッケージ {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` とをインストールしデフォルトのサーバーの設定をします。
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` そして他の顧客関連の用具。 および設置お客様の設定ファイルです。
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### RPMパッケージから {#from-rpm-packages}

公式の事前コンパイルを使用することをお勧めします `rpm` CentOS、RedHat、その他すべてのrpmベースのLinuxディストリビューション用のパッケージ。

まず、公式リポジトリを追加する必要があります:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

最新のバージョンを使用する場合は、置き換えます `stable` と `testing` (これはテスト環境に推奨されます)。 その `prestable` タグは時々あまりにも利用可能です。

そこにこれらのコマンド置パッケージ:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

でもダウンロードとインストールパッケージ動 [ここに](https://repo.clickhouse.tech/rpm/stable/x86_64).

### Tgzアーカイブから {#from-tgz-archives}

公式の事前コンパイルを使用することをお勧めします `tgz` のインストール `deb` または `rpm` パッケージはできません。

必要なバージョンは次のとおりです `curl` または `wget` リポジトリからhttps://repo.clickhouse.tech/tgz/.
その後、アーカイブをダウンロードは開梱と設置と設置のためのイントロダクションです。 最新バージョンの例:

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

本番環境では、最新のものを使用することをお勧めします `stable`-バージョン。 き、その番号をGitHubのページhttps://github.com/ClickHouse/ClickHouse/tags とpostfix `-stable`.

### Dockerイメージから {#from-docker-image}

Docker内でClickHouseを実行するには、次のガイドに従います [Dockerハブ](https://hub.docker.com/r/yandex/clickhouse-server/). このように映像公 `deb` 中のパッケージ。

### ソースから {#from-sources}

ClickHouseを手動でコンパイルするには、以下の手順に従います [Linux](../development/build.md) または [Mac OS X](../development/build-osx.md).

できるコンパイルパッケージはインストールしていたプログラムを使用もインストールせずにパッケージ。 またビルを手動で無数の組み合わせで自分だけのSSE4.2に必要構築のためのAArch64定する必要はありません。

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

データフォルダとメタデータフォルダを作成し、 `chown` 目的のユーザーのためのそれら。 それらのパスは、サーバー設定（src/programs/server/config）で変更することができます。xml)、デフォルトでは:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

Gentooでは、以下を使用することができます `emerge clickhouse` ソースからClickHouseをインストールする。

## 起動 {#launch}

サーバを起動デーモンとして、:

``` bash
$ sudo service clickhouse-server start
```

あなたが持っていない場合 `service` コマンドとして実行

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

のログを参照してください `/var/log/clickhouse-server/` ディレクトリ。

サーバーが起動しない場合は、ファイル内の構成を確認してください `/etc/clickhouse-server/config.xml`.

または手動で開始のサーバーからのコンソール:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

この場合、ログはコンソールに印刷され、開発中に便利です。
設定ファイルがカレントディレクトリにある場合は、 `--config-file` パラメータ。 デフォルトでは、 `./config.xml`.

ClickHouse対応アクセス制限を設定します。 彼らはに位置しています `users.xml` ファイル(隣のファイル `config.xml`).
デフォルトでは、 `default` ユーザー、パスワードなし。 見る `user/default/networks`.
詳細については [“Configuration Files”](../operations/configuration-files.md).

Serverを起動した後、コマンドラインクライアントを使用してserverに接続できます:

``` bash
$ clickhouse-client
```

デフォルトでは、 `localhost:9000` ユーザーに代わって `default` パスワードなし。 また、リモートサーバに接続するために使用することもできます `--host` 引数。

端末はUTF-8エンコードを使用する必要があります。
詳細については [“Command-line client”](../interfaces/cli.md).

例:

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

**生、おめでとうございます、システムを作ります！！**

継続実験をダウンロードでき、試験データセットやじ [チュートリ](https://clickhouse.tech/tutorial.html).

[元の記事](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
