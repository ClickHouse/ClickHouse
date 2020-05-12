---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 11
toc_title: "\u8A2D\u7F6E"
---

# 設置 {#installation}

## システム要件 {#system-requirements}

ClickHouseは、x86\_64、AArch64、またはPowerPC64LE CPUアーキテクチャを使用して、Linux、FreeBSD、またはMac OS X上で実行できます。

公式の事前構築されたバイナリは、通常、x86\_64用にコンパイルされ、sse4.2命令セットを利用するため、特に明記されていない限り、それをサポートす 現在のcpuがsse4.2をサポートしているかどう:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

走clickhouseにプロセッサーをサポートしていないsse4.2てaarch64はpowerpc64le建築き [ソースからのClickHouseのビルド](#from-sources) 適切な構成の調節を使って。

## 使用可能な設置方法 {#available-installation-options}

### DEBパッケージから {#install-from-deb-packages}

公式の事前コンパイルを使用することをお勧めします `deb` DebianやUbuntuのためのパッケージ.

そこにこれらのコマンド置パッケージ:

``` bash
{% include 'install/deb.sh' %}
```

でもダウンロードとインストールパッケージを手動で下からもアクセスできます。https://repo.clickhouse.tech/deb/stable/main/.

最新のバージョンを使用する場合は、以下を置き換えます `stable` と `testing` (これはテスト環境に推奨されます)。

#### パッケージ {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` とをインストールしデフォルトのサーバーの設定をします。
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` そして他の顧客関係した用具。 および設置お客様の設定ファイルです。
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### RPMパッケージから {#from-rpm-packages}

公式の事前コンパイルを使用することをお勧めします `rpm` パッケージCentOS,RedHat、その他のrpmベLinuxディストリビューション.

まず、公式リポジトリを追加する必要があります:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

最新のバージョンを使用する場合は、以下を置き換えます `stable` と `testing` (これはテスト環境に推奨されます)。 その `prestable` タグも時々利用可能です。

そこにこれらのコマンド置パッケージ:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

でもダウンロードとインストールパッケージを手動で下からもアクセスできます。https://repo.クリックハウス。テック/rpm/安定した/x86\_64.

### Tgzアーカイブから {#from-tgz-archives}

公式の事前コンパイルを使用することをお勧めします `tgz` すべてのLinuxディストリビュ `deb` または `rpm` パッケージは不可能です。

必要なバージョンは `curl` または `wget` リポジトリからhttps://repo.yandex.ru/clickhouse/tgz/。
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

運用環境では、最新のものを使用することをお勧めします `stable`-バージョン。 その番号はGitHubページにありますhttps://github.com/ClickHouse/ClickHouse/tags 後置を使って `-stable`.

### ドッカーの画像から {#from-docker-image}

Dockerの中でClickHouseを実行するには、次のガイドに従います [Docker拠点](https://hub.docker.com/r/yandex/clickhouse-server/). これらの画像は、公式を使用 `deb` 中のパッケージ。

### ソースから {#from-sources}

ClickHouseを手動でコンパイルするには、以下の手順に従います [Linux](../development/build.md) または [Mac OS X](../development/build-osx.md).

できるコンパイルパッケージはインストールしていたプログラムを使用もインストールせずにパッケージ。 またビルを手動で無数の組み合わせで自分だけのsse4.2に必要構築のためのaarch64定する必要はありません。

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

データとメタデータフォルダを作成する必要があります `chown` 目的のユーザーのためのそれら。 それらのパスは、server config（src/programs/server/config）で変更できます。xml)、デフォルトでは次のとおりです:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

Gentooでは、次のものを使用できます `emerge clickhouse` ソースからClickHouseをインストールする。

## 起動 {#launch}

サーバを起動デーモンとして、:

``` bash
$ sudo service clickhouse-server start
```

あなたが持っていない場合 `service` コマンド、実行

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

のログを参照してください `/var/log/clickhouse-server/` ディレクトリ。

サーバーが起動しない場合は、ファイル内の設定を確認してください `/etc/clickhouse-server/config.xml`.

または手動で開始のサーバーからのコンソール:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

この場合、ログはコンソールに印刷され、開発中に便利です。
設定ファイルがカレントディレクトリにある場合は、以下を指定する必要はありません。 `--config-file` パラメータ。 デフォルトでは、 `./config.xml`.

ClickHouse対応アクセス制限を設定します。 彼らはに位置しています `users.xml` ファイル(次へ `config.xml`).
デフォルトでは、アクセスはどこからでも可能です。 `default` ユーザー、パスワードなし。 見る `user/default/networks`.
詳細については、以下を参照してください [“Configuration Files”](../operations/configuration-files.md).

サーバーの起動後、コマンドラインクライアントを使用してサーバーに接続できます:

``` bash
$ clickhouse-client
```

デフォルトでは、 `localhost:9000` ユーザーに代わって `default` パスワードなし。 また、以下を使用してリモートサーバーに接続することもできます `--host` 引数。

端末はutf-8エンコードを使用する必要があります。
詳細については、以下を参照してください [“Command-line client”](../interfaces/cli.md).

例えば:

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

**おめでとう、システムの作品！**

継続実験をダウンロードでき、試験データセットやじ [tutorial](https://clickhouse.tech/tutorial.html).

[元の記事](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
