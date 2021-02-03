---
toc_priority: 11
toc_title: "インストール"
---

# インストール {#installation}

## システム要件 {#system-requirements}

ClickHouseは、x86_64、AArch64、またはPowerPC64LE CPUアーキテクチャを持つLinux、FreeBSD、またはMac OS X上で実行できます。

公式のプレビルドバイナリは通常、x86_64用にコンパイルされており、SSE 4.2命令セットを利用しています。現在のCPUがSSE 4.2をサポートしているかどうかを確認するコマンドは以下の通りです:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

SSEをサポートしていないプロセッサ上でClickHouseを実行するには SSE 4.2 がサポートされているか、AArch64またはPowerPC64LEアーキテクチャで上で、適切な設定と調整を行い、[ソースからClickHouseをビルド](#from-sources)する必要があります。

## 利用可能なインストールオプション {#available-installation-options}

### DEBパッケージから {#install-from-deb-packages}

Debian や Ubuntu 用にコンパイル済みの公式パッケージ `deb` を使用することをお勧めします。以下のコマンドを実行してパッケージをインストールして下さい:


``` bash
{% include 'install/deb.sh' %}
```

最新版を使いたい場合は、`stable`を`testing`に置き換えてください。（テスト環境ではこれを推奨します）

同様に、[こちら](https://repo.clickhouse.tech/deb/stable/main/)からパッケージをダウンロードして、手動でインストールすることもできます。

#### パッケージ {#packages}

-   `clickhouse-common-static`  — コンパイルされた ClickHouse のバイナリファイルをインストールします。
-   `clickhouse-server`  — `clickhouse-server` へのシンボリックリンクを作成し、デフォルトのサーバ設定をインストールします。
-   `clickhouse-client` — `clickhouse-server` および他のclient関連のツールへのシンボリックリンクを作成し、client関連の設定ファイルをインストールします。
-   `clickhouse-common-static-dbg` — コンパイルされた ClickHouse のバイナリファイルを、デバッグ情報と一緒にインストールします。

### RPMパッケージから {#from-rpm-packages}

CentOS、RedHat、その他すべてのrpmベースのLinuxディストリビューションでは、公式のコンパイル済み `rpm` パッケージを使用することを推奨します。

まず、公式リポジトリを追加する必要があります:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

最新版を使いたい場合は `stable` を `testing` に置き換えてください。(テスト環境ではこれが推奨されています)。`prestable` もしばしば同様に利用できます。

そして、以下のコマンドを実行してパッケージをインストールします:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

同様に、[こちら](https://repo.clickhouse.tech/rpm/stable/x86_64) からパッケージをダウンロードして、手動でインストールすることもできます。

### Tgzアーカイブから {#from-tgz-archives}

すべての Linux ディストリビューションで、`deb` や `rpm` パッケージがインストールできない場合は、公式のコンパイル済み `tgz` アーカイブを使用することをお勧めします。

必要なバージョンは、リポジトリ https://repo.clickhouse.tech/tgz/ から `curl` または `wget` でダウンロードできます。その後、ダウンロードしたアーカイブを解凍し、インストールスクリプトでインストールしてください。最新版の例は以下です:

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

本番環境では、最新の `stable` バージョンを使うことをお勧めします。GitHub のページ https://github.com/ClickHouse/ClickHouse/tags で 接尾辞 `-stable` となっているバージョン番号として確認できます。

### Dockerイメージから {#from-docker-image}

Docker内でClickHouseを実行するには、次の [DockerHub](https://hub.docker.com/r/yandex/clickhouse-server/) のガイドに従います。それらのイメージでは内部で公式の `deb` パッケージを使っています。

### 非標準環境向けの事前コンパイルされたバイナリから {#from-binaries-non-linux}

非LinuxオペレーティングシステムとAArch64 CPUアーキテクチャのために、ClickHouseのビルドは `master` ブランチの最新のコミットからクロスコンパイルされたバイナリを提供しています。(数時間の遅延があります)


-   [macOS](https://builds.clickhouse.tech/master/macos/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/macos/clickhouse' && chmod a+x ./clickhouse`
-   [FreeBSD](https://builds.clickhouse.tech/master/freebsd/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/freebsd/clickhouse' && chmod a+x ./clickhouse`
-   [AArch64](https://builds.clickhouse.tech/master/aarch64/clickhouse) — `curl -O 'https://builds.clickhouse.tech/master/aarch64/clickhouse' && chmod a+x ./clickhouse`

ダウンロード後、`clickhouse client` を使ってサーバーに接続したり、`clickhouse local` を使ってローカルデータを処理したりすることができます。`clickhouse server` を実行するには、GitHubから[server](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.xml)と[users](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/users.xml)の設定ファイルを追加でダウンロードする必要があります。

これらのビルドは十分ににテストされていないため、本番環境での使用は推奨されていませんが、自己責任で行うことができます。これらでは、ClickHouseの機能のサブセットのみが利用可能です。

### ソースから {#from-sources}

ClickHouseを手動でコンパイルするには，次の [Linux](../development/build.md) または [Mac OS X](../development/build-osx.md) の指示に従ってください．

パッケージをコンパイルしてインストールすることもできますし、パッケージをインストールせずにプログラムを使用することもできます。また、手動でビルドすることで、SSE 4.2 の要件を無効にしたり、AArch64 CPU 用にビルドしたりすることもできます。

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

ユーザのために、データとメタデータのフォルダを作成して `chown` する必要があります。それらのパスはサーバ設定 (src/programs/server/config.xml) で変更することができます。デフォルトは以下です:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

Gentooでは, ソースからClickHouseをインストールするために `emerge clickhouse` を使うことができます。

## 起動 {#launch}

サーバをデーモンとして起動するには:

``` bash
$ sudo service clickhouse-server start
```

`service` コマンドがない場合は以下のように実行します:

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

`/var/log/clickhouse-server/` ディレクトリのログを参照してください。

サーバが起動しない場合は、`/etc/clickhouse-server/config.xml` ファイル内の設定を確認してください。

同様に、コンソールから以下のように手動で起動することができます:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

この場合、コンソールに開発時に便利なログが出力されます。設定ファイルがカレントディレクトリにある場合は、`--config-file` パラメータを指定する必要はありません。デフォルトでは `./config.xml` を使用します。


ClickHouseはアクセス制限の設定をサポートしています．それらは `users.xml` ファイル（ `config.xml` の隣）にあります。デフォルトでは、`default` ユーザは、パスワードなしでどこからでもアクセスが許可されます。`user/default/networks` を参照し、詳細について、[「設定ファイル」](../operations/configuration-files.md)の項を参照してください。


サーバを起動した後、コマンドラインクライアントを使用してサーバに接続することができます:

``` bash
$ clickhouse-client
```

デフォルトでは、ユーザ `default` で `localhost:9000` にパスワードなしで接続します。また、`--host` 引数を使ってリモートサーバに接続することもできます。

端末はUTF-8エンコーディングを使用する必要があります。詳細については、[コマンドラインクライアント」](../interfaces/cli.md)を参照してください。

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

**おめでとうございます！システムが動きました！**

動作確認を続けるには、テストデータセットをダウンロードするか、[チュートリアル](https://clickhouse.tech/tutorial.html)を参照してください。

[元の記事](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
