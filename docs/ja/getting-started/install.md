---
sidebar_label: インストール
keywords: [clickhouse, install, getting started, quick start]
description: ClickHouseのインストール
slug: /ja/install
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

# ClickHouseをインストール

ClickHouseをすぐに利用し始めるためには、以下の4つのオプションがあります：

- **[ClickHouse Cloud](https://clickhouse.com/cloud/):** ClickHouseの開発者によって構築、維持、サポートされている公式のClickHouse as a Service
- **[クイックインストール](#quick-install):** ClickHouseのテストや開発のための簡単にダウンロード可能なバイナリ
- **[プロダクション向けデプロイメント](#available-installation-options):** ClickHouseは、x86-64、最新のARM (ARMv8.2-A以降)、またはPowerPC64LE CPUアーキテクチャを持つ任意のLinux、FreeBSD、またはmacOSで動作可能
- **[Dockerイメージ](https://hub.docker.com/r/clickhouse/clickhouse-server/):** Docker Hubにある公式のDockerイメージを使用

## ClickHouse Cloud

ClickHouseを迅速かつ簡単に利用し始める方法は、[ClickHouse Cloud](https://clickhouse.cloud/)で新しいサービスを作成することです。

## クイックインストール

:::tip
特定のリリースバージョンのプロダクションインストールについては、以下の[インストールオプション](#available-installation-options)をご覧ください。
:::

Linux、macOS、FreeBSDでは：

1. 始めたばかりでClickHouseの機能を確認したい場合、最も簡単な方法は次のコマンドを実行してClickHouseをローカルにダウンロードすることです。ご利用のオペレーティングシステム用の単一バイナリがダウンロードされ、それを使ってClickHouseサーバーや`clickhouse-client`、`clickhouse-local`、ClickHouse Keeper、その他のツールを実行できます：

   ```bash
   curl https://clickhouse.com/ | sh
   ```

2. [clickhouse-local](../operations/utilities/clickhouse-local.md)を起動するために次のコマンドを実行します：

   ```bash
   ./clickhouse
   ```

   `clickhouse-local`は、ローカルおよびリモートファイルをClickHouseの強力なSQLを使って処理することを可能にし、設定が不要です。テーブルデータは一時的な場所に保存され、`clickhouse-local`を再起動すると前に作成されたテーブルは利用できなくなります。

   代わりに次のコマンドでClickHouseサーバーを開始することもできます...

    ```bash
    ./clickhouse server
    ```

   ...そして、`clickhouse-client`でサーバに接続するために新しいターミナルを開きます：

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

   テーブルデータは現在のディレクトリに保存され、ClickHouseサーバーを再起動した後も利用可能です。必要に応じて、`./clickhouse server`に追加のコマンドライン引数として`-C config.xml`を渡し、設定ファイルで追加の設定を提供することができます。利用可能なすべての設定は[ここ](../operations/settings/settings.md)および[設定ファイルテンプレートの例](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.xml)に文書化されています。

   ClickHouseにSQLコマンドを送信する準備が整いました！

:::tip
[Quick Start](/docs/ja/quick-start.mdx)で、テーブルの作成とデータの挿入の手順を詳しく説明しています。
:::

## プロダクション向けデプロイメント {#available-installation-options}

ClickHouseのプロダクション向けデプロイメントには、以下のインストールオプションから選択してください。

### DEBパッケージからのインストール {#install-from-deb-packages}

DebianまたはUbuntuでは公式の事前コンパイル済みの`deb`パッケージを使用することをお勧めします。パッケージをインストールするために次のコマンドを実行してください：

#### Debianリポジトリの設定
```bash
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
```

#### ClickHouseサーバーとクライアントのインストール
```bash
sudo apt-get install -y clickhouse-server clickhouse-client
```

#### ClickHouseサーバーの起動

```bash
sudo service clickhouse-server start
clickhouse-client # または、パスワードを設定している場合は "clickhouse-client --password"
```

<details>
<summary>古いディストリビューションのdebパッケージインストール法</summary>

```bash
sudo apt-get install apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
clickhouse-client # または、パスワードを設定している場合は "clickhouse-client --password"
```

</details>

必要に応じて、`stable`を`lts`に置き換えて、ニーズに基づいた異なる[リリース種別](/knowledgebase/production)を使用することができます。

また、[こちら](https://packages.clickhouse.com/deb/pool/main/c/)からパッケージを手動でダウンロードしてインストールすることも可能です。

#### スタンドアロンClickHouse Keeperのインストール

:::tip
プロダクション環境では、ClickHouse Keeperを専用のノード上で実行することを[強く推奨します](/docs/ja/operations/tips.md#L143-L144)。テスト環境でClickHouse ServerとClickHouse Keeperを同じサーバーで実行することを決定した場合は、ClickHouse ServerにClickHouse Keeperが含まれているので、ClickHouse Keeperをインストールする必要はありません。このコマンドは、スタンドアロンのClickHouse Keeperサーバーにのみ必要です。
:::

```bash
sudo apt-get install -y clickhouse-keeper
```

#### ClickHouse Keeperの有効化と開始

```bash
sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper
```

#### パッケージ {#packages}

- `clickhouse-common-static` — ClickHouseのコンパイル済みバイナリファイルをインストールします。
- `clickhouse-server` — `clickhouse-server`のシンボリックリンクを作成し、デフォルトのサーバー設定をインストールします。
- `clickhouse-client` — `clickhouse-client`および他のクライアント関連ツールのシンボリックリンクを作成し、クライアント設定ファイルをインストールします。
- `clickhouse-common-static-dbg` — デバッグ情報付きのClickHouseコンパイル済みバイナリファイルをインストールします。
- `clickhouse-keeper` - 専用のClickHouse KeeperノードにClickHouse Keeperをインストールするために使用されます。ClickHouseサーバーと同じサーバーでClickHouse Keeperを実行している場合、このパッケージをインストールする必要はありません。ClickHouse KeeperとデフォルトのClickHouse Keeper設定ファイルをインストールします。

:::info
特定のバージョンのClickHouseをインストールする必要がある場合は、すべてのパッケージを同じバージョンでインストールする必要があります： `sudo apt-get install clickhouse-server=21.8.5.7 clickhouse-client=21.8.5.7 clickhouse-common-static=21.8.5.7`
:::

### RPMパッケージからのインストール {#from-rpm-packages}

CentOS、RedHat、およびすべてのrpmベースのLinuxディストリービューションでは、公式の事前コンパイル済み`rpm`パッケージを使用することをお勧めします。

#### RPMリポジトリの設定
まず、公式リポジトリを追加する必要があります：

```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
```

`zypper`パッケージマネージャーを使用するシステム（openSUSE, SLES）の場合：

```bash
sudo zypper addrepo -r https://packages.clickhouse.com/rpm/clickhouse.repo -g
sudo zypper --gpg-auto-import-keys refresh clickhouse-stable
```

その後、任意の`yum install`を`zypper install`に置き換えることができます。特定のバージョンを指定するには、パッケージ名の末尾に`-$VERSION`を追加します。例：`clickhouse-client-22.2.2.22`。

#### ClickHouseサーバーとクライアントのインストール

```bash
sudo yum install -y clickhouse-server clickhouse-client
```

#### ClickHouseサーバーの起動

```bash
sudo systemctl enable clickhouse-server
sudo systemctl start clickhouse-server
sudo systemctl status clickhouse-server
clickhouse-client # または "clickhouse-client --password"、パスワードを設定している場合
```

#### スタンドアロンClickHouse Keeperのインストール

:::tip
プロダクション環境では、ClickHouse Keeperを専用のノード上で実行することを[強く推奨します](/docs/ja/operations/tips.md#L143-L144)。テスト環境でClickHouse ServerとClickHouse Keeperを同じサーバーで実行することを決定した場合は、ClickHouse ServerにClickHouse Keeperが含まれているので、ClickHouse Keeperをインストールする必要はありません。このコマンドは、スタンドアロンのClickHouse Keeperサーバーにのみ必要です。
:::

```bash
sudo yum install -y clickhouse-keeper
```

#### ClickHouse Keeperの有効化と開始

```bash
sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper
```

必要に応じて、`stable`を`lts`に置き換えて、ニーズに基づいて異なる[リリース種別](/knowledgebase/production)を使用することができます。

その後、これらのコマンドを実行してパッケージをインストールします：

```bash
sudo yum install clickhouse-server clickhouse-client
```

また、[こちら](https://packages.clickhouse.com/rpm/stable)からパッケージを手動でダウンロードしてインストールすることも可能です。

### Tgzアーカイブからのインストール {#from-tgz-archives}

`deb`や`rpm`パッケージのインストールが不可能なすべてのLinuxディストリービューションでは、公式の事前コンパイル済みの`tgz`アーカイブを使用することをお勧めします。

必要なバージョンは、リポジトリhttps://packages.clickhouse.com/tgz/ から`curl`または`wget`を使用してダウンロードできます。その後、ダウンロードされたアーカイブを解凍し、インストールスクリプトでインストールします。最新の安定版の例：

```bash
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

プロダクション環境では、最新の`stable`-バージョンを使用することをお勧めします。バージョン番号はGitHubのページhttps://github.com/ClickHouse/ClickHouse/tags の`-stable`の接尾辞で確認できます。

### Dockerイメージからのインストール {#from-docker-image}

Docker内でClickHouseを実行するには、[Docker Hub](https://hub.docker.com/r/clickhouse/clickhouse-server/)のガイドに従ってください。これらのイメージは内部で公式の`deb`パッケージを使用します。

## 非プロダクションデプロイメント（上級）

### ソースからのコンパイル {#from-sources}

ClickHouseを手動でコンパイルするには、[Linux](/docs/ja/development/build.md)または[macOS](/docs/ja/development/build-osx.md)の指示に従ってください。

パッケージをコンパイルしてインストールするか、インストールせずにプログラムを使用することができます。

      クライアント: <build_directory>/programs/clickhouse-client
      サーバー: <build_directory>/programs/clickhouse-server

データおよびメタデータフォルダを手動で作成し、希望のユーザーに対して`chown`する必要があります。これらのパスはサーバーの設定ファイル（src/programs/server/config.xml）で変更でき、デフォルトでは以下のようになっています：

      /var/lib/clickhouse/data/default/
      /var/lib/clickhouse/metadata/default/

Gentooでは、単に`emerge clickhouse`コマンドを使用してソースからClickHouseをインストールできます。

### CIで生成されたバイナリのインストール

ClickHouseの継続的インテグレーション（CI）インフラストラクチャは、[ClickHouseのリポジトリ](https://github.com/clickhouse/clickhouse/)の各コミットのために特別にビルドされたビルドを生成します。例えば、[サニタイズ](https://github.com/google/sanitizers)されたビルド、最適化されていない（デバッグ）ビルド、クロスコンパイルされたビルドなどです。これらのビルドは通常、開発中のみ役立ちますが、特定の状況ではユーザーにとっても興味深い場合があります。

:::note
ClickHouseのCIは時間とともに進化するため、CIで生成されたビルドをダウンロードするための正確な手順は変わるかもしれません。
また、CIは古いビルドアーティファクトを削除することがあり、それによりそれらのダウンロードができなくなることがあります。
:::

例えば、ClickHouse v23.4のaarch64バイナリをダウンロードするには、以下の手順に従います：

- リリースv23.4のGitHubプルリクエストを見つけます：[Release pull request for branch 23.4](https://github.com/ClickHouse/ClickHouse/pull/49238)
- "Commits"をクリックし、特定のインストールしたいバージョンに対応する"Update autogenerated version to 23.4.2.1 and contributors"のようなコミットをクリックします。
- CIチェックのリストを開くため、緑のチェック / 黄色のドット / 赤いバツをクリックします。
- リスト内の"Builds"の横にある"Details"をクリックし、[このページ](https://s3.amazonaws.com/clickhouse-test-reports/46793/b460eb70bf29b19eadd19a1f959b15d186705394/clickhouse_build_check/report.html)のようなページを開きます。
- コンパイラ＝"clang-*-aarch64"の行を見つけます - 複数の行があります。
- これらのビルドのアーティファクトをダウンロードします。

[SSE3](https://ja.wikipedia.org/wiki/SSE3)サポートなしの非常に古いx86-64システムまたは[ARMv8.1-A](https://ja.wikipedia.org/wiki/AArch64#ARMv8.1-A)サポートなしの古いARMシステムのためのバイナリをダウンロードするには、プルリクエストを開いてCIチェック"BuilderBinAmd64Compat"、それぞれ"BuilderBinAarch64V80Compat"を見つけます。その後、"Details"をクリックしてビルドフォルダを開き、最後までスクロールし、メッセージ"Notice: Build URLs https://s3.amazonaws.com/clickhouse/builds/PRs/.../.../binary_aarch64_v80compat/clickhouse" を見つけます。そのリンクをクリックしてビルドをダウンロードできます。

### macOSのみ：Homebrewでのインストール

macOSで[homebrew](https://brew.sh/)を使用してClickHouseをインストールするには、ClickHouse [community homebrew formula](https://formulae.brew.sh/cask/clickhouse)を参照してください。

## 起動 {#launch}

サーバーをデーモンとして開始するには、次のコマンドを実行します：

```bash
$ clickhouse start
```

ClickHouseを実行する他の方法もあります：

```bash
$ sudo service clickhouse-server start
```

`service`コマンドがない場合は、このように実行します：

```bash
$ sudo /etc/init.d/clickhouse-server start
```

`systemctl`コマンドがある場合は、このように実行します：

```bash
$ sudo systemctl start clickhouse-server.service
```

`/var/log/clickhouse-server/`ディレクトリでログを参照します。

サーバーが起動しない場合は、`/etc/clickhouse-server/config.xml`ファイル内の設定を確認してください。

サーバーをコンソールから手動で起動することもできます：

```bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

この場合、ログはコンソールに出力されるため、開発中には便利です。設定ファイルが現在のディレクトリにある場合、`--config-file`パラメータを指定する必要はありません。デフォルトで`./config.xml`が使用されます。

ClickHouseはアクセス制限の設定をサポートしています。これらは`users.xml`ファイル（`config.xml`の横）に含まれています。
デフォルトでは、パスワードなしで`default`ユーザーに対するどこからでものアクセスが許可されています。`user/default/networks`を参照してください。
詳細については、[「設定ファイル」](/docs/ja/operations/configuration-files.md)のセクションを参照してください。

サーバーを起動した後、コマンドラインクライアントを使用して接続できます：

```bash
$ clickhouse-client
```

デフォルトでは、`localhost:9000`に`default`ユーザーで、パスワードなしで接続します。`--host`引数を使用してリモートサーバーに接続することもできます。

ターミナルはUTF-8エンコーディングを使用する必要があります。
詳細については、[「コマンドラインクライアント」](/docs/ja/interfaces/cli.md)のセクションを参照してください。

例：

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

**おめでとうございます、システムは正常に動作しています！**

実験を続けるには、テストデータセットの一つをダウンロードするか、[チュートリアル](/docs/ja/tutorial.md)を進めてください。

## セルフマネージドClickHouseに対する推奨事項

ClickHouseは、x86-64、ARM、またはPowerPC64LE CPUアーキテクチャを持つ任意のLinux、FreeBSD、またはmacOSで実行可能です。

ClickHouseは利用可能なハードウェアリソースを全て使用してデータを処理します。

ClickHouseは、少ないコア数で高いクロックレートよりも、低いクロックレートで多くのコア数を持つ方がより効率的に動作する傾向があります。

非トリビアルなクエリを実行するには、最低でも4GBのRAMを使用することをお勧めします。ClickHouseサーバーははるかに少ないRAMでも実行可能ですが、その場合、クエリはよく中断されます。

必要なRAMの容量は通常次の要因に依存します：

- クエリの複雑さ
- クエリで処理されるデータ量

必要なRAMの容量を計算するには、[GROUP BY](/docs/ja/sql-reference/statements/select/group-by.md#select-group-by-clause)、[DISTINCT](/docs/ja/sql-reference/statements/select/distinct.md#select-distinct)、[JOIN](/docs/ja/sql-reference/statements/select/join.md#select-join)など、使用している操作の一時データのサイズを見積もります。

メモリ消費を削減するために、ClickHouseは一時データを外部ストレージにスワップすることができます。詳細については、[外部メモリのGROUP BY](/docs/ja/sql-reference/statements/select/group-by.md#select-group-by-in-external-memory)を参照してください。

プロダクション環境では、オペレーティングシステムのスワップファイルを無効にすることをお勧めします。

ClickHouseバイナリのインストールには少なくとも2.5GBのディスクスペースが必要です。

データの保存に必要なストレージ容量は以下に基づいて別途計算できます

- データ容量の見積もり

    データのサンプルを取得し、その平均行サイズを計算します。そして、保存する予定の行数をかけます。

- データ圧縮係数

    データ圧縮係数を見積もるには、ClickHouseにデータのサンプルをロードし、実際のデータサイズを保存されたテーブルサイズと比較します。例えば、クリックストリームデータは通常6-10倍に圧縮されます。

データを保存するために必要な最終ボリュームを計算するには、圧縮係数をデータ容量の見積もりに適用します。複数のレプリカにデータを保存する計画がある場合は、その見積もり体積をレプリカ数で掛けます。

分散ClickHouseデプロイメント（クラスタリング）では、少なくとも10Gクラスのネットワーク接続を推奨します。

ネットワーク帯域幅は、大量の中間データを持つ分散クエリの処理にとって重要です。また、ネットワーク速度はレプリケーションプロセスにも影響を与えます。
