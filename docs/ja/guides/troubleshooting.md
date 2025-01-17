---
title: "トラブルシューティング"
---

## インストール

### apt-keyでkeyserver.ubuntu.comからGPGキーをインポートできない

[Advanced package tool (APT)の`apt-key`機能は廃止されています](https://manpages.debian.org/bookworm/apt/apt-key.8.en.html)。代わりに`gpg`コマンドを使用する必要があります。[インストールガイド](../getting-started/install.md)の記事を参照してください。

### gpgでkeyserver.ubuntu.comからGPGキーをインポートできない

1. `gpg`がインストールされているか確認します:

```shell
sudo apt-get install gnupg
```

### apt-getでClickHouseリポジトリからdebパッケージを取得できない

1. ファイアウォールの設定を確認します。
2. 何らかの理由でリポジトリにアクセスできない場合は、[インストールガイド](../getting-started/install.md)の記事で説明されているようにパッケージをダウンロードし、`sudo dpkg -i <packages>`コマンドを使って手動でインストールします。また、`tzdata`パッケージも必要です。

### apt-getでClickHouseリポジトリからdebパッケージを更新できない

この問題はGPGキーが変更されたときに発生する可能性があります。

リポジトリ設定を更新するには、[設定](../getting-started/install.md#setup-the-debian-repository)ページのマニュアルを使用してください。

### `apt-get update`で異なる警告が出る

警告メッセージの例は次のいずれかです:

```shell
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```shell
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```shell
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```shell
Err:11 https://packages.clickhouse.com/deb stable InRelease
400  Bad Request [IP: 172.66.40.249 443]
```

上記の問題を解決するには、以下のスクリプトを使用してください:

```shell
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

### 不正な署名のためにYumでパッケージを取得できない

可能性のある問題: キャッシュが間違っている、または2022-09に更新されたGPGキー後に破損した可能性があります。

解決方法は、Yumのキャッシュとlibディレクトリをクリアすることです:

```shell
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

その後、[インストールガイド](../getting-started/install.md#from-rpm-packages)に従ってください。

## サーバーへの接続

可能性のある問題:

- サーバーが稼働していない。
- 予期せぬ、または間違った構成パラメータ。

### サーバーが稼働していない

#### サーバーが稼働しているか確認

```shell
sudo service clickhouse-server status
```

サーバーが稼働していない場合は、次のコマンドで開始してください:

```shell
sudo service clickhouse-server start
```

#### ログを確認

`clickhouse-server`のメインログはデフォルトで`/var/log/clickhouse-server/clickhouse-server.log`にあります。

サーバーが正常に開始した場合、次の文字列が表示されます:

- `<Information> Application: starting up.` — サーバーが開始されました。
- `<Information> Application: Ready for connections.` — サーバーが稼働中で接続可能です。

構成エラーで`clickhouse-server`の開始が失敗した場合、`<Error>`文字列とエラーの説明が表示されます。例:

```plaintext
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

ファイルの末尾にエラーが表示されない場合は、次の文字列からファイル全体を確認してください:

```plaintext
<Information> Application: starting up.
```

サーバーに`clickhouse-server`の2番目のインスタンスを開始しようとすると、次のログが表示されます:

```plaintext
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

#### system.dログを確認

`clickhouse-server`ログに有用な情報が見つからない場合、またはログがない場合は、次のコマンドを使用して`system.d`ログを確認できます:

```shell
sudo journalctl -u clickhouse-server
```

#### インタラクティブモードでclickhouse-serverを起動

```shell
sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

このコマンドは、autostartスクリプトの標準パラメータを使用してサーバーをインタラクティブアプリとして起動します。このモードでは、`clickhouse-server`はすべてのイベントメッセージをコンソールに表示します。

### 構成パラメータ

確認事項:

1. Dockerの設定:

    - DockerでClickHouseを実行する際にIPv6ネットワークを使用する場合、`network=host`が設定されていることを確認してください。

1. エンドポイントの設定。
    - [listen_host](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-listen_host)と[tcp_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port)の設定を確認してください。
    - ClickHouseサーバーはデフォルトでlocalhost接続のみ受け入れます。

1. HTTPプロトコルの設定:

    - HTTP APIのプロトコル設定を確認します。

1. セキュア接続の設定。

    - 次を確認してください:
        - [tcp_port_secure](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure)の設定。
        - [SSL証明書](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-openssl)の設定。
    - 接続する際には適切なパラメータを使用してください。例えば、`clickhouse_client`には`port_secure`パラメータを使用します。

1. ユーザー設定:

    - 間違ったユーザー名またはパスワードを使用している可能性があります。

## クエリ処理

ClickHouseがクエリを処理できない場合、クライアントにエラーの説明を送信します。`clickhouse-client`では、コンソールにエラーの説明が表示されます。HTTPインターフェースを使用している場合、ClickHouseは応答ボディにエラーの説明を送信します。例えば:

```shell
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

`stack-trace`パラメータで`clickhouse-client`を起動すると、エラーの説明と共にサーバーのスタックトレースが返されます。

接続が切断されたというメッセージを見ることがあります。この場合、クエリを再実行することができます。クエリを実行するたびに接続が切断される場合は、サーバーログでエラーを確認してください。

## クエリ処理の効率

ClickHouseが非常に遅く動作している場合、クエリに対するサーバーのリソースとネットワーク負荷をプロファイルする必要があります。

clickhouse-benchmarkユーティリティを使用してクエリをプロファイルできます。これは、1秒あたりに処理されるクエリの数、1秒あたりに処理される行の数、およびクエリ処理時間のパーセンタイルを表示します。
