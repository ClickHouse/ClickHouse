---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u30C8\u30E9\u30D6\u30EB\u30B7"
---

# トラブルシ {#troubleshooting}

-   [設置](#troubleshooting-installation-errors)
-   [サーバーへの接続](#troubleshooting-accepts-no-connections)
-   [クエリ処理](#troubleshooting-does-not-process-queries)
-   [クエリ処理の効率](#troubleshooting-too-slow)

## 設置 {#troubleshooting-installation-errors}

### Apt-getではClickHouseリポジトリからDebパッケージを取得できません {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   ファイアウォールの設定
-   できない場合はアクセスリポジトリのために、何らかの理由でダウンロードパッケージに記載のとおり [はじめに](../getting-started/index.md) 記事とを使用して手動でインストール `sudo dpkg -i <packages>` コマンド また、必要になります `tzdata` パッケージ。

## サーバーへの接続 {#troubleshooting-accepts-no-connections}

考えられる問題:

-   サーバーが実行されていません。
-   想定外または誤った設定パラメータ。

### サーバの実行中に {#server-is-not-running}

**サーバーがrunnnigかどうかを確認する**

コマンド:

``` bash
$ sudo service clickhouse-server status
```

サーバーが実行されていない場合は、次のコマンドで起動します:

``` bash
$ sudo service clickhouse-server start
```

**ログの確認**

のメインログ `clickhouse-server` である `/var/log/clickhouse-server/clickhouse-server.log` デフォルトでは。

サーバーが正常に起動した場合は、文字列が表示されます:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

もし `clickhouse-server` 設定エラーで起動に失敗しました。 `<Error>` エラーの説明を含む文字列。 例えば:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

ファイルの最後にエラーが表示されない場合は、文字列から始まるファイル全体を調べます:

``` text
<Information> Application: starting up.
```

あなたが第二のインスタンスを起動しようとすると `clickhouse-server` サーバーには、次のログが表示されます:

``` text
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

**システムを参照。dログ**

で有用な情報が見つからない場合 `clickhouse-server` ログまたはログがない場合は、表示できます `system.d` コマンドを使用したログ:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Clickhouse-serverを対話モードで起動する**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

このコマ このモードでは `clickhouse-server` 版画のすべてのイベントメッセージです。

### 構成パラメータ {#configuration-parameters}

チェック:

-   ドッカーの設定。

    DockerでClickhouseをIPv6ネットワークで実行する場合は、次のことを確認してください `network=host` 設定されています。

-   エンドポイント設定。

    チェック [listen\_host](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) と [tcp\_port](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) 設定。

    ClickHouse serverはデフォルトでのみlocalhost接続を受け入れます。

-   HTTPプロトコル設定。

    HTTP APIのプロトコル設定を確認します。

-   安全な接続設定。

    チェック:

    -   その [tcp\_port\_secure](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) 設定。
    -   の設定 [SSLセルティクス](server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    適切なパラメータを接続 たとえば、 `port_secure` 変数との `clickhouse_client`.

-   ユーザー設定。

    きの違うユーザー名やパスワードになります。

## クエリ処理 {#troubleshooting-does-not-process-queries}

ClickHouseは、クエリを処理できない場合は、クライアントにエラーの説明を送信します。 で `clickhouse-client` コンソールにエラーの説明が表示されます。 HTTPインターフェイスを使用している場合、ClickHouseは応答本文にエラーの説明を送信します。 例えば:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

あなたが開始した場合 `clickhouse-client` と `stack-trace` パラメータClickHouseは、エラーの説明を含むサーバースタックトレースを返します。

あるいは、メッセージが壊れて接続します。 この場合、クエリを繰り返すことができます。 クエリを実行するたびに接続が切断された場合は、サーバーログでエラーを確認します。

## クエリ処理の効率 {#troubleshooting-too-slow}

ClickHouseの動作が遅すぎる場合は、クエリのサーバーリソースとネットワークの負荷をプロファイルする必要があります。

Clickhouse-benchmarkユーティリテ 毎秒処理されたクエリの数、毎秒処理された行数、およびクエリ処理時間の百分位数が表示されます。
