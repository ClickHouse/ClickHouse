[//]: # (このファイルはFAQ > トラブルシューティングに含まれます)

- [インストール](#troubleshooting-installation-errors)
- [サーバーへの接続](#troubleshooting-accepts-no-connections)
- [クエリ処理](#troubleshooting-does-not-process-queries)
- [クエリ処理の効率](#troubleshooting-too-slow)

## インストール {#troubleshooting-installation-errors}

### Apt-getでClickHouseリポジトリからDebパッケージを取得できない {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

- ファイアウォールの設定を確認してください。
- 何らかの理由でリポジトリにアクセスできない場合は、[インストールガイド](../getting-started/install.md)の記事で説明されているようにパッケージをダウンロードし、`sudo dpkg -i <packages>`コマンドを使って手動でインストールしてください。`tzdata`パッケージも必要です。

### Apt-getでClickHouseリポジトリからDebパッケージを更新できない {#you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get}

- GPGキーが変更された場合に問題が発生することがあります。

リポジトリ設定を更新するには、[セットアップ](../getting-started/install.md#setup-the-debian-repository)ページの手順に従ってください。

### `apt-get update`で異なる警告が表示される {#you-get-different-warnings-with-apt-get-update}

- 完全な警告メッセージは以下のいずれかです：

```
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

上記の問題を解決するには、以下のスクリプトを使用してください：

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

### 誤った署名のためにYumでパッケージを取得できない

可能な問題：キャッシュが誤っており、2022年9月にGPGキーが更新された後に壊れた可能性があります。

解決策はyumのキャッシュおよびlibディレクトリをクリアすることです：

```
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

その後、[インストールガイド](../getting-started/install.md#from-rpm-packages)に従ってください。

### Dockerコンテナを実行できない

単純な`docker run clickhouse/clickhouse-server`を実行すると、以下のようなスタックトレースでクラッシュします：

```
$ docker run -it clickhouse/clickhouse-server
........
2024.11.06 21:04:48.912036 [ 1 ] {} <Information> SentryWriter: Sending crash reports is disabled
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

理由は、バージョン`20.10.10`未満の古いDockerデーモンです。修正方法はそれをアップグレードするか、`docker run [--privileged | --security-opt seccomp=unconfined]`を実行することです。後者にはセキュリティ上の影響があります。

## サーバーへの接続 {#troubleshooting-accepts-no-connections}

可能な問題：

- サーバーが動作していない。
- 予期しない、または誤った構成パラメータ。

### サーバーが動作していない {#server-is-not-running}

**サーバーが動作しているか確認する**

コマンド：

``` bash
$ sudo service clickhouse-server status
```

サーバーが動作していない場合、以下のコマンドで開始してください：

``` bash
$ sudo service clickhouse-server start
```

**ログを確認する**

`clickhouse-server`のメインログはデフォルトで`/var/log/clickhouse-server/clickhouse-server.log`にあります。

サーバーが正常に起動した場合、以下の文字列が表示されるはずです：

- `<Information> Application: starting up.` — サーバーが起動しました。
- `<Information> Application: Ready for connections.` — サーバーが実行中で接続待機中です。

もし`clickhouse-server`の起動が構成エラーで失敗した場合、`<Error>`文字列とエラーの説明が表示されるはずです。例えば：

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

もしファイルの最後にエラーが表示されない場合、以下の文字列から始まる全ファイルを確認してください：

``` text
<Information> Application: starting up.
```

サーバーの2番目のインスタンスを起動しようとすると、以下のログが表示されます：

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

**system.dログを見る**

`clickhouse-server`のログに有用な情報がない、またはログが存在しない場合、以下のコマンドを使用して`system.d`ログを見ることができます：

``` bash
$ sudo journalctl -u clickhouse-server
```

**インタラクティブモードでclickhouse-serverを起動する**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

このコマンドは標準の自動起動スクリプトのパラメータでサーバーをインタラクティブアプリとして起動します。このモードでは`clickhouse-server`はすべてのイベントメッセージをコンソールに出力します。

### 設定パラメータ {#configuration-parameters}

確認事項：

- Docker設定。

  IPv6ネットワークでDocker内にClickHouseを実行する場合、`network=host`が設定されていることを確認してください。

- エンドポイント設定。

  [listen_host](../operations/server-configuration-parameters/settings.md#listen_host)と[tcp_port](../operations/server-configuration-parameters/settings.md#tcp_port)の設定を確認してください。

  ClickHouseサーバーはデフォルトでローカルホストからの接続のみを受け付けます。

- HTTPプロトコル設定。

  HTTP APIのプロトコル設定を確認してください。

- 安全な接続設定。

  以下の項目について確認してください：

  - [tcp_port_secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure)の設定。
  - [SSL証明書](../operations/server-configuration-parameters/settings.md#openssl)の設定。

  接続時に適切なパラメータを使用してください。例えば、`clickhouse_client`で`port_secure`パラメータを使用してください。

- ユーザー設定。

  誤ったユーザー名やパスワードを使用している可能性があります。

## クエリ処理 {#troubleshooting-does-not-process-queries}

ClickHouseがクエリを処理できない場合、エラーの説明をクライアントに送信します。`clickhouse-client`では、コンソールにエラーの説明が表示されます。HTTPインターフェースを使用している場合、ClickHouseはエラーメッセージをレスポンスボディに送信します。例えば：

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

`stack-trace`パラメータを使用して`clickhouse-client`を起動すると、ClickHouseはエラーの説明と共にサーバーのスタックトレースを返します。

接続が切れたというメッセージが表示された場合、そのクエリを再実行することができます。それでも毎回クエリを実行するたびに接続が切れる場合、サーバーログを確認してエラーをチェックしてください。

## クエリ処理の効率 {#troubleshooting-too-slow}

ClickHouseの動作が遅すぎる場合、クエリのためにサーバーリソースとネットワークに負荷をかけている可能性があります。

クエリのプロファイルを行うために`clickhouse-benchmark`ユーティリティを使用できます。これは、1秒あたり処理したクエリの数、1秒あたり処理した行の数、およびクエリ処理時間の百分位を示します。
