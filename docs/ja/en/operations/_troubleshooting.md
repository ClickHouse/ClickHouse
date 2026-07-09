---
title: トラブルシューティング
---

[//]: # "このファイルは「よくある質問」>「トラブルシューティング」に含まれています"

* [インストール](#troubleshooting-installation-errors)
* [サーバーへの接続](#troubleshooting-accepts-no-connections)
* [クエリ処理](#troubleshooting-does-not-process-queries)
* [クエリ処理の効率](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## インストール
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### apt-get では ClickHouseリポジトリから deb パッケージを取得できません
</div>

* ファイアウォールの設定を確認してください。
* 何らかの理由でリポジトリにアクセスできない場合は、[インストールガイド](../getting-started/install.md)に記載されている手順でパッケージをダウンロードし、`sudo dpkg -i <packages>` コマンドを使って手動でインストールしてください。`tzdata` パッケージも必要です。

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### apt-get を使用して ClickHouse リポジトリから deb パッケージを更新できない
</div>

* GPG キーが変更された場合、この問題が発生することがあります。

リポジトリ設定を更新するには、[セットアップ](../getting-started/install.md#setup-the-debian-repository) ページの手順に従ってください。

<div id="you-get-different-warnings-with-apt-get-update">
  ### `apt-get update` で異なる警告が表示される
</div>

* 表示される警告メッセージは、次のいずれかです。

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

上記の問題を解決するには、以下のスクリプトを使用してください。

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### 署名が正しくないため、yum でパッケージを取得できない
</div>

考えられる原因: キャッシュに問題がある可能性があります。おそらく、2022-09 に GPG キーが更新されたあとに破損しています。

解決するには、yum のキャッシュと lib ディレクトリを削除します:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

その後は、[インストールガイド](../getting-started/install.md#from-rpm-packages)に従ってください

<div id="you-cant-run-docker-container">
  ### Dockerコンテナーを実行できない
</div>

`docker run clickhouse/clickhouse-server` を実行すると、次のようなスタックトレースが表示されてクラッシュします:

```bash
$ docker run -it clickhouse/clickhouse-server
........
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

原因は、`20.10.10` 未満の古いバージョンの Docker デーモンです。対処方法としては、これをアップグレードするか、`docker run [--privileged | --security-opt seccomp=unconfined]` を実行します。後者にはセキュリティ上の注意点があります。

<div id="troubleshooting-accepts-no-connections">
  ## サーバーへの接続
</div>

考えられる問題:

* サーバーが稼働していません。
* 想定外または誤った設定パラメーター。

<div id="server-is-not-running">
  ### サーバーが起動していません
</div>

**サーバーが起動しているか確認します**

コマンド:

```bash
$ sudo service clickhouse-server status
```

サーバーが起動していない場合は、次のコマンドで起動してください。

```bash
$ sudo service clickhouse-server start
```

**ログを確認する**

`clickhouse-server` のメインログは、デフォルトでは `/var/log/clickhouse-server/clickhouse-server.log` にあります。

サーバーが正常に起動すると、次の文字列が表示されます。

* `<Information> Application: starting up.` — サーバーが起動しました。
* `<Information> Application: Ready for connections.` — サーバーは稼働中で、接続を受け付ける準備ができています。

`clickhouse-server` の起動が設定エラーで失敗した場合は、エラーの説明とともに `<Error>` という文字列が表示されます。たとえば、次のようになります。

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

ファイルの末尾にエラーが表示されていない場合は、次の文字列から始めてファイル全体を確認してください:

```text
<Information> Application: starting up.
```

サーバー上で `clickhouse-server` の2つ目のインスタンスを起動しようとすると、次のログが出力されます。

```text
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

**system.d のログを確認する**

`clickhouse-server` のログに有用な情報が見つからない、またはログ自体が存在しない場合は、次のコマンドで `system.d` のログを確認できます。

```bash
$ sudo journalctl -u clickhouse-server
```

**clickhouse-server を対話型モードで起動する**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

このコマンドは、自動起動スクリプトの標準パラメータを使って、サーバーを対話型アプリとして起動します。このモードでは、`clickhouse-server` はすべてのイベントメッセージをコンソールに表示します。

<div id="configuration-parameters">
  ### 設定パラメーター
</div>

確認してください。

* Docker の設定。

  IPv6 ネットワークで Docker 内の ClickHouse を実行している場合は、`network=host` が設定されていることを確認してください。

* エンドポイントの設定。

  [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) と [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port) の設定を確認してください。

  ClickHouse server は、デフォルトでは localhost からの接続のみを受け付けます。

* HTTP プロトコルの設定。

  HTTP API のプロトコル設定を確認してください。

* セキュア接続の設定。

  次を確認してください。

  * [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure) の設定。
  * [SSL certificates](../operations/server-configuration-parameters/settings.md#openssl) の設定。

    接続時には適切なパラメーターを使用してください。たとえば、`clickhouse_client` では `port_secure` パラメーターを使用します。

* ユーザー設定。

  ユーザー名またはパスワードが誤っている可能性があります。

<div id="troubleshooting-does-not-process-queries">
  ## クエリ処理
</div>

ClickHouse がクエリを処理できない場合は、クライアントにエラーの説明を送信します。`clickhouse-client` では、コンソールにエラーの説明が表示されます。HTTP インターフェイスを使用している場合、ClickHouse はレスポンスボディでエラーの説明を送信します。例:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

`clickhouse-client` を `stack-trace` パラメータ付きで起動すると、ClickHouse はエラーの説明とともにサーバーのスタックトレースを返します。

接続が切断されたことを示すメッセージが表示される場合があります。この場合は、クエリを再実行できます。クエリを実行するたびに接続が切断される場合は、サーバーログでエラーを確認してください。

<div id="troubleshooting-too-slow">
  ## クエリ処理の効率
</div>

ClickHouse の動作が遅い場合は、クエリ実行時のサーバーリソースやネットワークの負荷を分析する必要があります。

クエリのプロファイリングには `clickhouse-benchmark` ユーティリティを使用できます。このユーティリティでは、1 秒あたりに処理されるクエリ数、1 秒あたりに処理される行数、およびクエリの処理時間のパーセンタイルを確認できます。