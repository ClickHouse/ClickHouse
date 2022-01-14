---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u30B5\u30FC\u30D0\u30FC\u8A2D\u5B9A"
---

# サーバー設定 {#server-settings}

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

組み込み辞書を再ロードするまでの秒単位の間隔。

ClickHouseはx秒ごとに組み込みの辞書を再読み込みします。 これにより、辞書の編集が可能になります “on the fly” サーバーを再起動せずに。

デフォルト値は3600です。

**例**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## 圧縮 {#server-settings-compression}

データ圧縮の設定 [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md)-エンジンテーブル。

!!! warning "警告"
    ClickHouseの使用を開始したばかりの場合は、使用しないでください。

構成テンプレート:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` フィールド:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` または `zstd`.

複数の設定が可能です `<case>` セクション

条件を満たした場合のアクション:

-   データ部分が条件セットに一致する場合、ClickHouseは指定された圧縮方法を使用します。
-   データパーツが複数の条件セットに一致する場合、ClickHouseは最初に一致した条件セットを使用します。

データパーツの条件が満たされていない場合、ClickHouseは `lz4` 圧縮。

**例**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default_database {#default-database}

既定のデータベース。

データベースのリストを取得するには、 [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) クエリ。

**例**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

既定の設定プロファイル。

設定プロファイル内のファイルで指定されたパラメータ `user_config`.

**例**

``` xml
<default_profile>default</default_profile>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

外部辞書の設定ファイルへのパス。

パス:

-   絶対パスまたはサーバー設定ファイルに対する相対パスを指定します。
-   のパスを含むことができワイルドカード\*や?.

も参照。 “[外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**例**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

辞書の遅延読み込み。

もし `true` その後、各辞書は、最初の使用時に作成されます。 辞書の作成に失敗した場合、辞書を使用していた関数は例外をスローします。

もし `false` すべての辞書は、サーバーの起動時に作成され、エラーがある場合は、サーバーがシャットダウンされます。

既定値は次のとおりです `true`.

**例**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

入力データのスキーマを含むディレクトリへのパス。 [CapnProto](../../interfaces/formats.md#capnproto) 形式。

**例**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## 黒鉛 {#server_configuration_parameters-graphite}

データの送信先 [黒鉛](https://github.com/graphite-project).

設定:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root_path – Prefix for keys.
-   metrics – Sending data from the [システムメトリック](../../operations/system-tables.md#system_tables-metrics) テーブル。
-   events – Sending deltas data accumulated for the time period from the [システムイベント](../../operations/system-tables.md#system_tables-events) テーブル。
-   events_cumulative – Sending cumulative data from the [システムイベント](../../operations/system-tables.md#system_tables-events) テーブル。
-   asynchronous_metrics – Sending data from the [システムasynchronous_metrics](../../operations/system-tables.md#system_tables-asynchronous_metrics) テーブル。

複数の設定が可能です `<graphite>` 句。 たとえば、異なる間隔で異なるデータを送信するためにこれを使用できます。

**例**

``` xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

グラファイトのデータを薄くする設定。

詳細は、を参照してください [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**例**

``` xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## http_port/https_port {#http-porthttps-port}

HTTP経由でサーバーに接続するためのポート。

もし `https_port` 指定される, [openSSL](#server_configuration_parameters-openssl) 設定する必要があります。

もし `http_port` が指定されている場合、OpenSSL設定が設定されていても無視されます。

**例**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

ClickHouse HTTP(s)サーバーにアクセスするときにデフォルトで表示されるページ。
既定値は次のとおりです “Ok.” （最後に改行があります)

**例**

開く `https://tabix.io/` アクセス時 `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include_from {#server_configuration_parameters-include_from}

置換されたファイルへのパス。

詳細については “[設定ファイル](../configuration-files.md#configuration_files)”.

**例**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_http_port {#interserver-http-port}

ClickHouseサーバー間でデータを交換するポート。

**例**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

このサーバーへのアクセスに他のサーバーが使用できるホスト名。

省略された場合、これは `hostname-f` コマンド

特定のネッ

**例**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

認証時に使用されるユーザー名とパスワード [複製](../../engines/table-engines/mergetree-family/replication.md) 複製された\*エンジンで。 これらの資格情報は、レプリカ間の通信にのみ使用され、ClickHouseクライアントの資格情報とは無関係です。 サーバーにあるチェックにこれらの資格の接続にはレプリカと同じ資格を接続する場合はその他のレプリカ. なので、これらの資格を設定する同じすべてのレプリカ、クラスター
既定では、認証は使用されません。

このセクションでは以下のパラメータ:

-   `user` — username.
-   `password` — password.

**例**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep_alive_timeout {#keep-alive-timeout}

ClickHouseが接続を閉じる前に受信要求を待機する秒数。 既定値は3秒です。

**例**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

要求元のホストに対する制限。 したい場合はサーバーの回答をしているが、それらを指定し `::`.

例:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## ロガー {#server_configuration_parameters-logger}

ログ設定。

キー:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`と`errorlog`. ファイルが到達すると `size`、ClickHouseはアーカイブし、それの名前を変更し、その場所に新しいログファイルを作成します。
-   count – The number of archived log files that ClickHouse stores.

**例**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Syslogへの書き込みもサポートされています。 設定の例:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

キー:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [Syslog機能キーワード](https://en.wikipedia.org/wiki/Syslog#Facility) 大文字では “LOG_” 接頭辞: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`、というように）。
    デフォルト値: `LOG_USER` もし `address` 指定される, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` と `syslog.`

## マクロ {#macros}

複製されたテーブルのパラメーター置換。

ければ省略することができ複製のテーブルは使用しておりません。

詳細については “[複製テーブルの作成](../../engines/table-engines/mergetree-family/replication.md)”.

**例**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

テーブルエンジンが使用するマークのキャッシュのおおよそのサイズ(バイト単位) [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md) 家族だ

キャッシュの共有のサーバーメモリが割り当てられます。 キャッシュサイズは、少なくとも5368709120である必要があります。

**例**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_concurrent_queries {#max-concurrent-queries}

同時に処理される要求の最大数。

**例**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max_connections {#max-connections}

受信接続の最大数。

**例**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

開いているファイルの最大数。

既定では: `maximum`.

このオプションをMac OS Xで使用することをお勧めします。 `getrlimit()` 関数は、誤った値を返します。

**例**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

テーブルの削除に関する制限。

のサイズが [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md) テーブル超過 `max_table_size_to_drop` （バイト単位）、ドロップクエリを使用して削除することはできません。

ClickHouseサーバーを再起動せずにテーブルを削除する必要がある場合は、 `<clickhouse-path>/flags/force_drop_table` DROPクエリをファイルして実行します。

デフォルト値:50GB。

値0は、制限なしですべてのテーブルを削除できることを意味します。

**例**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge_tree {#server_configuration_parameters-merge_tree}

テーブルの微調整 [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md).

詳細については、MergeTreeSettingsを参照してください。hヘッダファイル。

**例**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSLクライアント/サーバー構成。

SSLのサポートは以下によって提供されます `libpoco` 図書館 ユーザーインターフェイスはファイルに記述 [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

サーバー/クライアント設定のキー:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` 証明書が含まれています。
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node's certificates. Details are in the description of the [文脈](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) クラス 可能な値: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. 許容値: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. このパラメータは常にお勧めです問題を避けることになるだろう両方の場合はサーバのキャッシュのセッションがクライアントの要望はキャッシュ. デフォルト値: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**設定例:**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## part_log {#server_configuration_parameters-part-log}

関連付けられたイベントのログ記録 [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md). たとえば、データの追加やマージなどです。 利用できるログを統合アルゴリズムと比較しています。 マージプロセスを視覚化できます。

クエリは [システムpart_log](../../operations/system-tables.md#system_tables-part-log) 別のファイルではなく、テーブル。 このテーブルの名前は、 `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**例**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## パス {#server_configuration_parameters-path}

データを含むディレクトリへのパス。

!!! note "注"
    末尾のスラッシュは必須です。

**例**

``` xml
<path>/var/lib/clickhouse/</path>
```

## プロメテウス {#server_configuration_parameters-prometheus}

スクレイピングの指標データの公開 [プロメテウス](https://prometheus.io).

設定:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [システムメトリック](../system-tables.md#system_tables-metrics) テーブル。
-   `events` – Flag that sets to expose metrics from the [システムイベント](../system-tables.md#system_tables-events) テーブル。
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [システムasynchronous_metrics](../system-tables.md#system_tables-asynchronous_metrics) テーブル。

**例**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query_log {#server_configuration_parameters-query-log}

クエリをログに記録するための設定 [log_queries=1](../settings/settings.md) 設定。

クエリは [システムquery_log](../../operations/system-tables.md#system_tables-query_log) 別のファイルではなく、テーブル。 テーブルの名前を変更することができます。 `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) テーブルのために。
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

テーブルが存在しない場合、ClickHouseはそれを作成します。 ClickHouseサーバーが更新されたときにクエリログの構造が変更された場合、古い構造のテーブルの名前が変更され、新しいテーブルが自動的に作成されます。

**例**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query-thread-log}

クエリのスレッドをログに記録するための設定 [log_query_threads=1](../settings/settings.md#settings-log-query-threads) 設定。

クエリは [システムquery_thread_log](../../operations/system-tables.md#system_tables-query-thread-log) 別のファイルではなく、テーブル。 テーブルの名前を変更することができます。 `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) システムテーブルの場合。
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

テーブルが存在しない場合、ClickHouseはそれを作成します。 ClickHouseサーバーの更新時にクエリスレッドログの構造が変更された場合、古い構造のテーブルの名前が変更され、新しいテーブルが自動的に作成されます。

**例**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace_log {#server_configuration_parameters-trace_log}

の設定 [trace_log](../../operations/system-tables.md#system_tables-trace_log) システムテーブル操作。

パラメータ:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) システムテーブルの場合。
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

既定のサーバー構成ファイル `config.xml` 次の設定セクションがあります:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query_masking_rules {#query-masking-rules}

サーバーログに格納する前に、すべてのログメッセージだけでなく、クエリにも適用されます,
`system.query_log`, `system.text_log`, `system.processes` クライアントに送信されたログ。 これにより
SQLクエリからの機密データ漏洩(名前、電子メール、個人など
ログへの識別子またはクレジットカード番号）。

**例**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

設定フィールド:
- `name` -ルールの名前(オプション)
- `regexp` -RE2互換の正規表現（必須)
- `replace` -機密データの置換文字列(オプション、デフォルトでは-六つのアスタリスク)

マスキングルールは、クエリ全体に適用されます(不正な形式/解析不可能なクエリから機密データが漏れるのを防ぐため)。

`system.events` テーブルカウンター `QueryMaskingRulesMatch` して全体のマスキングルール。

分散クエリの場合は、各サーバーを個別に構成する必要があります。
ノードはマスクなしで保存されます。

## remote_servers {#server-settings-remote-servers}

によって使用されるクラスタの構成 [分散](../../engines/table-engines/special/distributed.md) テーブルエンジンと `cluster` テーブル関数。

**例**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

の値に対して `incl` 属性は、節を参照してください “[設定ファイル](../configuration-files.md#configuration_files)”.

**も参照。**

-   [skip_unavailable_shards](../settings/settings.md#settings-skip_unavailable_shards)

## タイムゾーン {#server_configuration_parameters-timezone}

サーバーのタイムゾーン。

UTCタイムゾーンまたは地理的位置(たとえば、Africa/Abidjan)のIANA識別子として指定します。

タイムゾーンは、DateTimeフィールドをテキスト形式(画面またはファイルに出力)に出力するとき、および文字列からDateTimeを取得するときに、文字列とDateTime形式の間 また、タイムゾーンは、入力パラメーターでタイムゾーンを受信しなかった場合、時刻と日付を扱う関数で使用されます。

**例**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

TCPプロトコル経由でクライアントと通信するポート。

**例**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

クライアントとの安全な通信用のTCPポート。 それを使用して [OpenSSL](#server_configuration_parameters-openssl) 設定。

**可能な値**

正の整数。

**デフォルト値**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

MySQLプロトコ

**可能な値**

正の整数。

例

``` xml
<mysql_port>9004</mysql_port>
```

## tmp_path {#server-settings-tmp_path}

大規模なクエリを処理するための一時データへのパス。

!!! note "注"
    末尾のスラッシュは必須です。

**例**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#server-settings-tmp-policy}

ポリシーから [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 一時ファイルを格納する。
設定されていない場合 [`tmp_path`](#server-settings-tmp_path) それ以外の場合は無視されます。

!!! note "注"
    - `move_factor` 無視される
- `keep_free_space_bytes` 無視される
- `max_data_part_size_bytes` 無視される
なければならない同一数量の政策

## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

テーブルエンジンが使用する非圧縮データのキャッシュサイズ(バイト単位) [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md).

サーバーの共有キャッシュが一つあります。 メモリが割り当てられます。 キャッシュが使用されるのは [use_uncompressed_cache](../settings/settings.md#setting-use_uncompressed_cache) 有効です。

非圧縮キャッシュは、個々のケースで非常に短いクエリで有利です。

**例**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

ユ テーブル関数で使用されます [ファイル()](../../sql-reference/table-functions/file.md).

**例**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users_config {#users-config}

以下のファイルへのパス:

-   ユーザー構成。
-   アクセス権。
-   設定プロファイル。
-   クォータ設定。

**例**

``` xml
<users_config>users.xml</users_config>
```

## 飼育係 {#server-settings_zookeeper}

ClickHouseと対話できるようにする設定が含まれています。 [飼育係](http://zookeeper.apache.org/) クラスター。

ClickHouse用飼育係の保存メタデータのレプリカの使用時に再現します。 場合は複製のテーブルを使用していないので、このパラメータを省略することができます。

このセクションでは以下のパラメータ:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    例えば:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) これはClickHouseサーバーで使用されるznodeのルートとして使用されます。 任意。
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**設定例**

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
</zookeeper>
```

**も参照。**

-   [複製](../../engines/table-engines/mergetree-family/replication.md)
-   [ZooKeeperプログラマガイド](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

ZooKeeperのデータ部分ヘッダーの格納方法。

この設定は、 `MergeTree` 家族だ 指定できます:

-   グローバルに [merge_tree](#server_configuration_parameters-merge_tree) のセクション `config.xml` ファイル

    ClickHouseは、サーバー上のすべてのテーブルの設定を使用します。 設定はいつでも変更できます。 既存のテーブルは、設定が変更されると動作を変更します。

-   各テーブルのため。

    テーブルを作成するときは、対応する [エンジン設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). この設定を持つ既存のテーブルの動作は、グローバル設定が変更されても変更されません。

**可能な値**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

もし `use_minimalistic_part_header_in_zookeeper = 1` その後 [複製](../../engines/table-engines/mergetree-family/replication.md) テーブルは、単一のデータパーツのヘッダーをコンパクトに格納します `znode`. テーブルに多数の列が含まれている場合、この格納方法はZookeeperに格納されるデータの量を大幅に削減します。

!!! attention "注意"
    申請後 `use_minimalistic_part_header_in_zookeeper = 1` ClickHouseサーバーをこの設定をサポートしないバージョンにダウングレードすることはできません。 するとアップグレード時に注意ClickHouseサーバーにクラスター なアップの全てのサーバーです。 ClickHouseの新しいバージョンをテストするには、テスト環境またはクラスターの少数のサーバーでテストする方が安全です。

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**デフォルト値:** 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

内部DNSキャッシュを無効にします。 システムの作動のClickHouseのために推薦される
頻繁に変化するインフラなどのKubernetes.

**デフォルト値:** 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

ClickHouse内部DNSキャッシュに格納されているIPアドレスの更新期間(秒単位)。
更新は、別のシステムスレッドで非同期に実行されます。

**デフォルト値**: 15.

## access_control_path {#access_control_path}

パフォルダがClickHouseサーバー店舗ユーザーの役割構成で作成したSQLコマンド.

デフォルト値: `/var/lib/clickhouse/access/`.

**も参照。**

-   [アクセス制御とアカウント管理](../access-rights.md#access-control)

[元の記事](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
