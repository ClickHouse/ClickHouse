---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 57
toc_title: "\u30B5\u30FC\u30D0\u30FC\u8A2D\u5B9A"
---

# サーバー設定 {#server-settings}

## builtin\_dictionaries\_reload\_interval {#builtin-dictionaries-reload-interval}

組み込みの辞書を再ロードする前の秒単位の間隔。

クリックハウスは、内蔵の辞書ごとにx秒をリロードします。 これにより、辞書の編集が可能になります “on the fly” サーバーを再起動せずに。

デフォルト値:3600.

**例えば**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## 圧縮 {#server-settings-compression}

以下のためのデータ圧縮設定 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-エンジンテーブル。

!!! warning "警告"
    ClickHouseを使用し始めたばかりの場合は使用しないでください。

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

複数を設定できます `<case>` セクション。

条件が満たされたときの動作:

-   データパーツが条件セットと一致する場合、clickhouseは指定された圧縮方法を使用します。
-   データパートが複数の条件セットと一致する場合、clickhouseは最初に一致した条件セットを使用します。

デー `lz4` 圧縮。

**例えば**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default\_database {#default-database}

既定のデータベース。

データベースのリストを取得するには、 [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) クエリ。

**例えば**

``` xml
<default_database>default</default_database>
```

## default\_profile {#default-profile}

既定の設定プロファイル。

設定プロファイルはパラ `user_config`.

**例えば**

``` xml
<default_profile>default</default_profile>
```

## dictionaries\_config {#server_configuration_parameters-dictionaries_config}

外部ディクショナリの設定ファイルへのパス。

パス:

-   サーバー設定ファイルに対する絶対パスまたは相対パスを指定します。
-   のパスを含むことができワイルドカード\*や?.

また見なさい “[外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**例えば**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries\_lazy\_load {#server_configuration_parameters-dictionaries_lazy_load}

辞書の遅延ロード。

もし `true` その後、各辞書は最初の使用時に作成されます。 辞書の作成に失敗した場合、辞書を使用していた関数は例外をスローします。

もし `false` すべての辞書は、サーバーの起動時に作成され、エラーが発生した場合、サーバーはシャットダウンされます。

デフォルトは `true`.

**例えば**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format\_schema\_path {#server_configuration_parameters-format_schema_path}

入力データのスキームを持つディレクトリへのパス。 [CapnProto](../../interfaces/formats.md#capnproto) フォーマット。

**例えば**

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
-   root\_path – Prefix for keys.
-   metrics – Sending data from the [システム。指標](../../operations/system-tables.md#system_tables-metrics) テーブル。
-   events – Sending deltas data accumulated for the time period from the [システム。イベント](../../operations/system-tables.md#system_tables-events) テーブル。
-   events\_cumulative – Sending cumulative data from the [システム。イベント](../../operations/system-tables.md#system_tables-events) テーブル。
-   asynchronous\_metrics – Sending data from the [システム。asynchronous\_metrics](../../operations/system-tables.md#system_tables-asynchronous_metrics) テーブル。

複数を設定できます `<graphite>` 句。 たとえば、異なる間隔で異なるデータを送信するためにこれを使用できます。

**例えば**

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

## graphite\_rollup {#server_configuration_parameters-graphite-rollup}

グラファイトの間引きデータの設定。

詳細については、 [グラフィットメールグツリー](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**例えば**

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

## http\_port/https\_port {#http-porthttps-port}

HTTP経由でサーバーに接続するためのポート。

もし `https_port` が指定される。, [openSSL](#server_configuration_parameters-openssl) 構成する必要があります。

もし `http_port` が指定されている場合、OpenSSL設定が設定されていても、その設定は無視される。

**例えば**

``` xml
<https>0000</https>
```

## http\_server\_default\_response {#server_configuration_parameters-http_server_default_response}

ClickHouse HTTP(s)サーバーにアクセスするときにデフォルトで表示されるページ。
デフォルト値は “Ok.” （最後にラインフィード付き)

**例えば**

開く `https://tabix.io/` アクセス時 `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include\_from {#server_configuration_parameters-include_from}

置換を伴うファイルへのパス。

詳細については、以下を参照してください “[設定ファイル](../configuration-files.md#configuration_files)”.

**例えば**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver\_http\_port {#interserver-http-port}

ClickHouseサーバ間でデータを交換するためのポート。

**例えば**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver\_http\_host {#interserver-http-host}

このサーバーへのアクセスに他のサーバーが使用できるホスト名。

省略された場合、それは同じ方法で定義されます `hostname-f` 司令部

特定のネットワー

**例えば**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver\_http\_credentials {#server-settings-interserver-http-credentials}

認証に使用するユーザー名とパスワード [複製](../../engines/table-engines/mergetree-family/replication.md) レプリケートされた\*エンジン。 これらの資格情報は、レプリカ間の通信にのみ使用され、ClickHouseクライアントの資格情報とは無関係です。 サーバーにあるチェックにこれらの資格の接続にはレプリカと同じ資格を接続する場合はその他のレプリカ. なので、これらの資格を設定する同じすべてのレプリカ、クラスター
デフォルトでは、認証は使用されません。

このセクショ:

-   `user` — username.
-   `password` — password.

**例えば**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep\_alive\_timeout {#keep-alive-timeout}

接続を閉じる前に、clickhouseが着信要求を待機する秒数。 デフォルトは3秒です。

**例えば**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen\_host {#server_configuration_parameters-listen_host}

要求元のホストの制限。 したい場合はサーバーの回答をしているが、それらを指定し `::`.

例:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## ロガー {#server_configuration_parameters-logger}

ログの設定。

キー:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`と`errorlog`. ファイルが届くと `size`、ClickHouseのアーカイブと名前を変更し、その場所に新しいログファイルを作成します。
-   count – The number of archived log files that ClickHouse stores.

**例えば**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Syslogへの書き込みもサポートされています。 設定例:

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

-   use\_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [Syslog機能キーワード](https://en.wikipedia.org/wiki/Syslog#Facility) 大文字で “LOG\_” 接頭辞: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`、というように)。
    デフォルト値: `LOG_USER` もし `address` が指定される。, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` と `syslog.`

## マクロ {#macros}

パラメータの置換のために再現します。

ければ省略することができ複製のテーブルは使用しておりません。

詳細については、以下を参照してください “[複製テーブルの作成](../../engines/table-engines/mergetree-family/replication.md)”.

**例えば**

``` xml
<macros incl="macros" optional="true" />
```

## mark\_cache\_size {#server-mark-cache-size}

約サイズ(バイトのキャッシュのマークの使用によりテーブルエンジンの [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族

キャッシュの共有のサーバーメモリが割り当てられます。 キャッシュサイズは5368709120以上である必要があります。

**例えば**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max\_concurrent\_queries {#max-concurrent-queries}

同時に処理された要求の最大数。

**例えば**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max\_connections {#max-connections}

受信接続の最大数。

**例えば**

``` xml
<max_connections>4096</max_connections>
```

## max\_open\_files {#max-open-files}

開いているファイルの最大数。

デフォルトでは: `maximum`.

Mac OS Xでこのオプションを使用することをお勧めします。 `getrlimit()` 関数は不正な値を返します。

**例えば**

``` xml
<max_open_files>262144</max_open_files>
```

## max\_table\_size\_to\_drop {#max-table-size-to-drop}

テーブルの削除に関する制限。

のサイズ [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルを超え `max_table_size_to_drop` （バイト単位）、ドロップクエリを使用して削除することはできません。

それでもclickhouseサーバーを再起動せずにテーブルを削除する必要がある場合は、 `<clickhouse-path>/flags/force_drop_table` ドロップクエリを実行します。

デフォルト値:50gb.

値0は、制限なしにすべてのテーブルを削除できることを意味します。

**例えば**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge\_tree {#server_configuration_parameters-merge_tree}

のテーブルのための微調整 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

詳細については、“mergetreesettings”を参照してください。hヘッダファイル。

**例えば**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSLクライアント/サーバー構成。

SSLのサポートは、 `libpoco` ライブラリ。 ユーザーインターフェイスはファイルに記述 [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

サーバー/クライアント設定のキー:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` 証明書が含まれています。
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node’s certificates. Details are in the description of the [文脈](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) クラス。 可能な値: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. 許容値: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. このパラメータは常にお勧めです問題を避けることになるだろう両方の場合はサーバのキャッシュのセッションがクライアントの要望はキャッシュ. デフォルト値: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1\_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library’s OpenSSL version supports FIPS.
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

## part\_log {#server_configuration_parameters-part-log}

関連付けられているログイベント [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). たとえば、データの追加やマージなどです。 利用できるログを統合アルゴリズムと比較しています。 マージプロセスを視覚化できます。

クエリはログに記録されます [システム。part\_log](../../operations/system-tables.md#system_tables-part-log) テーブル、別のファイルではありません。 このテーブルの名前を設定することができます `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**例えば**

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

!!! note "メモ"
    末尾のスラッシュは必須です。

**例えば**

``` xml
<path>/var/lib/clickhouse/</path>
```

## クエリーログ {#server_configuration_parameters-query-log}

で受信したロギングクエリの設定 [log\_queries=1](../settings/settings.md) 設定。

クエリはログに記録されます [システム。クエリーログ](../../operations/system-tables.md#system_tables-query_log) テーブル、別のファイルではありません。 テーブルの名前を変更することができます `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) テーブルのため。
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

テーブルが存在しない場合、clickhouseはそれを作成します。 clickhouseサーバーが更新されたときにクエリログの構造が変更された場合、古い構造を持つテーブルの名前が変更され、新しいテーブルが自動的に作成されます。

**例えば**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query\_thread\_log {#server_configuration_parameters-query-thread-log}

受信したクエリのスレッドをログに記録する設定 [log\_query\_threads=1](../settings/settings.md#settings-log-query-threads) 設定。

クエリはログに記録されます [システム。query\_thread\_log](../../operations/system-tables.md#system_tables-query-thread-log) テーブル、別のファイルではありません。 テーブルの名前を変更することができます `table` パラメータ(下記参照)。

以下のパラメータの設定ロギング:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) システムテーブルの場合。
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

テーブルが存在しない場合、clickhouseはそれを作成します。 clickhouseサーバーの更新時にクエリスレッドログの構造が変更された場合、古い構造を持つテーブルの名前が変更され、新しいテーブルが自動的に作成されます。

**例えば**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace\_log {#server_configuration_parameters-trace_log}

のための設定 [trace\_log](../../operations/system-tables.md#system_tables-trace_log) システムテーブル操作。

パラメータ:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [カスタム分割キー](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) システムテーブルの場合。
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

既定のサーバー設定ファイル `config.xml` 次の設定セクションを含みます:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query\_masking\_rules {#query-masking-rules}

サーバーログに保存する前に、クエリとすべてのログメッセージに適用される、regexpベースのルール,
`system.query_log`, `system.text_log`, `system.processes` テーブル、およびクライアントに送信されたログ。 これにより
SQLクエリからの機密データ漏えい（名前、電子メール、個人など）
ログへの識別子またはクレジットカード番号）。

**例えば**

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
- `regexp` -RE2互換の正規表現(必須)
- `replace` -機密データのための置換文字列（デフォルトではオプション-sixアスタリスク)

マスキングルールは、クエリ全体に適用されます（不正な形式の非解析可能なクエリからの機密データの漏洩を防ぐため）。

`system.events` テーブルに反対がある `QueryMaskingRulesMatch` クエリマスキングルールの総数が一致する。

分散クエリの場合、各サーバーを個別に構成する必要があります。
ノードはマスクせずに保存されます。

## リモートサーバー {#server-settings-remote-servers}

によって使用されるクラスターの構成 [分散](../../engines/table-engines/special/distributed.md) テーブルエンジンと `cluster` テーブル機能。

**例えば**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

の値について `incl` 属性、セクションを参照 “[設定ファイル](../configuration-files.md#configuration_files)”.

**また見なさい**

-   [skip\_unavailable\_shards](../settings/settings.md#settings-skip_unavailable_shards)

## タイムゾーン {#server_configuration_parameters-timezone}

サーバーのタイムゾーン。

UTCタイムゾーンまたは地理的位置(たとえば、Africa/Abidjan)のIANA識別子として指定します。

タイムゾーンは、datetimeフィールドがテキスト形式（画面またはファイルに印刷される）に出力される場合、および文字列からdatetimeを取得する場合に、文字列とdatetime さらに、タイムゾーンは、入力パラメータでタイムゾーンを受信しなかった場合、時刻と日付を扱う関数で使用されます。

**例えば**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp\_portgenericname {#server_configuration_parameters-tcp_port}

TCPプロトコル経由でクライアントと通信するポート。

**例えば**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp\_port\_secure {#server_configuration_parameters-tcp_port_secure}

クライアン それを使用する [OpenSSL](#server_configuration_parameters-openssl) 設定。

**可能な値**

正の整数。

**デフォルト値**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql\_portgenericname {#server_configuration_parameters-mysql_port}

ポートと通信すmysqlプロトコルです。

**可能な値**

正の整数。

例えば

``` xml
<mysql_port>9004</mysql_port>
```

## tmp\_path {#server-settings-tmp_path}

大規模なクエリを処理するための一時データへのパス。

!!! note "メモ"
    末尾のスラッシュは必須です。

**例えば**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp\_policy {#server-settings-tmp-policy}

からのポリシー [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 一時ファイルを保存する。
設定されていない場合 [`tmp_path`](#server-settings-tmp_path) それ以外の場合は無視されます。

!!! note "メモ"
    - `move_factor` は無視されます
- `keep_free_space_bytes` は無視されます
- `max_data_part_size_bytes` は無視されます
-そのポリシーには正確に一つのボリュームが必要です

## uncompressed\_cache\_size {#server-settings-uncompressed_cache_size}

テーブルエンジンによって使用される非圧縮データのキャッシュサイズ(バイト単位)。 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

サーバーの共有キャッシュがあります。 メモ このオプ [use\_uncompressed\_cache](../settings/settings.md#setting-use_uncompressed_cache) は有効です。

圧縮されていないキャッシュは、個々のケースで非常に短いクエリに有利です。

**例えば**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user\_files\_path {#server_configuration_parameters-user_files_path}

ユー テーブル関数で使用されます [ファイル()](../../sql-reference/table-functions/file.md).

**例えば**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users\_config {#users-config}

以下を含むファイルへのパス:

-   ユーザー構成。
-   アクセス権。
-   設定プロファイル。
-   クォータの設定。

**例えば**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

ClickHouseとの対話を許可する設定が含まれています [ZooKeeper](http://zookeeper.apache.org/) クラスター

ClickHouse用飼育係の保存メタデータのレプリカの使用時に再現します。 場合は複製のテーブルを使用していないので、このパラメータを省略することができます。

このセクショ:

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
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) これは、ClickHouseサーバーで使用されるznodesのルートとして使用されます。 任意です。
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

**また見なさい**

-   [複製](../../engines/table-engines/mergetree-family/replication.md)
-   [ZooKeeperプログラマーズガイド](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use\_minimalistic\_part\_header\_in\_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

ZooKeeperのデータパートヘッダーの保存方法。

この設定は、 `MergeTree` 家族 指定できます:

-   グローバルに [merge\_tree](#server_configuration_parameters-merge_tree) のセクション `config.xml` ファイル。

    ClickHouseは、サーバー上のすべてのテーブルの設定を使用します。 設定はいつでも変更できます。 既存の表は、設定が変更されたときの動作を変更します。

-   各テーブルのため。

    テーブルを作成するときは、対応する [エンジンの設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). この設定を持つ既存のテーブルの動作は、グローバル設定が変更されても変更されません。

**可能な値**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

もし `use_minimalistic_part_header_in_zookeeper = 1`、その後 [複製された](../../engines/table-engines/mergetree-family/replication.md) テーブルのヘッダのデータ部品のコンパクトを `znode`. の場合はテーブルを含む多く、この保管方法を大幅に低減量のデータが保存されて飼育係.

!!! attention "注意"
    適用後 `use_minimalistic_part_header_in_zookeeper = 1` ClickHouseサーバーをこの設定をサポートしないバージョンにダウングレードすることはできません。 するとアップグレード時に注意ClickHouseサーバーにクラスター なアップの全てのサーバーです。 テスト環境で、またはクラスターのほんの数台のサーバーで、新しいバージョンのClickHouseをテストする方が安全です。

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**デフォルト値:** 0.

## disable\_internal\_dns\_cache {#server-settings-disable-internal-dns-cache}

内部dnsキャッシュを無効にします。 システムの作動のclickhouseのために推薦される
Kubernetesのような頻繁に変更の下部組織を使って。

**デフォルト値:** 0.

## dns\_cache\_update\_period {#server-settings-dns-cache-update-period}

ClickHouse内部DNSキャッシュに保存されているIPアドレスの更新期間(秒単位)。
更新は、別のシステムスレッドで非同期に実行されます。

**デフォルト値**: 15.

[元の記事](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
