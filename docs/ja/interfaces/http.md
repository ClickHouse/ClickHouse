---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 19
toc_title: "HTTP\u30A4\u30F3\u30BF\u30FC\u30D5\u30A7"
---

# HTTPインターフェ {#http-interface}

HTTPイトのご利用ClickHouseにプラットフォームからゆるプログラミング言語です。 JavaやPerl、シェルスクリプトからの作業に使用します。 他の部門では、HttpインターフェイスはPerl、Python、Goから使用されます。 HTTPイ

デフォルトでは、clickhouse-serverはポート8123でHTTPをリッスンします（これは設定で変更できます）。

パラメータなしでGET/requestを行うと、200の応答コードとで定義された文字列が返されます [http_server_default_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) デフォルト値 “Ok.” （最後に改行があります)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

ヘルスチェックスクリプトでGET/ping要求を使用します。 このハンドラは常に “Ok.” （最後にラインフィード付き）。 バージョン18.12.13から利用可能。

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

リクエストをURLとして送信する ‘query’ パラメータ、またはポストとして。 または、クエリの先頭を ‘query’ パラメータ、およびポストの残りの部分（これが必要な理由を後で説明します）。 URLのサイズは16KBに制限されているため、大規模なクエリを送信する場合はこの点に注意してください。

成功すると、200の応答コードとその結果が応答本文に表示されます。
エラーが発生すると、応答本文に500応答コードとエラー説明テキストが表示されます。

GETメソッドを使用する場合, ‘readonly’ 設定されています。 つまり、データを変更するクエリでは、POSTメソッドのみを使用できます。 クエリ自体は、POST本文またはURLパラメータのいずれかで送信できます。

例:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -nv -O- 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

ご覧の通り、カールはやや不便ですがそのスペースするURLが自動的にエスケープされます。
Wgetはすべてそのものをエスケープしますが、keep-aliveとTransfer-Encoding:chunkedを使用する場合、HTTP1.1よりもうまく動作しないため、使用することはお勧めしません。

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

クエリの一部がパラメータで送信され、投稿の一部が送信されると、これらの二つのデータ部分の間に改行が挿入されます。
例（これは動作しません):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

デフォルトでは、データはTabSeparated形式で返されます(詳細については、 “Formats” セクション）。
他の形式を要求するには、クエリのFORMAT句を使用します。

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

データを送信するPOSTメソッドは、INSERTクエリに必要です。 この場合、URLパラメーターにクエリの先頭を記述し、POSTを使用して挿入するデータを渡すことができます。 挿入するデータは、たとえば、MySQLからのタブ区切りのダンプです。 このようにして、INSERTクエリはMYSQLからのLOAD DATA LOCAL INFILEを置き換えます。

例:テーブルの作成:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

データ挿入のための使い慣れたINSERTクエリの使用:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

データはクエリとは別に送信できます:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

任意のデータ形式を指定できます。 その ‘Values’ 書式は、INSERTをt値に書き込むときに使用される書式と同じです:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

タブ区切りのダンプからデータを挿入するには、対応する形式を指定します:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

テーブルの内容を読む。 並列クエリ処理により、データがランダムに出力されます:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

テーブルの削除。

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

データテーブルを返さない正常な要求の場合、空の応答本文が返されます。

データを送信するときは、内部ClickHouse圧縮形式を使用できます。 圧縮されたデータは非標準形式であり、特別な形式を使用する必要があります `clickhouse-compressor` それで動作するようにプログラム（それは `clickhouse-client` パッケージ）。 データ挿入の効率を高めるために、以下を使用してサーバー側のチェックサム検証を無効にできます [http_native_compression_disable_checksumming_on_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) 設定。

指定した場合 `compress=1` URLでは、サーバーが送信するデータを圧縮します。
指定した場合 `decompress=1` このURLでは、サーバーは渡すデータと同じデータを解凍します。 `POST` 方法。

また、使用することもできます [HTTP圧縮](https://en.wikipedia.org/wiki/HTTP_compression). 圧縮を送信するには `POST` リクエストヘッダーを追加します `Content-Encoding: compression_method`. ClickHouseが応答を圧縮するには、次のように追加する必要があります `Accept-Encoding: compression_method`. ClickHouseサポート `gzip`, `br`,and `deflate` [圧縮方法](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens). HTTP圧縮を有効にするには、ClickHouseを使用する必要があります [enable_http_compression](../operations/settings/settings.md#settings-enable_http_compression) 設定。 データ圧縮レベルは、 [http_zlib_compression_level](#settings-http_zlib_compression_level) すべての圧縮方法の設定。

利用することができ削減ネットワーク通信の送受信には大量のデータをダンプすると直ちに圧縮されます。

圧縮によるデータ送信の例:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "注"
    あるHTTPお客様が圧縮解除データからサーバによるデフォルト（ `gzip` と `deflate`）圧縮設定を正しく使用しても、圧縮解除されたデータが得られることがあります。

を使用することができます ‘database’ 既定のデータベースを指定するURLパラメータ。

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

既定では、サーバー設定に登録されているデータベースが既定のデータベースとして使用されます。 デフォルトでは、このデータベース ‘default’. または、テーブル名の前にドットを使用してデータベースを常に指定できます。

ユーザー名とパスワードは、次のいずれかの方法で指定できます:

1.  HTTP基本認証の使用。 例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  で ‘user’ と ‘password’ URLパラメータ。 例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  を使用して ‘X-ClickHouse-User’ と ‘X-ClickHouse-Key’ ヘッダー。 例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

ユーザ名が指定されていない場合、 `default` 名前が使用されます。 パスワードを指定しない場合は、空のパスワードが使用されます。
にお使いいただけますURLパラメータで指定した設定処理の単一クエリーまたは全体をプロファイルを設定します。 例:http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1

詳細については、を参照してください [設定](../operations/settings/index.md) セクション

``` bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

のための情報その他のパラメータの項をご参照ください “SET”.

同様に、HttpプロトコルでClickHouseセッションを使用できます。 これを行うには、 `session_id` 要求のパラメータを取得します。 セッションIDとして任意の文字列を使用できます。 既定では、セッションは非アクティブの60秒後に終了します。 このタイムアウトを変更するには、 `default_session_timeout` サーバー構成で設定するか、または `session_timeout` 要求のパラメータを取得します。 セッションステータスを確認するには、 `session_check=1` パラメータ。 単一のセッション内で一度に一つのクエリだけを実行できます。

クエリの進行状況に関する情報を受け取ることができます `X-ClickHouse-Progress` 応答ヘッダー。 これを行うには、有効にします [send_progress_in_http_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). ヘッダシーケンスの例:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

可能なヘッダ項目:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

HTTP接続が失われても、要求の実行は自動的に停止しません。 解析とデータの書式設定はサーバー側で実行され、ネットワークを使用すると無効になる可能性があります。
任意 ‘query_id’ パラメータは、クエリID(任意の文字列)として渡すことができます。 詳細については “Settings, replace_running_query”.

任意 ‘quota_key’ パラメータとして渡すことができ、クォーターキー(切文字列). 詳細については “Quotas”.

HTTPイ 詳細については “External data for query processing”.

## 応答バッファリング {#response-buffering}

サーバー側で応答バッファリングを有効にできます。 その `buffer_size` と `wait_end_of_query` URLパラメータを提供しています。

`buffer_size` サーバーメモリ内のバッファーに格納する結果のバイト数を決定します。 結果の本文がこのしきい値より大きい場合、バッファはHTTPチャネルに書き込まれ、残りのデータはHTTPチャネルに直接送信されます。

保全の対応はバッファ処理されますが、設定 `wait_end_of_query=1`. この場合、メモリに格納されていないデータは一時サーバーファイルにバッファされます。

例:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

使用バッファリングなどでクエリの処理エラーが発生した後は、応答コード、HTTPヘッダが送信されます。 この状況では、応答本文の最後にエラーメッセージが書き込まれ、クライアント側では、エラーは解析段階でのみ検出できます。

### パラメー {#cli-queries-with-parameters}

パラメータを使用してクエリを作成し、対応するHTTP要求パラメータから値を渡すことができます。 詳細については、 [CLIのパラメータを使用した照会](cli.md#cli-queries-with-parameters).

### 例 {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## 所定のHTTPス {#predefined_http_interface}

ClickHouse支特定のクエリはHTTPインターフェース。 たとえば、次のように表にデータを書き込むことができます:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouseにも対応した所定のHTTPインターフェースができありがとの統合に第三者のリーディングプロジェクト [プロメテウス輸出](https://github.com/percona-lab/clickhouse_exporter).

例:

-   まず、このセクションをサーバー設定ファイルに追加します:

<!-- -->

``` xml
<http_handlers>
    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </handler>
    </rule>
    <rule>...</rule>
    <rule>...</rule>
</http_handlers>
```

-   Prometheus形式のデータのurlを直接リクエストできるようになりました:

<!-- -->

``` bash
$ curl -v 'http://localhost:8123/predefined_query'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /predefined_query HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Tue, 28 Apr 2020 08:52:56 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-mloy5trc
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: 96fe0052-01e6-43ce-b12a-6b7370de6e8a
< X-ClickHouse-Format: Template
< X-ClickHouse-Timezone: Asia/Shanghai
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact


* Connection #0 to host localhost left intact
```

この例からわかるように、 `<http_handlers>` 設定で構成されています。xmlファイルと `<http_handlers>` を含むことができ多くの `<rule>s`. ClickHouseは、受信したHTTP要求を事前定義されたタイプに一致させます `<rule>` 最初に一致したハンドラが実行されます。 一致が成功すると、ClickHouseは対応する事前定義されたクエリを実行します。

> さて `<rule>` 構成できます `<method>`, `<headers>`, `<url>`,`<handler>`:
> `<method>` HTTP要求のメソッド部分の照合を担当します。 `<method>` 十分に定義にの合致します [方法](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) HTTPプロトコルで。 これはオプションの構成です。 構成ファイルで定義されていない場合、HTTP要求のメソッド部分と一致しません。
>
> `<url>` HTTPリクエストのurl部分の照合を担当します。 それはと互換性があります [RE2](https://github.com/google/re2)の正規表現。 これはオプションの構成です。 構成ファイルで定義されていない場合、HTTP要求のurl部分と一致しません。
>
> `<headers>` HTTPリクエストのヘッダー部分の照合を担当します。 RE2の正規表現と互換性があります。 これはオプションの構成です。 構成ファイルで定義されていない場合、HTTP要求のヘッダー部分と一致しません。
>
> `<handler>` 主要な処理の部品を含んでいます。 さて `<handler>` 構成できます `<type>`, `<status>`, `<content_type>`, `<response_content>`, `<query>`, `<query_param_name>`.
> \> `<type>` 現在サポート: **predefined_query_handler**, **dynamic_query_handler**, **静的**.
> \>
> \> `<query>` -predefined_query_handler型で使用し、ハンドラが呼び出されたときにクエリを実行します。
> \>
> \> `<query_param_name>` -dynamic_query_handler型で使用すると、それに対応する値を抽出して実行します。 `<query_param_name>` HTTP要求パラメータの値。
> \>
> \> `<status>` -静的タイプ、応答ステータスコードで使用します。
> \>
> \> `<content_type>` -静的なタイプ、応答との使用 [コンテンツタイプ](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).
> \>
> \> `<response_content>` 使用静止型で、応答が送信（発信）したコンテンツをクライアントでご使用になる場合、接頭辞 ‘file://’ または ‘config://’、クライアントに送信するファイルまたは構成から内容を検索します。

次に、異なる設定方法を示します `<type>`.

## predefined_query_handler {#predefined_query_handler}

`<predefined_query_handler>` 設定とquery_params値の設定をサポートします。 設定できます `<query>` のタイプで `<predefined_query_handler>`.

`<query>` 値は以下の定義済みクエリです `<predefined_query_handler>` これは、Http要求が一致し、クエリの結果が返されたときにClickHouseによって実行されます。 これは必須構成です。

次の例では、次の値を定義します `max_threads` と `max_alter_threads` 設定、そしてクエリのテーブルから設定設定します。

例:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "注意"
    一つで `<predefined_query_handler>` 一つだけをサポート `<query>` 挿入タイプ。

## dynamic_query_handler {#dynamic_query_handler}

で `<dynamic_query_handler>`、クエリは、HTTP要求のparamの形式で書かれています。 違いは、 `<predefined_query_handler>`、クエリは設定ファイルに書き込まれます。 設定できます `<query_param_name>` で `<dynamic_query_handler>`.

クリックハウスは、 `<query_param_name>` HTTP要求のurlの値。 のデフォルト値 `<query_param_name>` は `/query` . これはオプションの構成です。 設定ファイルに定義がない場合、paramは渡されません。

この機能を試すために、この例ではmax_threadsとmax_alter_threadsの値を定義し、設定が正常に設定されたかどうかを照会します。

例:

``` xml
<http_handlers>
    <rule>
    <headers>
        <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>    </headers>
    <handler>
        <type>dynamic_query_handler</type>
        <query_param_name>query_param</query_param_name>
    </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_alter_threads=2&param_name_1=max_threads&param_name_2=max_alter_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_alter_threads   2
```

## 静的 {#static}

`<static>` 戻れる [content_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [状態](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) そしてresponse_content。 response_contentは、指定された内容を返すことができます

例:

メッセージを返す。

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ curl -vv  -H 'XXX:xxx' 'http://localhost:8123/hi'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /hi HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 402 Payment Required
< Date: Wed, 29 Apr 2020 03:51:26 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

のコンテンツから構成送信します。

``` xml
<get_config_static_handler><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></get_config_static_handler>

<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_config_static_handler</url>
            <handler>
                <type>static</type>
                <response_content>config://get_config_static_handler</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ curl -v  -H 'XXX:xxx' 'http://localhost:8123/get_config_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_config_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:01:24 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

クライアントに送信するファイルから内容を検索します。

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file:///absolute_path_file.html</response_content>
            </handler>
        </rule>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_relative_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file://./relative_path_file.html</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ user_files_path='/var/lib/clickhouse/user_files'
$ sudo echo "<html><body>Relative Path File</body></html>" > $user_files_path/relative_path_file.html
$ sudo echo "<html><body>Absolute Path File</body></html>" > $user_files_path/absolute_path_file.html
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_absolute_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_absolute_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:16 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Absolute Path File</body></html>
* Connection #0 to host localhost left intact
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_relative_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_relative_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:31 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```

[元の記事](https://clickhouse.tech/docs/en/interfaces/http_interface/) <!--hide-->
