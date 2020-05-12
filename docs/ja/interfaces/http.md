---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 19
toc_title: "HTTP\u30A4\u30F3\u30BF\u30FC"
---

# HTTPインター {#http-interface}

ﾂづﾂつｿﾂづｫﾂづｱﾂ鳴ｳﾂ猟ｿﾂづﾂづﾂつｫﾂづ慊つｷﾂ。 私たちは、javaやperl、シェルスクリプトから作業するためにそれを使用します。 他の部門では、perl、python、goからhttpインターフェイスが使用されています。 httpのインタフェースが限られにより、ネイティブインタフェースでより良い対応しています。

デフォルトでは、clickhouse-serverはポート8123でhttpをリッスンします（これは設定で変更できます）。

パラメータを指定せずにget/requestを実行すると、200個の応答コードと、以下で定義されている文字列が返されます [http\_server\_default\_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) デフォルト値 “Ok.” （最後にラインフィード付き)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

ヘルスチェックスクリプトでget/ping要求を使用します。 このハンドラは常に “Ok.” （最後に改行を入れて）。 バージョン18.12.13から利用可能。

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

リクエストをurlとして送信する ‘query’ パラメータ、または投稿として。 または、クエリの先頭を ‘query’ パラメータ、およびポストの残りの部分（これが必要な理由を後で説明します）。 URLのサイズは16KBに制限されているため、大きなクエリを送信するときはこの点に注意してください。

成功すると、200応答コードとその結果が応答本文に表示されます。
エラーが発生すると、応答本文に500応答コードとエラーの説明テキストが表示されます。

GETメソッドを使用する場合, ‘readonly’ 設定されています。 つまり、データを変更するクエリでは、POSTメソッドのみを使用できます。 クエリ自体は、POST本体またはURLパラメータのいずれかで送信できます。

例:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
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

あなたが見ることができるように、curlはurlエスケープする必要があります。
Wgetはそれ自体をすべてエスケープしますが、keep-aliveとTransfer-Encoding:chunkedを使用するとHTTP1.1よりもうまく動作しないため、使用することはお勧めしません。

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

デフォルトでは、データはtabseparated形式で返されます(詳細については、 “Formats” セクション）。
他の形式を要求するには、クエリのformat句を使用します。

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

データを送信するpostメソッドは、insertクエリに必要です。 この場合、urlパラメータにクエリの先頭を書き込み、postを使用して挿入するデータを渡すことができます。 挿入するデータは、たとえば、mysqlからタブで区切られたダンプです。 このようにして、insertクエリはmysqlからのload data local infileを置き換えます。

例:テーブルの作成:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

データ挿入用の使い慣れたinsertクエリの使用:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

データはクエリとは別に送信できます:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

任意のデータ形式を指定できます。 その ‘Values’ formatは、INSERTをt値に書き込むときに使用されるものと同じです:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

タブ区切りのダンプからデータを挿入するには、対応する形式を指定します:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

テーブルの内容を読む。 データは、並列クエリ処理によりランダムな順序で出力されます:

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

データテーブルを返さない要求が成功すると、空のレスポンスボディが返されます。

データを送信するときには、内部のclickhouse圧縮形式を使用できます。 圧縮されたデータには標準以外の形式があり、特別な形式を使用する必要があります `clickhouse-compressor` それを使用するプログラム（それは `clickhouse-client` パッケージ）。 データ挿入の効率を高めるために、サーバー側のチェックサム検証を無効にするには [http\_native\_compression\_disable\_checksumming\_on\_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) 設定。

指定した場合 `compress=1` URLでは、サーバーは送信するデータを圧縮します。
指定した場合 `decompress=1` URLでは、サーバーは渡したデータと同じデータを解凍します。 `POST` 方法。

また、使用することを選択 [HTTP圧縮](https://en.wikipedia.org/wiki/HTTP_compression). 圧縮を送信するには `POST` 要求、要求ヘッダーを追加します `Content-Encoding: compression_method`. ClickHouseが応答を圧縮するには、次のように追加する必要があります `Accept-Encoding: compression_method`. ClickHouse支援 `gzip`, `br`、と `deflate` [圧縮方法](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens). HTTP圧縮を有効にするには、ClickHouseを使用する必要があります [enable\_http\_compression](../operations/settings/settings.md#settings-enable_http_compression) 設定。 データ圧縮レベルを設定することができます [http\_zlib\_compression\_level](#settings-http_zlib_compression_level) すべての圧縮方法の設定。

利用することができ削減ネットワーク通信の送受信には大量のデータをダンプすると直ちに圧縮されます。

圧縮によるデータ送信の例:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "メモ"
    一部のhttpクライアントは、デフォルトでサーバーからデータを解凍します `gzip` と `deflate` 圧縮設定を正しく使用していても、圧縮解除されたデータが得られることがあります。

を使用することができ ‘database’ URLパラメータの指定はデフォルトのデータベースです。

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

既定では、サーバー設定に登録されているデータベースが既定のデータベースとして使用されます。 既定では、これはデータベースと呼ばれます ‘default’. あるいは、必ず指定のデータベースをドットの前にテーブルの名前です。

ユーザー名とパスワードは、次のいずれかの方法で指定できます:

1.  HTTP基本認証を使用する。 例えば:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  で ‘user’ と ‘password’ URLパラメーター。 例えば:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  を使用して ‘X-ClickHouse-User’ と ‘X-ClickHouse-Key’ ヘッダー。 例えば:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

ユーザー名が指定されていない場合は、 `default` 名前が使用されます。 パスワードを指定しない場合は、空のパスワードが使用されます。
にお使いいただけますurlパラメータで指定した設定処理の単一クエリーまたは全体をプロファイルを設定します。 例：http：//localhost：8123/？プロファイル=ウェブ&max\_rows\_to\_read=1000000000&クエリ=選択+1

詳細については、 [設定](../operations/settings/index.md) セクション。

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

その他のパラ “SET”.

同様に、httpプロトコルでclickhouseセッションを使用できます。 これを行うには、以下を追加する必要があります `session_id` 要求のパラメーターを取得します。 セッションIDとして任意の文字列を使用できます。 デフォルトでは、セッションは60秒の非アクティブの後に終了します。 このタイムアウトを変更するには、 `default_session_timeout` サーバー構成での設定、または `session_timeout` 要求のパラメーターを取得します。 セッショ `session_check=1` パラメータ。 単一のセッション内で実行できるのは、一度にひとつのクエリのみです。

クエリの進行状況に関する情報を受け取ることができます `X-ClickHouse-Progress` 応答ヘッダー。 これを行うには、 [send\_progress\_in\_http\_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). ヘッダーシーケンスの例:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

可能なヘッダフィールド:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

HTTP接続が失われた場合、実行中の要求は自動的に停止しません。 解析とデータフォーマットはサーバー側で実行され、ネットワークを使用することは効果がありません。
任意 ‘query\_id’ パラメータは、クエリID(任意の文字列)として渡すことができます。 詳細については、以下を参照してください “Settings, replace\_running\_query”.

任意 ‘quota\_key’ パラメータとして渡すことができ、クォーターキー(切文字列). 詳細については、以下を参照してください “Quotas”.

HTTPのインターフェースにより通外部データ（外部テンポラリテーブル)照会. 詳細については、以下を参照してください “External data for query processing”.

## 応答バッファリング {#response-buffering}

サーバー側で応答バッファリングを有効にできます。 その `buffer_size` と `wait_end_of_query` URLパラメータを提供しています。

`buffer_size` サーバーメモリ内のバッファーに結果内のバイト数を決定します。 結果本体がこのしきい値より大きい場合、バッファはHTTPチャネルに書き込まれ、残りのデータはHTTPチャネルに直接送信されます。

応答全体を確実にバッファリングするには、以下を設定します `wait_end_of_query=1`. この場合、メモリに格納されていないデータは、一時サーバーファイルにバッファーされます。

例えば:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

使用バッファリングなどでクエリの処理エラーが発生した後は、応答コード、httpヘッダが送信されます。 この状況では、エラーメッセージが応答本文の最後に書き込まれ、クライアント側では、解析の段階でのみエラーを検出できます。

### クエリパラメータ {#cli-queries-with-parameters}

を作成でき、クエリパラメータおよびパスの値から、対応するhttpリクエストパラメータ. 詳細については、 [CLIのパラメータを持つクエリ](cli.md#cli-queries-with-parameters).

### 例えば {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## 所定のhttpス {#predefined_http_interface}

ClickHouse支特定のクエリはHTTPインターフェース。 たとえば、次のように表にデータを書き込むことができます:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouseは定義済みのHTTPインターフェイスもサポートしています。 [プロメテウス輸出](https://github.com/percona-lab/clickhouse_exporter).

例えば:

-   まず、このセクションをサーバー構成ファイルに追加します:

<!-- -->

``` xml
<http_handlers>
  <predefine_query_handler>
      <url>/metrics</url>
        <method>GET</method>
        <queries>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </queries>
  </predefine_query_handler>
</http_handlers>
```

-   Prometheus形式のデータのurlを直接要求できるようになりました:

<!-- -->

``` bash
curl -vvv 'http://localhost:8123/metrics'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /metrics HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Wed, 27 Nov 2019 08:54:25 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-tl62qd0o
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: f39235f6-6ed7-488c-ae07-c7ceafb960f6
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
```

この例からわかるように、 `<http_handlers>` 設定で設定されています。xmlファイル、ClickHouseは、定義済みのタイプで受信したHTTP要求と一致します `<http_handlers>` 一致が成功した場合、ClickHouseは対応する事前定義されたクエリを実行します。

さて `<http_handlers>` 設定できます `<root_handler>`, `<ping_handler>`, `<replicas_status_handler>`, `<dynamic_query_handler>` と `<no_handler_description>` .

## root\_handler {#root_handler}

`<root_handler>` ルートパス要求の指定された内容を返します。 特定の戻りコンテンツは、 `http_server_default_response` 設定で。xmlだ 指定しない場合は、戻り値 **わかった**

`http_server_default_response` 定義されておらず、HttpリクエストがClickHouseに送信されます。 結果は次のとおりです:

``` xml
<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    Ok.

`http_server_default_response` 定義され、HttpリクエストがClickHouseに送信されます。 結果は次のとおりです:

``` xml
<http_server_default_response><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></http_server_default_response>

<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    <html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%

## ping\_handler {#ping_handler}

`<ping_handler>` 現在のClickHouseサーバーの健康を調査するのに使用することができます。 がClickHouse HTTPサーバが正常にアクセスClickHouseを通じて `<ping_handler>` 戻ります **わかった**.

例えば:

``` xml
<http_handlers>
    <ping_handler>/ping</ping_handler>
</http_handlers>
```

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

## replicas\_status\_handler {#replicas_status_handler}

`<replicas_status_handler>` レプリカノードの状態を検出して返すために使用されます **わかった** レプリカノードに遅延がない場合。 遅延がある場合、リターン特定の遅延。 の値 `<replicas_status_handler>` カスタマイズ対応。 指定しない場合 `<replicas_status_handler>`、ClickHouseデフォルト設定 `<replicas_status_handler>` は **/replicas\_status**.

例えば:

``` xml
<http_handlers>
    <replicas_status_handler>/replicas_status</replicas_status_handler>
</http_handlers>
```

遅延なしケース:

``` bash
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

遅延ケース:

``` bash
$ curl 'http://localhost:8123/replicas_status'
db.stats:  Absolute delay: 22. Relative delay: 22.
```

## predefined\_query\_handler {#predefined_query_handler}

設定できます `<method>`, `<headers>`, `<url>` と `<queries>` で `<predefined_query_handler>`.

`<method>` は、HTTPリクエストのメソッド部分のマッチングを担当します。 `<method>` 定義にの十分に合致します [方法](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) HTTPプロトコルで。 これはオプションの設定です。 構成ファイルで定義されていない場合は、HTTP要求のメソッド部分と一致しません

`<url>` HTTPリクエストのurl部分を照合する責任があります。 それはと互換性があります [RE2](https://github.com/google/re2)’の正規表現。 これはオプションの設定です。 構成ファイルで定義されていない場合は、HTTP要求のurl部分と一致しません

`<headers>` HTTPリクエストのヘッダ部分に一致させる責任があります。 これはRE2の正規表現と互換性があります。 これはオプションの設定です。 構成ファイルで定義されていない場合は、HTTP要求のヘッダー部分と一致しません

`<queries>` 値は、以下の定義済みのクエリです `<predefined_query_handler>` これは、HTTP要求が一致し、クエリの結果が返されたときにClickHouseによって実行されます。 これは必須の設定です。

`<predefined_query_handler>` 設定とquery\_params値の設定をサポートしています。

次の例では、次の値を定義します `max_threads` と `max_alter_threads` 設定、そしてクエリのテーブルから設定設定します。

例えば:

``` xml
<root_handlers>
    <predefined_query_handler>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <queries>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </queries>
    </predefined_query_handler>
</root_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "メモ"
    一つに `<predefined_query_handler>`、ワン `<queries>` のみをサポート `<query>` 挿入タイプの。

## dynamic\_query\_handler {#dynamic_query_handler}

`<dynamic_query_handler>` より `<predefined_query_handler>` 増加した `<query_param_name>` .

ClickHouse抽出し実行値に対応する `<query_param_name>` HTTPリクエストのurlの値。
ClickHouseのデフォルト設定 `<query_param_name>` は `/query` . これはオプションの設定です。 設定ファイルに定義がない場合、paramは渡されません。

この機能を試すために、この例では、max\_threadsとmax\_alter\_threadsの値を定義し、設定が正常に設定されたかどうかを照会します。
違いは、 `<predefined_query_handler>`、クエリは、設定ファイルに書き込まれます。 しかし、 `<dynamic_query_handler>`、クエリは、HTTP要求のparamの形で書かれています。

例えば:

``` xml
<root_handlers>
    <dynamic_query_handler>
        <headers>
            <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>
            <PARAMS_XXX><![CDATA[(?P<param_name_1>[^/]+)(/(?P<param_name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <query_param_name>query_param</query_param_name>
    </dynamic_query_handler>
</root_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/?query_param=SELECT%20value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D&max_threads=1&max_alter_threads=2&param_name_2=max_alter_threads'
1
2
```

[元の記事](https://clickhouse.tech/docs/en/interfaces/http_interface/) <!--hide-->
