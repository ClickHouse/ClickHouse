---
slug: /ja/interfaces/http
sidebar_position: 19
sidebar_label: HTTP インターフェース
---

# HTTP インターフェース

HTTP インターフェースを使用すると、ClickHouse を任意のプラットフォームで任意のプログラミング言語から REST API の形式で利用できます。HTTP インターフェースはネイティブインターフェースよりも制限がありますが、言語サポートが優れています。

デフォルトでは、`clickhouse-server` は HTTP のためにポート 8123 でリスンしています（これは設定で変更可能です）。
HTTPS も有効化可能で、デフォルトポートは 8443 です。

パラメータなしで `GET /` リクエストを行うと、200 のレスポンスコードと [http_server_default_response](../operations/server-configuration-parameters/settings.md#http_server_default_response) に定義されたデフォルト値「Ok.」の文字列（行末に改行付き）が返されます。

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

また参照してください: [HTTP レスポンスコードの注意事項](#http_response_codes_caveats)。

ユーザーのオペレーティングシステムで `curl` コマンドが利用できない場合もあります。Ubuntu や Debian では、`sudo apt install curl` を実行してください。例を実行する前に、この[ドキュメント](https://curl.se/download.html)を参照してインストールしてください。

Web UI は次のURLでアクセス可能です: `http://localhost:8123/play`。

![Web UI](../images/play.png)

ヘルスチェックスクリプトでは `GET /ping` リクエストを使用します。このハンドラは常に「Ok.」（行末に改行付き）を返します。バージョン 18.12.13 から利用可能です。レプリカの遅延をチェックするには `/replicas_status` も参照してください。

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

リクエストをURLの `query` パラメータとして、またはPOSTとして送信します。または `query` パラメータにクエリの最初の部分を送り、残りをPOSTで送信します（後でこれが必要な理由を説明します）。URLのサイズはデフォルトで1 MiBに制限されており、これは `http_max_uri_size` 設定で変更可能です。

成功すると、200 のレスポンスコードとレスポンスボディに結果が返されます。
エラーが発生すると、500 のレスポンスコードとエラーの説明文がレスポンスボディに含まれます。

GET メソッドを使用する際は、`readonly` が設定されます。つまり、データを修正するクエリの場合は、POST メソッドのみを使用できます。クエリ自体を POST ボディまたは URL パラメータに送信できます。

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
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}

1
```

ご覧のとおり、`curl` はスペースを URL エスケープしなければならず、やや不便です。
`wget` はすべてを自動でエスケープしますが、保持接続 `keep-alive` や `Transfer-Encoding: chunked` を使う際に HTTP 1.1 でうまく動作しないため、使用をお勧めしません。

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

一部のクエリがパラメータとして送信され、一部が POST で送信されると、これらのデータ部分間に改行が挿入されます。
例（これは動作しません）:

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

デフォルトでは、データは [TabSeparated](formats.md#tabseparated) 形式で返されます。

クエリの FORMAT 句を使用して、他の任意の形式をリクエストできます。

または、`default_format` URL パラメータまたは `X-ClickHouse-Format` ヘッダーを使用して、TabSeparated 以外のデフォルト形式を指定することもできます。

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

`INSERT` クエリではデータの送信に POST メソッドが必要です。この場合、クエリの開始部分を URL パラメータに書き、POST を使用してデータを渡すことができます。挿入するデータは、たとえば MySQL のタブ区切りダンプかもしれません。このようにして、`INSERT` クエリは MySQL の `LOAD DATA LOCAL INFILE` を置き換えます。

**例**

テーブルの作成:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

おなじみの INSERT クエリを使用したデータ挿入:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

データをクエリと別に送信:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

任意のデータ形式を指定できます。「Values」形式は、INSERT INTO t VALUES と同じです:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

タブ区切りのダンプからデータを挿入するには、対応する形式を指定します:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

テーブル内容を読み出します。データは並列クエリ処理によりランダムな順序で出力されます:

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

テーブルを削除します。

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

データテーブルを返さない成功したリクエストの場合、空のレスポンスボディが返されます。


## 圧縮 {#compression}

大量のデータを送信する際のネットワークトラフィックを削減したり、圧縮済みのダンプを作成するために圧縮を使用できます。

データを送信する際に、ClickHouse の内部圧縮形式を使用できます。圧縮データは非標準の形式を持ち、`clickhouse-compressor` プログラムを使用して操作する必要があります。これは `clickhouse-client` パッケージとともにインストールされます。データ挿入の効率を高めるために、[http_native_compression_disable_checksumming_on_decompress](../operations/settings/settings.md#http_native_compression_disable_checksumming_on_decompress) 設定を使用してサーバー側のチェックサム検証を無効にできます。

URLに `compress=1` を指定すると、サーバーは送信するデータを圧縮します。URLに `decompress=1` を指定すると、サーバーは POST 方法で渡されたデータを解凍します。

また、[HTTP圧縮](https://en.wikipedia.org/wiki/HTTP_compression)を使用することもできます。ClickHouse は次の[圧縮方式](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens)をサポートしています:

- `gzip`
- `br`
- `deflate`
- `xz`
- `zstd`
- `lz4`
- `bz2`
- `snappy`

圧縮された `POST` リクエストを送信するには、リクエストヘッダー `Content-Encoding: compression_method` を追加します。
ClickHouse に応答を圧縮させるためには、[enable_http_compression](../operations/settings/settings.md#enable_http_compression) 設定で圧縮を有効化し、`Accept-Encoding: compression_method` ヘッダーをリクエストに追加してください。全ての圧縮方法に対するデータ圧縮レベルは [http_zlib_compression_level](../operations/settings/settings.md#http_zlib_compression_level) 設定で設定可能です。

:::info
一部の HTTP クライアントはデフォルトでサーバーからのデータを解凍する場合があり（`gzip` や `deflate`）、正しく圧縮設定を使用した場合でも解凍済みのデータを受け取ることがあります。
:::

**例**

``` bash
# 圧縮データをサーバーに送信する
$ echo "SELECT 1" | gzip -c | \
  curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

``` bash
# サーバーから圧縮されたデータアーカイブを受け取る
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" \
    -H 'Accept-Encoding: gzip' --output result.gz -d 'SELECT number FROM system.numbers LIMIT 3'
$ zcat result.gz
0
1
2
```

```bash
# サーバーから圧縮されたデータを受け取り、gunzipを使用して解凍されたデータを受け取る
$ curl -sS "http://localhost:8123/?enable_http_compression=1" \
    -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 3' | gunzip -
0
1
2
```

## デフォルトデータベース {#default-database}

`database` URL パラメータまたは `X-ClickHouse-Database` ヘッダーを使用してデフォルトデータベースを指定できます。

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

デフォルトでは、サーバー設定に登録されているデータベースがデフォルトデータベースとして使用されます。デフォルトでは「default」という名前のデータベースです。あるいは、常にテーブル名の前にドットを付けてデータベースを指定することもできます。

ユーザー名とパスワードは次の方法のいずれかで指定できます:

1.  HTTP 基本認証を使用します。例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

2.  `user` および `password` URL パラメータで指定する（*この方法は、パラメータがウェブプロキシによってログ記録され、ブラウザでキャッシュされる可能性があるため推奨されません*）。例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

3.  `X-ClickHouse-User` および `X-ClickHouse-Key` ヘッダーを使用します。例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

ユーザー名が指定されていない場合は、`default` 名が使用されます。パスワードが指定されていない場合は、空のパスワードが使用されます。
また、問い合わせの処理に関する任意の設定や設定プロファイル全体を URL パラメータで指定することもできます。例: http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1

詳細については、[設定](../operations/settings/index.md)セクションを参照してください。

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

その他のパラメータについては、「SET」セクションを参照してください。

同様に、HTTP プロトコルで ClickHouse セッションを使用することができます。これを行うには、`session_id` GET パラメータをリクエストに追加する必要があります。任意の文字列をセッション ID として使用できます。デフォルトでは、セッションは非アクティブ状態が 60 秒続くと終了します。このタイムアウトを変更するには、サーバー構成で `default_session_timeout` 設定を変更するか、リクエストに `session_timeout` GET パラメータを追加します。セッションステータスを確認するには、`session_check=1` パラメータを使用します。単一セッション内では、一度に一つのクエリしか実行できません。

クエリの進行状況についての情報を `X-ClickHouse-Progress` レスポンスヘッダーで受信できます。これを行うには、[send_progress_in_http_headers](../operations/settings/settings.md#send_progress_in_http_headers) を有効にします。ヘッダーシーケンスの例:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128","elapsed_ns":"662334"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128","elapsed_ns":"992334"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128","elapsed_ns":"1232334"}
```

可能なヘッダーフィールド:

- `read_rows` — 読み取った行数。
- `read_bytes` — バイト単位で読み取ったデータのボリューム。
- `total_rows_to_read` — 読み取るべき総行数。
- `written_rows` — 書き込まれた行数。
- `written_bytes` — バイト単位で書き込まれたデータのボリューム。

実行中のリクエストは、HTTP 接続が失われても自動的には停止しません。解析とデータフォーマットはサーバー側で実行され、ネットワークの使用が非効率的になる場合があります。
オプションの `query_id` パラメータはクエリ ID（任意の文字列）として渡すことができます。詳細については、「設定、replace_running_query」セクションを参照してください。

オプションの `quota_key` パラメータは、クォータキーとして（任意の文字列）渡すことができます。詳細については、「クォータ」セクションを参照してください。

HTTP インターフェースは、クエリのために外部データ（外部一時テーブル）を渡すことを可能にします。詳細については、「クエリ処理のための外部データ」セクションを参照してください。

## レスポンスバッファリング {#response-buffering}

サーバー側でレスポンスバッファリングを有効にできます。この目的のために `buffer_size` と `wait_end_of_query` URL パラメータが提供されています。
または、設定 `http_response_buffer_size` と `http_wait_end_of_query` を使用できます。

`buffer_size` は、サーバーメモリ内でバッファリングする結果のバイト数を決定します。結果本体がこのしきい値を超える場合、バッファは HTTP チャネルに書き込まれ、残りのデータは直接 HTTP チャネルに送信されます。

レスポンス全体がバッファリングされるようにするには、`wait_end_of_query=1` を設定します。この場合、メモリに保存されないデータは一時的なサーバーファイルにバッファリングされます。

例:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

バッファリングを使用して、レスポンスコードと HTTP ヘッダーがクライアントに送信された後にクエリ処理エラーが発生した場合の状況を回避します。この状況では、エラーメッセージがレスポンス本体の末尾に書き込まれ、クライアント側では解析段階でのみエラーを検出できます。

## クエリパラメータによるロール設定 {#setting-role-with-query-parameters}

これは ClickHouse 24.4 に追加された新機能です。

特定のシナリオでは、文自体を実行する前に付与されたロールを設定する必要がある場合があります。
しかし、`SET ROLE` と文を一緒に送信することは不可能です。マルチステートメントは許可されていません:

```
curl -sS "http://localhost:8123" --data-binary "SET ROLE my_role;SELECT * FROM my_table;"
```

この結果、次のエラーが発生します:

```
Code: 62. DB::Exception: Syntax error (Multi-statements are not allowed)
```

この制限を克服するために、`role` クエリパラメータを使用できます:

```
curl -sS "http://localhost:8123?role=my_role" --data-binary "SELECT * FROM my_table;"
```

これにより、文の前に `SET ROLE my_role` を実行するのと同等になります。

さらに、複数の `role` クエリパラメータを指定することも可能です:

```
curl -sS "http://localhost:8123?role=my_role&role=my_other_role" --data-binary "SELECT * FROM my_table;"
```

この場合、`?role=my_role&role=my_other_role` は文の前に `SET ROLE my_role, my_other_role` を実行するのと同様に機能します。

## HTTP レスポンスコードの注意事項 {#http_response_codes_caveats}

HTTP プロトコルの制限のため、HTTP 200 のレスポンスコードはクエリが成功したことを保証しません。

例を示します:

```
curl -v -Ss "http://localhost:8123/?max_block_size=1&query=select+sleepEachRow(0.001),throwIf(number=2)from+numbers(5)"
*   Trying 127.0.0.1:8123...
...
< HTTP/1.1 200 OK
...
Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(number, 2) :: 1) -> throwIf(equals(number, 2))
```

この動作の原因は HTTP プロトコルの特性によるものです。HTTP ヘッダーが最初に HTTP コード 200 で送信され、続いて HTTP 本体が送信され、エラーがプレーンテキストとして本体に注入されます。
この動作は、使用される形式に関係なく、`Native`、`TSV`、`JSON` のいずれであっても、エラーメッセージはレスポンスストリームの途中に常に挿入されます。
この問題を軽減するには、[レスポンスバッファリング](#response-buffering)を有効にしてください。この場合、HTTP ヘッダーの送信がクエリ全体が解決されるまで遅延します。
ただし、これでは問題が完全に解決されるわけではありません。結果が `http_response_buffer_size` に収まらなければならず、`send_progress_in_http_headers` のような他の設定がヘッダーの遅延に干渉する可能性があります。
すべてのエラーをキャッチする唯一の方法は、必要な形式を使用して解析する前に HTTP 本体を分析することです。

## パラメータを持つクエリ {#cli-queries-with-parameters}

パラメータを持つクエリを作成し、それらに対応する HTTP リクエストパラメータから値を渡すことができます。詳しくは [CLI 用のパラメータ付きクエリ](../interfaces/cli.md#cli-queries-with-parameters) を参照してください。

### 例 {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

### URL パラメータ内のタブ

クエリパラメータは「エスケープ」形式から解析されます。これには、`\N` としてヌルを明確に解析する可能性があるという利点があります。つまり、タブ文字は `\t`（または `\` とタブ）としてエンコードする必要があります。たとえば、次の例では `abc` と `123` の間に実際のタブが含まれており、入力文字列が2つの値に分割されます:

```bash
curl -sS "http://localhost:8123" -d "SELECT splitByChar('\t', 'abc      123')"
```

```response
['abc','123']
```

しかし、URLパラメータで実際のタブを `%09` を使用してエンコードしようとすると、正しく解析されません:

```bash
curl -sS "http://localhost:8123?param_arg1=abc%09123" -d "SELECT splitByChar('\t', {arg1:String})"
Code: 457. DB::Exception: Value abc	123 cannot be parsed as String for query parameter 'arg1' because it isn't parsed completely: only 3 of 7 bytes was parsed: abc. (BAD_QUERY_PARAMETER) (version 23.4.1.869 (official build))
```

URLパラメータを使用する場合、`\t` を `%5C%09` としてエンコードする必要があります。例えば:

```bash
curl -sS "http://localhost:8123?param_arg1=abc%5C%09123" -d "SELECT splitByChar('\t', {arg1:String})"
```

```response
['abc','123']
```

## 事前定義された HTTP インターフェース {#predefined_http_interface}

ClickHouse は HTTP インターフェースを介して特定のクエリをサポートしています。例えば、次のようにしてテーブルにデータを書き込むことができます:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse は、[Prometheus エクスポーター](https://github.com/ClickHouse/clickhouse_exporter)のようなサードパーティツールと簡単に統合できる事前定義された HTTP インターフェースもサポートしています。

例:

- まず、サーバー設定ファイルに次のセクションを追加します:

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

- Prometheus 形式でデータを直接リクエストすることができます:

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
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}
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

例からもわかるように、`http_handlers` が config.xml ファイルで設定されている場合、`http_handlers` には多くの `rules` を含めることができます。ClickHouse は受信したHTTPリクエストを事前定義された型と一致させ、一致すれば最初の処理が実行されます。そして、ClickHouse は一致が成功した場合に対応する事前定義されたクエリを実行します。

現在、`rule` は `method`、`headers`、`url`、`handler` を設定できます:
- `method` は HTTP リクエストのメソッド部分をマッチングする役割を持ちます。`method` は HTTP プロトコルでの[メソッド](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) の定義を完全に遵守しています。この設定はオプションです。設定ファイルに定義されていない場合、HTTP リクエストのメソッド部分とは一致しません。

- `url` は HTTP リクエストの URL 部分をマッチングする役割を持ち、[RE2](https://github.com/google/re2) の正規表現と互換性があります。この設定はオプションです。設定ファイルに定義されていない場合、HTTP リクエストの URL 部分とは一致しません。

- `headers` は HTTP リクエストのヘッダー部分をマッチングする役割を持ち、RE2 の正規表現と互換性があります。この設定はオプションです。設定ファイルに定義されていない場合、HTTP リクエストのヘッダー部分とは一致しません。

- `handler` はメインの処理部分を含みます。現在、`handler` は `type`、`status`、`content_type`、`http_response_headers`、`response_content`、`query`、`query_param_name` を設定できます。
    `type` は現在、次の3種類をサポートしています: [predefined_query_handler](#predefined_query_handler)、[dynamic_query_handler](#dynamic_query_handler)、[static](#static)。

    - `query` — `predefined_query_handler` タイプで使用され、ハンドラが呼び出されたときにクエリを実行します。

    - `query_param_name` — `dynamic_query_handler` タイプで使用され、HTTP リクエストパラメータ内の `query_param_name` 値に対応する値を抽出して実行します。

    - `status` — `static` タイプで使用され、レスポンスステータスコードを返します。

    - `content_type` — どのタイプでも使用可能です。レスポンスの[コンテンツタイプ](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)を返します。

    - `http_response_headers` — どのタイプでも使用可能です。レスポンスヘッダーのマップです。コンテンツタイプを設定するにも使用できます。

    - `response_content` — `static` タイプで使用され、クライアントに送信されるレスポンスコンテンツです。接頭辞 'file://' または 'config://' を使用する場合、クライアントに送信するコンテンツをファイルまたは設定から探します。

次に、異なる `type` に対する設定方法を説明します。

### predefined_query_handler {#predefined_query_handler}

`predefined_query_handler` は `Settings` および `query_params` 値を設定することをサポートしています。`predefined_query_handler` タイプで `query` を設定できます。

`query` の値は、`predefined_query_handler` の事前定義されたクエリであり、HTTP リクエストがマッチしたときに ClickHouse によって実行され、クエリの結果が返されます。これは必須の設定です。

次の例では、[max_threads](../operations/settings/settings.md#max_threads) および `max_final_threads` 設定の値を定義し、それらの設定が正常に設定されたかどうかをクエリで確認します。

:::注意
デフォルトの `handlers` である `query`、`play`、`ping` を保持するには、`<defaults/>` ルールを追加してください。
:::

例:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[regex:/query_param_with_url/(?P<name_1>[^/]+)]]></url>
        <methods>GET</methods>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[regex:(?P<name_2>[^/]+)]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                SELECT name, value FROM system.settings
                WHERE name IN ({name_1:String}, {name_2:String})
            </query>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_final_threads' 'http://localhost:8123/query_param_with_url/max_threads?max_threads=1&max_final_threads=2'
max_final_threads	2
max_threads	1
```

:::注意
1 つの `predefined_query_handler` には 1 つの `query` のみがサポートされています。
:::

### dynamic_query_handler {#dynamic_query_handler}

`dynamic_query_handler` では、クエリは HTTP リクエストのパラメータの形で書かれています。`predefined_query_handler` では、クエリは設定ファイルに書かれています。`dynamic_query_handler` で `query_param_name` を設定できます。

ClickHouse は HTTP リクエスト URL での `query_param_name` の値に対応する値を抽出して実行します。`query_param_name` のデフォルト値は `/query` です。これはオプションの設定です。設定ファイルに定義がない場合、パラメータは渡されません。

この機能を試すために、例では [max_threads](../operations/settings/settings.md#max_threads) および `max_final_threads` の値を定義し、設定が正常に設定されたかどうかをクエリします。

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
    <defaults/>
</http_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_final_threads=2&param_name_1=max_threads&param_name_2=max_final_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_final_threads   2
```

### static {#static}

`static` は [content_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)、[status](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) および `response_content` を返すことができます。`response_content` は指定されたコンテンツを返します。

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
                <http_response_headers>
                    <Content-Language>en</Content-Language>
                    <X-My-Custom-Header>43</X-My-Custom-Header>
                </http_response_headers>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
        <defaults/>
</http_handlers>
```

`http_response_headers` は `content_type` ではなく、コンテンツタイプを設定するために使用できます。

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <http_response_headers>
                    <Content-Type>text/html; charset=UTF-8</Content-Type>
                    <Content-Language>en</Content-Language>
                    <X-My-Custom-Header>43</X-My-Custom-Header>
                </http_response_headers>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
        <defaults/>
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
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

設定からクライアントに送信するコンテンツを探します。

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
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

ファイルからクライアントに送信するコンテンツを探します。

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <ETag>737060cd8c284d8af7ad3082f209582d</ETag>
                </http_response_headers>
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
                <http_response_headers>
                    <ETag>737060cd8c284d8af7ad3082f209582d</ETag>
                </http_response_headers>
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
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}
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
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```

## HTTP ストリーミング中に発生した例外での有効な JSON/XML 応答 {valid-output-on-exception-http-streaming}

HTTP 経由でのクエリ実行中、データの一部が既に送信されたときに例外が発生する可能性があります。通常、特定のデータ形式が使用されてデータを出力し、出力が指定されたデータ形式において無効になる場合でも、例外はプレーンテキストでクライアントに送信されます。
これを防ぐために、指定されたフォーマットで例外を記述する `http_write_exception_in_output_format` 設定（デフォルトで有効）が使用できます（現在、XML および JSON* 形式でサポートされています）。

例:

```bash
$ curl 'http://localhost:8123/?query=SELECT+number,+throwIf(number>3)+from+system.numbers+format+JSON+settings+max_block_size=1&http_write_exception_in_output_format=1'
{
	"meta":
	[
		{
			"name": "number",
			"type": "UInt64"
		},
		{
			"name": "throwIf(greater(number, 2))",
			"type": "UInt8"
		}
	],

	"data":
	[
		{
			"number": "0",
			"throwIf(greater(number, 2))": 0
		},
		{
			"number": "1",
			"throwIf(greater(number, 2))": 0
		},
		{
			"number": "2",
			"throwIf(greater(number, 2))": 0
		}
	],

	"rows": 3,

	"exception": "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(greater(number, 2) :: 2) -> throwIf(greater(number, 2)) UInt8 : 1'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 23.8.1.1)"
}
```

```bash
$ curl 'http://localhost:8123/?query=SELECT+number,+throwIf(number>2)+from+system.numbers+format+XML+settings+max_block_size=1&http_write_exception_in_output_format=1'
<?xml version='1.0' encoding='UTF-8' ?>
<result>
	<meta>
		<columns>
			<column>
				<name>number</name>
				<type>UInt64</type>
			</column>
			<column>
				<name>throwIf(greater(number, 2))</name>
				<type>UInt8</type>
			</column>
		</columns>
	</meta>
	<data>
		<row>
			<number>0</number>
			<field>0</field>
		</row>
		<row>
			<number>1</number>
			<field>0</field>
		</row>
		<row>
			<number>2</number>
			<field>0</field>
		</row>
	</data>
	<rows>3</rows>
	<exception>Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(greater(number, 2) :: 2) -> throwIf(greater(number, 2)) UInt8 : 1'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 23.8.1.1)</exception>
</result>
```

