---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 17
toc_title: "\u30B3\u30DE\u30F3\u30C9\u30E9\u30A4\u30F3"
---

# コマンドライン {#command-line-client}

ClickHouseスネイティブコマンドラインのクライアント: `clickhouse-client`. クライアン 詳細については、 [設定](#interfaces_cli_configuration).

[設置](../getting-started/index.md) それから `clickhouse-client` パッケージとコマンドでそれを実行 `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

異なるクライアントとサーババージョンと互換性が、一部機能が利用できない古いクライアント. サーバーアプリと同じバージョンのクライアン 古いバージョンのクライアントを使用しようとすると、サーバー, `clickhouse-client` メッセージを表示する:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## 使い方 {#cli_usage}

このクラ バッチモードを使用するには、 ‘query’ パラメータ、または送信データに ‘stdin’ （それはそれを確認します ‘stdin’ は端末ではありません)、またはその両方。 HTTPインターフェイスと同様に、 ‘query’ パラメータとデータの送信先 ‘stdin’ リクエストはリクエストの連結である。 ‘query’ パラメータ、ラインフィード、および ‘stdin’. これは、大規模な挿入クエリに便利です。

クライアントを使用してデータを挿入する例:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

バッチモードでは、デフォルトのデータ形式はtabseparatedです。 形式は、クエリのformat句で設定できます。

既定では、単一のクエリのみをバッチモードで処理できます。 aから複数のクエリを作成するには “script,” を使用 `--multiquery` パラメータ。 これはINSERT以外のすべてのクエリで機能します。 クエリ結果は、追加の区切り文字なしで連続して出力されます。 同様に、多数のクエリを処理するには、以下を実行します ‘clickhouse-client’ 各クエリについて。 それは起動に数十ミリ秒かかることに注意してください ‘clickhouse-client’ プログラム。

対話モードでは、クエリを入力できるコマンドラインが表示されます。

もし ‘multiline’ クエリを実行するには、Enterキーを押します。 クエリの最後にセミコロンは必要ありません。 複数行のクエリを入力するには、円記号を入力します `\` ラインフィードの前に。 Enterキーを押すと、クエリの次の行を入力するように求められます。

複数行が指定されている場合:クエリを実行するには、クエリをセミコロンで終了し、enterキーを押します。 入力された行の最後にセミコロンが省略された場合は、クエリの次の行を入力するように求められます。

単一のクエリのみが実行されるので、セミコロンの後のすべてが無視されます。

指定できます `\G` 代わりに、またはセミコロンの後に。 これは縦書式を示します。 この形式では、各値は別々の行に印刷されます。 この珍しい機能は、MySQL CLIとの互換性のために追加されました。

コマンドラインは以下に基づきます ‘replxx’ （に似て ‘readline’). つまり、使い慣れたキーボードショートカットを使用し、履歴を保持します。 歴史はに書かれています `~/.clickhouse-client-history`.

デフォルトでは、使用される形式はprettycompactです。 クエリのformat句で形式を変更するか、次のように指定できます `\G` クエリの最後に、次のコマンドを使用します `--format` または `--vertical` コマンドラインの引数、またはクライアント構成ファイルの使用。

クライアントを終了するには、ctrl+d(またはctrl+c)を押すか、クエリの代わりに次のいずれかを入力します: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

が処理クエリー、クライアントを示し:

1.  （デフォルトでは）毎秒せいぜい10回updatedされている進捗、。 クイッククエリの場合、進行状況が表示される時間がないことがあります。
2.  デバッグのための、解析後の書式付きクエリ。
3.  指定された形式の結果。
4.  結果の行数、経過時間、およびクエリ処理の平均速度。

ただし、サーバーが要求を中止するのを少し待つ必要があります。 特定の段階でクエリをキャンセルすることはできません。 待たずにctrl+cをもう一度押すと、クライアントは終了します。

コマン 詳細については、以下を参照してください “External data for query processing”.

### クエリパラメータ {#cli-queries-with-parameters}

を作成でき、クエリパラメータおよびパスの値からのお知らクライアントアプリケーション. これを避けるフォーマットのクエリが特定の動的価値観にクライアント側で行われます。 例えば:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### クエリの構文 {#cli-queries-with-parameters-syntax}

通常どおりにクエリを書式設定し、次の形式でアプリパラメーターからクエリに渡す値を中かっこで囲みます:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [データ型](../sql-reference/data-types/index.md) アプリのパラメータ値の。 たとえば、次のようなデータ構造 `(integer, ('string', integer))` を持つことができ `Tuple(UInt8, Tuple(String, UInt8))` デー [整数](../sql-reference/data-types/int-uint.md) タイプ）。

#### 例えば {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## 設定 {#interfaces_cli_configuration}

パラメータを渡すには `clickhouse-client` (すべてのパラメータの既定値がある):

-   コマンドラインから

    コマンドラインオプションは、構成ファイルの既定値と設定を上書きします。

-   構成ファイル。

    構成ファイルの設定は、デフォルト値を上書きします。

### コマンドラインオプ {#command-line-options}

-   `--host, -h` -– The server name, ‘localhost’ デフォルトでは。 名前またはIPv4アドレスまたはIPv6アドレスを使用できます。
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ デフォルトでは）。
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as ‘–format=Vertical’. この形式では、各値は別の行に印刷されます。
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ 非対話モードでは。
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection.
-   `--param_<name>` — Value for a [クエリパラメータ](#cli-queries-with-parameters).

### 設定ファイル {#configuration_files}

`clickhouse-client` 次の最初の既存のファイルを使用します:

-   で定義される `--config-file` パラメータ。
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

設定ファイルの例:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[元の記事](https://clickhouse.tech/docs/en/interfaces/cli/) <!--hide-->
