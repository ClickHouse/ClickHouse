---
slug: /ja/interfaces/cli
sidebar_position: 17
sidebar_label: コマンドラインクライアント
title: コマンドラインクライアント
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_native.md';

## clickhouse-client

ClickHouseはネイティブのコマンドラインクライアント、`clickhouse-client`を提供しています。このクライアントはコマンドラインオプションと設定ファイルをサポートしています。詳細は[設定](#interfaces_cli_configuration)をご覧ください。

`clickhouse-client`パッケージから[インストール](../getting-started/install.md)し、`clickhouse-client`コマンドで実行します。

```bash
$ clickhouse-client
ClickHouse client version 20.13.1.5273 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 20.13.1.

:)
```

異なるクライアントとサーバーバージョンは互換性がありますが、一部の機能は古いクライアントでは使用できない可能性があります。クライアントとサーバーアプリの同じバージョンを使用することをお勧めします。古いバージョンのクライアントを使用しようとすると、サーバーは`clickhouse-client`が以下のメッセージを表示します：

```response
ClickHouse client version is older than ClickHouse server.
It may lack support for new features.
```

## 使用法 {#cli_usage}

クライアントは、インタラクティブモードと非インタラクティブ（バッチ）モードで使用できます。

### 接続の詳細を集める
<ConnectionDetails />

### インタラクティブ

ClickHouse CloudサービスまたはTLSおよびパスワードを使用する任意のClickHouseサーバーに接続するには、インタラクティブに`--secure`、ポート9440、およびユーザー名とパスワードを指定します：

```bash
clickhouse-client --host <HOSTNAME> \
                  --secure \
                  --port 9440 \
                  --user <USERNAME> \
                  --password <PASSWORD>
```

セルフマネージドのClickHouseサーバーに接続するには、そのサーバーの詳細が必要です。TLSが使用されるかどうか、ポート番号、パスワードもすべて設定可能です。ClickHouse Cloudの例を出発点として使用してください。

### バッチ

バッチモードを使用するには、`query`パラメーターを指定するか、`stdin`にデータを送信します（`stdin`がターミナルでないことを確認します）。両方を行うこともできます。HTTPインターフェースに似ていますが、`query`パラメーターを使用し`stdin`にデータを送信する場合、リクエストは`query`パラメーター、改行、`stdin`のデータの連結です。これは大きなINSERTクエリに便利です。

クライアントを使用してデータを挿入する例：

#### リモートClickHouseサービスにCSVファイルを挿入する

この例はTLSとパスワードを使用するClickHouse Cloudまたは任意のClickHouseサーバーに適しています。この例ではサンプルデータセットのCSVファイル`cell_towers.csv`を既存の`default`データベース内の`cell_towers`テーブルに挿入します：

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --secure \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

:::note
クエリの構文に集中するために、他の例では接続の詳細（`--host`、`--port`など）を省略しています。コマンドを試す際にはこれらを追加してください。
:::

#### データを挿入する三つの方法

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

### 注意事項

バッチモードでは、デフォルトのデータフォーマットはTabSeparatedです。クエリのFORMAT句でフォーマットを設定できます。

デフォルトでは、バッチモードでは単一のクエリしか処理できません。“スクリプト”から複数のクエリを作成するには、`--multiquery`パラメータを使用します。これはINSERTを除くすべてのクエリに有効です。クエリ結果は連続して無分別に出力されます。同様に、大量のクエリを処理するには、各クエリに対して‘clickhouse-client’を実行することができます。‘clickhouse-client’プログラムの起動に数十ミリ秒かかることに注意してください。

インタラクティブモードでは、クエリを入力するためのコマンドラインが得られます。

‘multiline’が指定されていない場合（デフォルト）：クエリを実行するにはEnterキーを押します。クエリの最後にセミコロンは必要ありません。複数行のクエリを入力するには、改行前にバックスラッシュ`\`を入力します。Enterキーを押した後、次の行のクエリを入力するように求められます。

multilineが指定されている場合：クエリを実行するには、最後にセミコロンを付けてEnterキーを押します。入力した行の最後にセミコロンを付けなかった場合、次の行のクエリを入力するように求められます。

単一のクエリのみが実行されるため、セミコロン以降のすべては無視されます。

`\G`をセミコロンとして、または後に指定することができます。これはVerticalフォーマットを示します。このフォーマットでは、各値が別々の行に印刷されるため、幅広いテーブルに便利です。この珍しい機能はMySQL CLIとの互換性のために追加されました。

コマンドラインは‘replxx’（‘readline’に似ています）に基づいています。つまり、慣れ親しんだキーボードショートカットを使用し、履歴を保持します。履歴は`~/.clickhouse-client-history`に書き込まれます。

デフォルトでは、使用されるフォーマットはPrettyCompactです。クエリのFORMAT句でフォーマットを変更するか、クエリの最後に`\G`を指定したり、コマンドラインで`--format`または`--vertical`引数を使用したり、クライアントの設定ファイルを使用したりすることでフォーマットを変更できます。

クライアントを終了するには、Ctrl+Dを押すか、クエリの代わりに次のいずれかを入力します：「exit」、「quit」、「logout」、「exit;」、「quit;」、「logout;」、「q」、「Q」、「:q」

クエリを処理する際に、クライアントは次を表示します：

1.  プログレスは、1秒間に最大10回（デフォルト）更新されます。クエリが迅速な場合、プログレスは表示されない可能性があります。
2.  デバッグのために、解析後のフォーマットされたクエリ。
3.  指定されたフォーマットでの結果。
4.  結果の行数、経過時間、クエリ処理の平均速度。すべてのデータ量は圧縮されていないデータを指します。

長いクエリをキャンセルするには、Ctrl+Cを押します。ただし、サーバーがリクエストを中断するのを待つ必要があります。一部のステージではクエリをキャンセルすることはできません。待たずに2回目のCtrl+Cを押すと、クライアントは終了します。

コマンドラインクライアントは外部データ（外部一時テーブル）をクエリに渡すことができます。詳細については、「クエリ処理のための外部データ」セクションをご覧ください。

### パラメータ付きクエリ {#cli-queries-with-parameters}

クライアントアプリケーションから値を渡してパラメータ付きクエリを作成できます。これにより、クライアント側で特定の動的値を持つクエリをフォーマットする必要がなくなります。 例えば：

```bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

インタラクティブセッション内からパラメータを設定することもできます：
```bash
$ clickhouse-client -nq "
  SET param_parName='[1, 2]';
  SELECT {parName:Array(UInt16)}"
```

#### クエリ構文 {#cli-queries-with-parameters-syntax}

通常どおりにクエリをフォーマットし、次にアプリのパラメータからクエリに渡したい値を以下の形式で中括弧に入れます：

```sql
{<name>:<data type>}
```

- `name` — プレースホルダー識別子。コンソールクライアントでは`--param_<name> = value`としてアプリパラメータで使用されます。
- `data type` — アプリパラメータ値の[データ型](../sql-reference/data-types/index.md)。例えば、`(integer, ('string', integer))`のようなデータ構造は、`Tuple(UInt8, Tuple(String, UInt8))`データ型を持つことができます（他の[整数](../sql-reference/data-types/int-uint.md)タイプも使用できます）。テーブル、データベース、カラム名をパラメータとして渡すことも可能で、その場合`Identifier`をデータ型として使用する必要があります。

#### 例 {#example}

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

## 設定 {#interfaces_cli_configuration}

`clickhouse-client`にパラメータを渡すことができ（すべてのパラメータにはデフォルト値があります）、以下の方法を使用します：

- コマンドラインから

    コマンドラインオプションは、設定ファイル内でのデフォルト値と設定を上書きします。

- 設定ファイルから。

    設定ファイルの設定はデフォルト値を上書きします。

### コマンドラインオプション {#command-line-options}

- `--host, -h` – サーバー名（デフォルトは 'localhost'）。名前またはIPv4もしくはIPv6アドレスを使用できます。
- `--port` – 接続先のポート。デフォルト値: 9000。HTTPインターフェースとネイティブインターフェースは異なるポートを使用することに注意してください。
- `--user, -u` – ユーザー名。デフォルト値: default。
- `--password` – パスワード。デフォルト値: 空文字列。
- `--ask-password` - ユーザーにパスワードの入力を促します。
- `--query, -q` – 非インタラクティブモード使用時の処理クエリ。`--query`は複数回指定できます（例：`--query "SELECT 1" --query "SELECT 2"`）。`--queries-file`とは同時に使用できません。
- `--queries-file` – 実行するクエリが記述されたファイルパス。`--queries-file`は複数回指定できます（例：`--queries-file  queries1.sql --queries-file  queries2.sql`）。`--query`とは同時に使用できません。
- `--multiquery, -n` – 指定された場合、セミコロンで区切られた複数のクエリを`--query`オプションの後にリストできます。便利なことに、`--query`を省略し、`--multiquery`の後にクエリを直接渡すことも可能です。
- `--multiline, -m` – 指定された場合、複数行のクエリを許可します（Enterでクエリを送信しません）。
- `--database, -d` – 現在のデフォルトデータベースを選択します。デフォルト値: サーバー設定からの現在のデータベース（デフォルトは ‘default’）。
- `--format, -f` – 結果を出力するためのデフォルトのフォーマットを指定します。
- `--vertical, -E` – 指定された場合、結果を出力するためにデフォルトで[Verticalフォーマット](../interfaces/formats.md#vertical)を使用します。これは`–format=Vertical`と同じです。このフォーマットでは、各値が別々の行に印刷されるため、幅広いテーブルの表示に役立ちます。
- `--time, -t` – 指定された場合、非インタラクティブモードでクエリ実行時間を‘stderr’に出力します。
- `--memory-usage` – 指定された場合、非インタラクティブモードでメモリ使用量を‘stderr’に出力します。可能な値は：'none' - メモリ使用量を出力しない、'default' - バイト数を出力、'readable' - メモリ使用量を人間が読みやすい形式で出力。
- `--stacktrace` – 指定された場合、例外が発生したときにスタックトレースも出力します。
- `--config-file` – 設定ファイルの名前。
- `--secure` – 指定された場合、セキュア接続（TLS）を介してサーバーに接続します。CA証明書を[設定ファイル](#configuration_files)で設定する必要があるかもしれません。利用可能な設定は[サーバー側のTLS設定](../operations/server-configuration-parameters/settings.md#openssl)と同じです。
- `--history_file` — コマンド履歴を含むファイルのパス。
- `--history_max_entries` — 履歴ファイルの最大エントリー数。デフォルト値：1 000 000。
- `--param_<name>` — [パラメータ付きクエリ](#cli-queries-with-parameters)のための値。
- `--hardware-utilization` — プログレスバーでハードウェア利用情報を表示。
- `--print-profile-events` – `ProfileEvents`パケットを出力。
- `--profile-events-delay-ms` – `ProfileEvents`パケットの出力間隔（-1 - 合計のみ出力、0 - 単一パケットごとに出力）。
- `--jwt` – 指定された場合、JSON Web Tokenを介して認証を有効にします。サーバーJWT認証はClickHouse Cloudでのみ利用可能です。
- `--progress` – クエリ実行の進捗を表示。可能な値: 'tty|on|1|true|yes' - インタラクティブモードでTTYに出力; 'err' - 非インタラクティブモードでSTDERRに出力; 'off|0|false|no' - 進捗表示を無効にします。デフォルト：インタラクティブモードではTTY、非インタラクティブモードでは無効。
- `--progress-table` – クエリ実行中に変化するメトリクスを持つ進捗テーブルを表示。可能な値: 'tty|on|1|true|yes' - インタラクティブモードでTTYに出力; 'err' - 非インタラクティブモードでSTDERRに出力; 'off|0|false|no' - 進捗テーブルを無効にします。デフォルト：インタラクティブモードではTTY、非インタラクティブモードでは無効。
- `--enable-progress-table-toggle` – 進捗テーブル印刷が有効なインタラクティブモードで、コントロールキー（スペース）を押して進捗テーブルを切り替えることを有効にします。デフォルト：'true'。

`--host`、`--port`、`--user`、`--password`オプションの代わりに、ClickHouseクライアントは接続文字列もサポートしています（次のセクションを参照）。

## エイリアス {#cli_aliases}

- `\l` - SHOW DATABASES
- `\d` - SHOW TABLES
- `\c <DATABASE>` - USE DATABASE
- `.` - 最後のクエリを繰り返す

## ショートキー {#shortkeys_aliases}

- `Alt (Option) + Shift + e` - 現在のクエリをエディタで開く。環境変数を設定できます（デフォルトはvim）。
- `Alt (Option) + #` - 行をコメントアウト。
- `Ctrl + r` - あいまい履歴検索。

:::tip
MacOSでメタキー（Option）の正しい動作を設定するには：

iTerm2: Preferences -> Profile -> Keys -> Left Option keyへ行き、Esc+をクリックします。
:::

使用可能なすべてのショートキーの完全なリストは[replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262)をご覧ください。

## 接続文字列 {#connection_string}

clickhouse-clientはまた、`MongoDB`や`PostgreSQL`、`MySQL`と似た接続文字列を使用してclickhouseサーバーに接続することをサポートしています。以下の構文で接続文字列を記述します：

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

ここで

- `user` - （オプション）ユーザー名,
- `password` - （オプション）ユーザーパスワード。`:`が指定され、パスワードが空白ならば、クライアントはユーザーにパスワードを入力するように促します。
- `hosts_and_ports` - （オプション）ホストとポートのリスト `host[:port] [, host:[port]], ...`,
- `database` - （オプション）データベース名,
- `query_parameters` - （オプション）キーと値のペアのリスト `param1=value1[,&param2=value2], ...`。一部のパラメータには値が必要ありません。パラメータ名と値は大文字小文字を区別します。

ユーザーが指定されていない場合、`default`ユーザーがパスワードなしで使用されます。  
ホストが指定されていない場合、`localhost`が使用されます。  
ポートが指定されていない場合、デフォルトで`9000`が使用されます。  
データベースが指定されていない場合、`default`データベースが使用されます。

ユーザー名、パスワード、またはデータベースが接続文字列で指定されている場合、`--user`、`--password`、または`--database`で指定することはできません（その逆も同様です）。

ホストコンポーネントは、ホスト名とIPアドレスのどちらかを指定できます。IPv6アドレスを指定するには、角括弧内に記載します：

```text
clickhouse://[2001:db8::1234]
```

URIでは複数のホストへの接続が可能です。接続文字列には複数のホストを含めることができます。ClickHouse-clientはこれらのホストに順に接続を試みます（つまり、左から右へ）。接続が確立されると、残りのホストへの接続は行われません。

接続文字列はclickhouse-clientの最初の引数として指定する必要があります。接続文字列は任意の他の[command-line-options](#command-line-options)と組み合わせて使用できますが、`--host/-h`および`--port`は除きます。

`query_parameters`コンポーネントに許可されるキーは以下の通りです：

- `secure`またはショートハンド` s` - 値なし。指定された場合、クライアントはセキュア接続（TLS）を介してサーバーに接続します。[コマンドラインオプション](#command-line-options)の`secure`を参照してください

### パーセントエンコーディング {#connection_string_uri_percent_encoding}

`user`、`password`、`hosts`、`database`、`query parameters`内の非ASCII、スペース、特殊文字は[パーセントエンコーディング](https://en.wikipedia.org/wiki/URL_encoding)する必要があります。

### 例 {#connection_string_examples}

ポート9000でlocalhostに接続し、クエリ`SELECT 1`を実行します。

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

ユーザー`john`、パスワード`secret`、ホスト`127.0.0.1`、ポート`9000`を使用してlocalhostに接続します。

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

デフォルトユーザーを使用し、ホストにIPV6アドレス`[::1]`、ポート`9000`を使用してlocalhostに接続します。

```bash
clickhouse-client clickhouse://[::1]:9000
```

ポート9000を使用し、複数行モードでlocalhostに接続します。

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

ユーザー`default`を使用してポート9000でlocalhostに接続します。

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

ポート9000でlocalhostに接続し、`my_database`データベースを使用します。

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

ショートカット's' URIパラメータを使用して、接続文字列で指定された`my_database`データベースとセキュア接続を使用してlocalhostに接続します。

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

デフォルトのホスト、デフォルトのポート、デフォルトユーザー、デフォルトのデータベースに接続します。

```bash
clickhouse-client clickhouse:
```

デフォルトホスト、デフォルトポートを使用し、ユーザー`my_user`を使用して接続し、パスワードなしで接続します。

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

ユーザー名としてメールアドレスを使用してlocalhostに接続します。'@' シンボルはパーセントエンコードされ、'%40'になります。

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

指定されたホストの一つに接続します：`192.168.1.15`、`192.168.1.25`。

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

### 設定ファイル {#configuration_files}

`clickhouse-client`は以下の最初に存在するファイルを使用します：

- `--config-file`パラメータで定義された。
- `./clickhouse-client.xml`, `.yaml`, `.yml`
- `~/.clickhouse-client/config.xml`, `.yaml`, `.yml`
- `/etc/clickhouse-client/config.xml`, `.yaml`, `.yml`

設定ファイルの例：

```xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>true</secure>
    <openSSL>
      <client>
        <caConfig>/etc/ssl/cert.pem</caConfig>
      </client>
    </openSSL>
</config>
```

または、YAML形式で同じ設定：

```yaml
user: username
password: 'password'
secure: true
openSSL:
  client:
    caConfig: '/etc/ssl/cert.pem'
```

### クエリIDフォーマット {#query-id-format}

インタラクティブモードでは`clickhouse-client`は各クエリのクエリIDを表示します。デフォルトでは、IDは次のようにフォーマットされています：

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

カスタムフォーマットは設定ファイル内の`query_id_formats`タグで指定できます。フォーマット文字列内の`{query_id}`プレースホルダーはクエリのIDに置き換えられます。タグ内に複数のフォーマット文字列を指定することができます。この機能は、クエリのプロファイリングを促進するURLを生成するために使用できます。

**例**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

上記の設定が適用されると、クエリのIDは次のように表示されます：

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```
