---
description: 'ClickHouse コマンドラインクライアント インターフェイスのドキュメント'
sidebar_label: 'ClickHouse Client'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse Client'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse には、ClickHouse サーバー に対して直接 SQL クエリを実行できるネイティブなコマンドラインクライアントが用意されています。
対話型モード (その場でクエリを実行する場合) とバッチモード (スクリプトや自動化で使用する場合) の両方に対応しています。
クエリ結果はターミナルに表示することも、ファイルにエクスポートすることもでき、Pretty、CSV、JSON など、すべての ClickHouse 出力[フォーマット](formats.md)をサポートしています。

このクライアントは、進行状況バー、読み取った行数、処理したバイト数、クエリ実行時間を通じて、クエリ実行中の状況をリアルタイムで表示します。
[コマンドラインオプション](#command-line-options)と[設定ファイル](#configuration_files)の両方に対応しています。

<div id="install">
  ## インストール
</div>

ClickHouseをダウンロードするには、次を実行します。

```bash
curl https://clickhouse.com/ | sh
```

これもインストールするには、次を実行します：

```bash
sudo ./clickhouse install
```

インストール方法の詳細については、[Install ClickHouse](../getting-started/install/install.mdx)を参照してください。

異なるバージョンのクライアントとサーバー間でも互換性はありますが、古いクライアントでは一部の機能を利用できない場合があります。クライアントとサーバーには同じバージョンを使用することを推奨します。

<div id="run">
  ## 実行
</div>

:::note
ClickHouse をダウンロードしただけでインストールしていない場合は、`clickhouse-client` ではなく `./clickhouse client` を使用してください。
:::

ClickHouse サーバーに接続するには、次を実行します。

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

必要に応じて、追加の接続情報を指定します。

| Option                           | Description                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `--port <port>`                  | ClickHouse server が接続を受け付けるポートです。デフォルトのポートは 9440 (TLS) と 9000 (TLS なし) です。ClickHouse Client は HTTP(S) ではなくネイティブプロトコルを使用する点に注意してください。 |
| `-s [ --secure ]`                | TLS を使用するかどうかを指定します (通常は自動検出されます) 。                                                                                                  |
| `-u [ --user ] <username>`       | 接続に使用するデータベースユーザーです。デフォルトでは `default` ユーザーとして接続します。                                                                                  |
| `--password <password>`          | データベースユーザーのパスワードです。接続のパスワードは設定ファイルで指定することもできます。パスワードを指定しない場合、クライアントによって入力を求められます。                                                    |
| `-c [ --config ] <path-to-file>` | ClickHouse Client の設定ファイルの場所です。デフォルトのいずれかの場所にない場合に指定します。[Configuration Files](#configuration_files) を参照してください。                       |
| `--connection <name>`            | [設定ファイル](#connection-credentials) にある、事前設定済みの接続情報の名前です。                                                                              |

コマンドラインオプションの完全な一覧については、[Command Line Options](#command-line-options) を参照してください。

<div id="connecting-cloud">
  ### ClickHouse Cloud への接続
</div>

ClickHouse Cloud サービスの接続情報は、ClickHouse Cloud コンソールで確認できます。接続するサービスを選択し、**Connect** をクリックします。

<Image img={cloud_connect_button} size="md" alt="ClickHouse Cloud サービスの接続ボタン" />

<br />

<br />

**Native** を選択すると、接続情報が `clickhouse-client` コマンドの例とともに表示されます。

<Image img={connection_details_native} size="md" alt="ClickHouse Cloud の Native TCP 接続情報" />

<div id="connection-credentials">
  ### 設定ファイルに接続情報を保存する
</div>

1 つ以上の ClickHouse サーバー の接続情報を[設定ファイル](#configuration_files)に保存できます。

形式は次のとおりです。

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

詳しくは、[設定ファイルに関するセクション](#configuration_files)を参照してください。

:::note
クエリ構文に集中できるよう、以降の例では接続情報 (`--host`、`--port` など) を省略しています。コマンドを使用する際は、忘れずに追加してください。
:::

<div id="interactive-mode">
  ## 対話型モード
</div>

<div id="using-interactive-mode">
  ### 対話型モードを使用する
</div>

ClickHouse を対話型モードで実行するには、次のコマンドを実行します。

```bash
clickhouse-client
```

これにより、対話形式で SQL クエリを入力できる Read-Eval-Print Loop (REPL) が開きます。
接続が完了すると、クエリを入力するためのプロンプトが表示されます。

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

対話型モードでは、デフォルトの出力フォーマットは `PrettyCompact` です。
フォーマットは、クエリの `FORMAT` 句で変更するか、コマンドラインオプション `--format` で指定できます。
Vertical format を使用するには、`--vertical` を使うか、クエリの末尾に `\G` を指定します。
このフォーマットでは各値が1行ずつ表示されるため、列数の多いテーブルで便利です。

対話型モードでは、デフォルトでは入力した内容は `Enter` を押すと実行されます。
クエリの末尾にセミコロンは必要ありません。

クライアントは `-m, --multiline` パラメータを付けて起動できます。
複数行のクエリを入力するには、改行の前にバックスラッシュ `\` を入力します。
`Enter` を押すと、クエリの次の行を入力するよう求められます。
クエリを実行するには、末尾にセミコロンを付けて `Enter` を押します。

ClickHouse Client は `replxx` (`readline` に類似) をベースにしているため、使い慣れたキーボードショートカットを使用でき、履歴も保持されます。
履歴はデフォルトで `~/.clickhouse-client-history` に書き込まれます。

クライアントを終了するには、`Ctrl+D` を押すか、クエリの代わりに次のいずれかを入力します。

* `exit` または `exit;`
* `quit` または `quit;`
* `q`、`Q` または `:q`
* `logout` または `logout;`

<div id="getting-help">
  ### ヘルプを参照する
</div>

クライアントを離れずに、任意の関数、テーブルエンジン、データ型、フォーマット、設定、その他のシステムコンポーネントのドキュメントを参照できます。`help` に続けて名前を入力してください (`/help`、`man`、`/man` も同様に使用できます) :

```text
help domainWithoutWWW
```

ルックアップでは大文字と小文字を区別せず、[`system.documentation`](../operations/system-tables/documentation.md) テーブルを検索します。一致したドキュメントはターミナル上で Markdown からレンダリングされ、太字/斜体のテキスト、表、シンタックスハイライトされたコードブロック付きで表示されます。複数のコンポーネントで同じ名前が使われている場合 (たとえば `file` は関数でもありテーブルエンジンでもあります) 、それらがすべて表示されます。

完全一致するものがない場合、クライアントは似た名前 (タイプミスを考慮) と、その単語に言及しているドキュメントを持つコンポーネントを一覧表示します。

```text
help maxx_threads
```

`help` だけを入力すると、簡単な使い方の概要が表示されます。

<div id="processing-info">
  ### クエリ処理に関する情報
</div>

クエリの処理中、クライアントには次の情報が表示されます。

1. Progress。既定では、1 秒あたり最大 10 回更新されます。
   すぐに終わるクエリでは、進行状況が表示される前に処理が完了することがあります。
2. デバッグ用の、パース後に整形されたクエリ。
3. 指定したフォーマットでの結果。
4. 結果の行数、経過時間、およびクエリ処理の平均速度。
   すべてのデータ量は非圧縮データを基準としています。

時間のかかるクエリは、`Ctrl+C` を押すことでキャンセルできます。
ただし、サーバーがリクエストを中止するまで、しばらく待つ必要があります。
クエリは、特定の段階ではキャンセルできません。
待たずにもう一度 `Ctrl+C` を押すと、クライアントは終了します。

ClickHouse Client では、クエリ実行用に外部データ (外部一時テーブル) を渡すことができます。
詳しくは、[クエリ処理用の外部データ](../engines/table-engines/special/external-data.md) のセクションを参照してください。

<div id="cli_aliases">
  ### 別名
</div>

REPL 内では、次の別名を使用できます。

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - 直前のクエリを繰り返す

<div id="keyboard_shortcuts">
  ### キーボードショートカット
</div>

* `Alt (Option) + Shift + e` - 現在のクエリでエディタを開きます。使用するエディタは環境変数 `EDITOR` で指定できます。デフォルトでは `vim` が使用されます。
* `Alt (Option) + #` - 行をコメントアウトします。
* `Ctrl + r` - 履歴をあいまい検索します。

利用可能なすべてのキーボードショートカットの一覧は、[replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262)で確認できます。

:::tip
MacOS でメタキー (Option) が正しく動作するように設定するには、次のようにします。

iTerm2: Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key に移動し、Esc+ をクリックします
:::

<div id="batch-mode">
  ## バッチモード
</div>

<div id="using-batch-mode">
  ### バッチモードを使う
</div>

ClickHouse Client を対話形式で使う代わりに、バッチモードで実行できます。
バッチモードでは、ClickHouse は単一のクエリを実行するとすぐに終了します。対話型のプロンプトやループはありません。

次のように単一のクエリを指定できます。

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

`--query` コマンドラインオプションも使用できます。

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

`stdin` からクエリを渡せます：

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

`messages` テーブルが存在する場合、コマンドラインからデータを挿入することもできます。

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

`--query` が指定されている場合、すべての入力は改行文字を挟んでリクエストの末尾に追加されます。

<div id="cloud-example">
  ### リモートのClickHouseサービスにCSVファイルを挿入する
</div>

この例では、サンプルデータセットのCSVファイル `cell_towers.csv` を、`default` データベース内の既存の `cell_towers` テーブルに挿入します:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### コマンドラインからデータを挿入する例
</div>

コマンドラインからデータを挿入する方法はいくつかあります。
以下の例では、バッチモードを使って、2行のCSVデータをClickHouseテーブルに挿入します。

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

以下の例では、`cat <<_EOF` でヒアドキュメントが始まり、再度 `_EOF` が現れるまでの内容をすべて読み込み、その後それを出力します。

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

以下の例では、`cat` を使って file.csv の内容を stdout に出力し、その出力を入力として `clickhouse-client` にパイプで渡します。

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

バッチモードでは、デフォルトのデータ[フォーマット](formats.md)は `TabSeparated` です。
フォーマットは、上記の例のようにクエリの `FORMAT` 句で設定できます。

<div id="cli-queries-with-parameters">
  ## パラメータ付きクエリ
</div>

クエリにパラメータを指定し、コマンドラインオプションでその値を渡すことができます。
これにより、クライアント側で特定の動的な値を使ってクエリをフォーマットする必要がなくなります。
例えば:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

[対話型セッション](#interactive-mode)内でパラメータを設定することもできます。

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### クエリ構文
</div>

クエリでは、コマンドラインパラメータで指定する値を、次の形式で波かっこ内に記述します。

```sql
{<name>:<data type>}
```

| パラメータ       | 説明                                                                                                                                                                                                                                                                                                                                        |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | プレースホルダーの識別子です。対応するコマンドラインオプションは `--param_<name> = value` です。                                                                                                                                                                                                                                                                             |
| `data type` | パラメータの[データ型](../sql-reference/data-types/index.md)です。 <br /><br />たとえば、`(integer, ('string', integer))` のようなデータ構造には、`Tuple(UInt8, Tuple(String, UInt8))` というデータ型を指定できます ([integer](../sql-reference/data-types/int-uint.md) 型には他の型も使用できます) 。 <br /><br />また、テーブル名、データベース名、カラム名をパラメータとして渡すこともできます。その場合は、データ型として `Identifier` を使用する必要があります。 |

<div id="cli-queries-with-parameters-examples">
  ### 例
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## AI を活用した SQL 生成
</div>

ClickHouse Client には、自然言語での説明から SQL クエリを生成する AI 支援機能が組み込まれています。この機能を使うと、SQL の深い知識がなくても複雑なクエリを作成できます。

`OPENAI_API_KEY` または `ANTHROPIC_API_KEY` のいずれかの環境変数が設定されていれば、AI 支援はそのまま利用できます。さらに高度な設定については、[設定](#ai-sql-generation-configuration) セクションを参照してください。

<div id="ai-sql-generation-usage">
  ### 使用方法
</div>

AI SQL生成を使用するには、自然言語のクエリの先頭に `??` を付けます。

```bash
:) ?? show all users who made purchases in the last 30 days
```

AI は次のことを行います。

1. データベースのスキーマを自動的に解析します
2. 検出されたテーブルとカラムに基づいて適切な SQL を生成します
3. 生成されたクエリをすぐに実行します

<div id="cli-queries-with-parameters-examples">
  ### 例
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### 設定
</div>

AI SQL generationを利用するには、ClickHouse Clientの設定ファイルでAIプロバイダーを設定する必要があります。OpenAI、Anthropic、またはOpenAI互換のAPIサービスを使用できます。

<div id="ai-sql-generation-fallback">
  #### 環境変数ベースのフォールバック
</div>

設定ファイルで AI の設定が指定されていない場合、ClickHouse Client は自動的に環境変数の使用を試みます。

1. まず `OPENAI_API_KEY` 環境変数を確認します
2. 見つからない場合は、`ANTHROPIC_API_KEY` 環境変数を確認します
3. どちらも見つからない場合、AI 機能は無効になります

これにより、設定ファイルを用意しなくてもすばやくセットアップできます。

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### 設定ファイル
</div>

AI の設定をより細かく制御するには、以下の場所にある ClickHouse Client の設定ファイルで構成します。

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (`XDG_CONFIG_HOME` が設定されていない場合は `~/.config/clickhouse/config.xml`)  (XML フォーマット)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (`XDG_CONFIG_HOME` が設定されていない場合は `~/.config/clickhouse/config.yaml`)  (YAML フォーマット)
* `~/.clickhouse-client/config.xml` (XML フォーマット、従来の場所)
* `~/.clickhouse-client/config.yaml` (YAML フォーマット、従来の場所)
* または、`--config-file` で任意の場所を指定します

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- 必須: API キー（または環境変数で設定） -->
            <api_key>your-api-key-here</api_key>

            <!-- 必須: プロバイダーの種類（openai、anthropic） -->
            <provider>openai</provider>

            <!-- 使用するモデル（デフォルトはプロバイダーによって異なります） -->
            <model>gpt-4o</model>

            <!-- 任意: OpenAI互換サービス用のカスタム API エンドポイント -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- スキーマ探索の設定 -->
            <enable_schema_access>true</enable_schema_access>

            <!-- 生成パラメータ -->
            <!-- 任意: temperature は、ここで設定した場合にのみモデルに送信されます。
                 一部のモデルはこのパラメータを受け付けないため、デフォルトでは省略されます。 -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- 任意: カスタムのシステムプロンプト -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # 必須: API キー（または環境変数で設定）
      api_key: your-api-key-here

      # 必須: プロバイダーの種類（openai、anthropic）
      provider: openai

      # 使用するモデル
      model: gpt-4o

      # 任意: OpenAI互換サービス用のカスタム API エンドポイント
      # base_url: https://openrouter.ai/api

      # スキーマアクセスを有効化 - AI がデータベース/テーブル情報をクエリできるようにします
      enable_schema_access: true

      # 生成パラメータ
      # temperature はここで設定した場合にのみモデルに送信され、デフォルトでは省略されます
      # 一部のモデルはこのパラメータを受け付けないためです。
      # temperature: 0.0    # ランダム性を制御します（0.0 = 決定論的）
      max_tokens: 1000      # 最大レスポンス長
      timeout_seconds: 30   # リクエストのタイムアウト
      max_steps: 10         # スキーマ探索の最大ステップ数

      # 任意: カスタムのシステムプロンプト
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**OpenAI互換 API の使用 (例: OpenRouter) :**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**最小構成の例:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### パラメータ
</div>

<details>
  <summary>パラメータ</summary>

  * `api_key` - AI サービス用の API キーです。環境変数で設定されている場合は省略できます。
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * 注: 設定ファイル内の API キーは環境変数より優先されます
  * `provider` - AI プロバイダ: `openai` または `anthropic`
    * 省略した場合は、利用可能な環境変数に基づいて自動的にフォールバックします
</details>

<details>
  <summary>モデル設定</summary>

  * `model` - 使用するモデル (デフォルト: プロバイダ固有)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo` など
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229` など
    * OpenRouter: `anthropic/claude-3.5-sonnet` のようなモデル名を使用します
</details>

<details>
  <summary>接続設定</summary>

  * `base_url` - OpenAI 互換サービス用のカスタム API エンドポイント (任意)
  * `timeout_seconds` - リクエストのタイムアウト時間 (秒)  (デフォルト: `30`)
</details>

<details>
  <summary>スキーマ探索</summary>

  * `enable_schema_access` - AI がデータベースのスキーマを探索できるようにします (デフォルト: `true`)
  * `max_steps` - スキーマ探索時のツール呼び出しステップの最大数 (デフォルト: `10`)
</details>

<details>
  <summary>生成パラメータ</summary>

  * `temperature` - ランダム性を制御します。0.0 = 決定論的、1.0 = 創造的。デフォルトでは省略され、明示的に設定した場合にのみモデルへ送信されます。一部のモデルはこのパラメータを受け付けないためです。
  * `max_tokens` - 応答の最大長 (トークン単位)  (デフォルト: `1000`)
  * `system_prompt` - AI へのカスタム指示 (任意)
</details>

<div id="ai-sql-generation-how-it-works">
  ### 動作の仕組み
</div>

AI SQL ジェネレーターは、複数のステップで処理を行います。

<VerticalStepper headerLevel="list">
  1. **スキーマ検出**

  AI は組み込みツールを使ってデータベースを調べます

  * 利用可能なデータベースを一覧表示します
  * 関連するデータベース内のテーブルを検出します
  * `CREATE TABLE` ステートメントを使ってテーブル構造を確認します

  2. **クエリ生成**

  検出したスキーマに基づいて、AI は次のような SQL を生成します。

  * 自然言語で指定した意図に沿っている
  * 正しいテーブル名とカラム名を使用している
  * 適切な JOIN と集計を適用している

  3. **実行**

  生成された SQL は自動的に実行され、結果が表示されます
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### 制限事項
</div>

* 有効なインターネット接続が必要です
* API の利用には、AIプロバイダーによるレート制限や費用が適用されます
* 複雑なクエリでは、複数回の調整が必要になる場合があります
* AI がアクセスできるのは、実際のデータではなく、スキーマ情報の読み取り専用アクセスのみです

<div id="ai-sql-generation-security">
  ### セキュリティ
</div>

* APIキーが ClickHouseサーバーに送信されることはありません
* AIが参照するのはスキーマ情報 (テーブル名/カラム名と型) のみで、実際のデータは参照しません
* 生成されるすべてのクエリは、既存のデータベース権限に従います

<div id="connection_string">
  ## 接続文字列
</div>

<div id="ai-sql-generation-usage">
  ### 使用方法
</div>

ClickHouse Client は、[MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/)、[PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)、[MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri) と同様に、接続文字列を使用して ClickHouseサーバー に接続することもできます。構文は次のとおりです。

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| コンポーネント (すべて省略可能)  | 説明                                                                                                | デフォルト            |
| ------------------ | ------------------------------------------------------------------------------------------------- | ---------------- |
| `user`             | データベースのユーザー名。                                                                                     | `default`        |
| `password`         | データベースユーザーのパスワード。`:` が指定され、パスワードが空の場合、クライアントはユーザーのパスワード入力を求めます。                                   | -                |
| `hosts_and_ports`  | ホストと省略可能なポートの一覧 `host[:port] [, host:[port]], ...`。                                               | `localhost:9000` |
| `database`         | データベース名。                                                                                          | `default`        |
| `query_parameters` | キー・バリューのペアの一覧 `param1=value1[,&param2=value2], ...`。一部のパラメーターでは値の指定は不要です。パラメーター名と値は大文字と小文字を区別します。 | -                |

<div id="connection-string-notes">
  ### 注記
</div>

username、password、または database を接続文字列で指定した場合、`--user`、`--password`、または `--database` で指定することはできません (逆も同様です) 。

host 部分には、hostname、IPv4 アドレス、または IPv6 アドレスを指定できます。
IPv6 アドレスは `[]` で囲んでください。

```text
clickhouse://[2001:db8::1234]
```

接続文字列には複数のホストを含めることができます。
ClickHouse Client は、これらのホストに定義順 (左から右) で接続を試みます。
いずれかのホストとの接続が確立されると、残りのホストへの接続は試行されません。

接続文字列は、`clickHouse-client` の最初の引数として指定する必要があります。
接続文字列は、`--host` と `--port` を除き、任意の数の他の[コマンドラインオプション](#command-line-options)と組み合わせて使用できます。

`query_parameters` では、次のキーを使用できます。

| Key               | Description                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------------- |
| `secure` (or `s`) | 指定すると、クライアントは安全な接続 (TLS) でサーバーに接続します。[コマンドラインオプション](#command-line-options)の `--secure` を参照してください。 |

**パーセントエンコーディング**

次のパラメータ内の非 US-ASCII 文字、スペース、および特殊文字は、[パーセントエンコード](https://en.wikipedia.org/wiki/URL_encoding)する必要があります。

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### 例
</div>

`localhost` のポート9000に接続し、クエリ `SELECT 1` を実行します。

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

`localhost` に、ユーザー `john`、パスワード `secret`、ホスト `127.0.0.1`、ポート `9000` を指定して接続します

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

`default` ユーザーとして、`localhost` (IPv6 アドレス `[::1]`) 、ポート `9000` に接続します。

```bash
clickhouse-client clickhouse://[::1]:9000
```

`localhost` のポート9000にマルチラインモードで接続します。

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

`localhost` に、ユーザー `default` としてポート 9000 で接続します。

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

`localhost` のポート9000に接続し、デフォルトでは `my_database` データベースを使用します。

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

`localhost` のポート 9000 に接続し、接続文字列で指定した `my_database` データベースを既定として使用し、短縮形の `s` パラメータでセキュア接続を行います。

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

デフォルトのホストに、デフォルトのポート、default ユーザー、および default データベースを使用して接続します。

```bash
clickhouse-client clickhouse:
```

`my_user` ユーザーとして、パスワードなしで、デフォルトのポートを使用してデフォルトのホストに接続します。

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

メールアドレスをユーザー名として使用し、`localhost` に接続します。`@` 記号は `%40` にパーセントエンコードされます。

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

次の 2 つのホストのいずれかに接続します: `192.168.1.15`、`192.168.1.25`。

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## Query ID の形式
</div>

対話型モードでは、ClickHouse Client は各クエリの Query ID を表示します。デフォルトでは、ID は次の形式になります。

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

カスタムフォーマットは、設定ファイル内の `query_id_formats` タグで指定できます。フォーマット文字列内の `{query_id}` プレースホルダーは、クエリ ID に置き換えられます。このタグ内では複数のフォーマット文字列を指定できます。
この機能を使うと、クエリのプロファイリングに役立つ URL を生成できます。

**例**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

上記の設定では、クエリIDは次のフォーマットで表示されます:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## 設定ファイル
</div>

ClickHouse Client は、次のうち最初に見つかったファイルを使用します。

* `-c [ -C, --config, --config-file ]` パラメータで指定されたファイル。
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (`XDG_CONFIG_HOME` が設定されていない場合は `~/.config/clickhouse/config.[xml|yaml|yml]`)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

サンプル設定ファイルについては、ClickHouse リポジトリ内の [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml) を参照してください。

<Tabs>
  <TabItem value="xml" label="XML" default>
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
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## 環境変数オプション
</div>

ユーザー名、パスワード、ホストは、環境変数 `CLICKHOUSE_USER`、`CLICKHOUSE_PASSWORD`、`CLICKHOUSE_HOST` を使って設定できます。
コマンドライン引数 `--user`、`--password`、`--host`、または [接続文字列](#connection_string) が指定されている場合は、環境変数よりそちらが優先されます。

<div id="command-line-options">
  ## コマンドラインオプション
</div>

すべてのコマンドラインオプションは、コマンドラインで直接指定することも、[設定ファイル](#configuration_files)でデフォルト値として指定することもできます。

<div id="command-line-options-general">
  ### 一般オプション
</div>

| Option                                              | Description                                                                                        | Default              |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | クライアントの設定ファイルが既定の場所にない場合は、その設定ファイルの場所を指定します。[Configuration Files](#configuration_files) を参照してください。 | -                    |
| `--help`                                            | 使用方法の概要を表示して終了します。`--verbose` と組み合わせると、クエリ設定を含む利用可能なすべてのオプションを表示します。                               | -                    |
| `--history_file <path-to-file>`                     | コマンド履歴を含むファイルのパスです。                                                                                | -                    |
| `--history_max_entries`                             | 履歴ファイルに保存する最大エントリ数です。                                                                              | `1000000` (100万)     |
| `--prompt <prompt>`                                 | カスタムプロンプトを指定します。                                                                                   | サーバーの `display_name` |
| `--verbose`                                         | 出力をより詳細にします。                                                                                       | -                    |
| `-V [ --version ]`                                  | バージョンを表示して終了します。                                                                                   | -                    |

<div id="command-line-options-connection">
  ### 接続オプション
</div>

| Option                               | Description                                                                                                                                                                                                                                                              | Default                                                                                                |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `--connection <name>`                | 設定ファイルに事前設定された接続情報の名前です。[接続資格情報](#connection-credentials)を参照してください。                                                                                                                                                                                                      | -                                                                                                      |
| `-d [ --database ] <database>`       | この接続で既定として使用するデータベースを選択します。                                                                                                                                                                                                                                              | サーバー設定の現在のデータベース (デフォルトでは `default`)                                                                   |
| `-h [ --host ] <host>`               | 接続先の ClickHouseサーバーのホスト名です。ホスト名、IPv4 アドレス、IPv6 アドレスのいずれも指定できます。複数の引数を使って複数のホストを渡すこともできます。                                                                                                                                                                                | `localhost`                                                                                            |
| `--jwt <value>`                      | 認証に JSON Web Token (JWT) を使用します。<br /><br />サーバーの JWT 認可は ClickHouse Cloud でのみ利用できます。                                                                                                                                                                                    | -                                                                                                      |
| `login`                              | IdP 経由で認証するため、デバイス grant の OAuthフローを開始します。<br /><br />ClickHouse Cloud のホストでは OAuth 変数は自動的に推論されます。それ以外の場合は、`--oauth-url`、`--oauth-client-id`、`--oauth-audience` を指定する必要があります。                                                                                            | -                                                                                                      |
| `--no-warnings`                      | クライアントがサーバーに接続したときに `system.warnings` の警告を表示しないようにします。                                                                                                                                                                                                                   | -                                                                                                      |
| `--no-server-client-version-message` | クライアントがサーバーに接続したときに、サーバーとクライアントのバージョン不一致メッセージを表示しません。                                                                                                                                                                                                                    | -                                                                                                      |
| `--password <password>`              | データベースユーザーのパスワードです。設定ファイルで接続用のパスワードを指定することもできます。パスワードを指定しない場合、クライアントが入力を求めます。                                                                                                                                                                                            | -                                                                                                      |
| `--port <port>`                      | サーバーが接続を受け付けるポートです。デフォルトのポートは 9440 (TLS) と 9000 (TLS なし) です。<br /><br />注: クライアントは HTTP(S) ではなくネイティブプロトコルを使用します。                                                                                                                                                         | `--secure` が指定されている場合は `9440`、それ以外は `9000` です。ホスト名が `.clickhouse.cloud` で終わる場合は、常に `9440` がデフォルトになります。 |
| `-s [ --secure ]`                    | TLS を使用するかどうかを指定します。<br /><br />ポート 9440 (デフォルトのセキュアポート) または ClickHouse Cloud に接続する場合は自動的に有効になります。<br /><br />[設定ファイル](#configuration_files) で CA証明書を設定する必要がある場合があります。利用可能な設定項目は、[サーバー側のTLS設定](../operations/server-configuration-parameters/settings.md#openssl) と同じです。 | ポート 9440 または ClickHouse Cloud に接続する場合は自動的に有効化されます                                                      |
| `--ssh-key-file <path-to-file>`      | サーバー認証に使用する SSH 秘密鍵を含むファイルです。                                                                                                                                                                                                                                            | -                                                                                                      |
| `--ssh-key-passphrase <value>`       | `--ssh-key-file` で指定した SSH 秘密鍵のパスフレーズです。                                                                                                                                                                                                                                 | -                                                                                                      |
| `--tls-sni-override <server name>`   | TLS を使用する場合、ハンドシェイク時に渡すサーバー名 (SNI) です。                                                                                                                                                                                                                                   | `-h` または `--host` で指定したホスト。                                                                            |
| `-u [ --user ] <username>`           | 接続に使用するデータベースユーザーです。                                                                                                                                                                                                                                                     | `default`                                                                                              |

:::note
クライアントは、`--host`、`--port`、`--user`、`--password` オプションの代わりに、[接続文字列](#connection_string) もサポートしています。
:::

<div id="command-line-options-query">
  ### クエリオプション
</div>

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | [パラメータ付きクエリ](#cli-queries-with-parameters)のパラメータに対する置換用の値。                                                                                                                                                                                                                                                                                                                                                                      |
| `-q [ --query ] <query>`        | バッチモードで実行するクエリ。複数回指定することも (`--query "SELECT 1" --query "SELECT 2"`) 、セミコロン区切りで複数のクエリをまとめて 1 回だけ指定することもできます (`--query "SELECT 1; SELECT 2;"`) 。後者の場合、`VALUES` 以外のフォーマットを使用する `INSERT` クエリは、空行で区切る必要があります。 <br /><br />単一のクエリは、パラメータなしでも指定できます: `clickhouse-client "SELECT 1"` <br /><br />`--queries-file` と同時には使用できません。                                                                                                         |
| `--queries-file <path-to-file>` | クエリを含むファイルのパス。`--queries-file` は複数回指定できます。たとえば `--queries-file queries1.sql --queries-file queries2.sql` のように指定します。 <br /><br />`--query` と同時には使用できません。                                                                                                                                                                                                                                                                         |
| `-m [ --multiline ]`            | 指定すると、複数行のクエリを入力できるようになります (Enter を押してもクエリは送信されません) 。クエリはセミコロンで終えた場合にのみ送信されます。                                                                                                                                                                                                                                                                                                                                                  |
| `--inline-insert-data`          | データをネイティブフォーマットのブロックに変換する代わりに、`INSERT ... VALUES` (および他のインラインフォーマット) をクエリテキスト内でそのまま送信します。サーバー側でインラインデータを解析するため、テーブル構造やカラムのデフォルト値をクライアントに送り返す往復変換を省けます。これにより、ネイティブプロトコル経由で多数の小さな `INSERT` を行う場合のパフォーマンスが向上することがあります。[`send_table_structure_on_insert_with_inline_data`](/ja/operations/settings/settings#send_table_structure_on_insert_with_inline_data) は自動的に `0` に設定されます。インラインデータや外部データ (stdin または `INFILE` から) とは組み合わせて使用できません。 |

<div id="command-line-options-query-settings">
  ### クエリ設定
</div>

クエリ設定は、たとえば client でコマンドラインオプションとして指定できます。

```bash
$ clickhouse-client --max_threads 1
```

設定の一覧は、[Settings](../operations/settings/settings.md) を参照してください。

<div id="command-line-options-formatting">
  ### フォーマットオプション
</div>

| オプション                             | 説明                                                                                                                                                                                                                                                                                                                    | デフォルト                                    |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `-f [ --format ] <format>`        | 指定したフォーマットで結果を出力します。 <br /><br />対応フォーマットの一覧については、[入力データと出力データのフォーマット](formats.md)を参照してください。                                                                                                                                                                                                                          | `TabSeparated`                           |
| `--pager <command>`               | すべての出力をこのコマンドにパイプします。通常は `less` (たとえば、列数の多い結果セットを表示するには `less -S`) や同様のコマンドを使用します。                                                                                                                                                                                                                                    | -                                        |
| `-E [ --vertical ]`               | 結果の出力に [Vertical format](/ja/interfaces/formats/Vertical) を使用します。これは `–-format Vertical` と同じです。このフォーマットでは各値が別々の行に表示されるため、列数の多いテーブルを表示する際に便利です。                                                                                                                                                                           | -                                        |
| `--echo [ <bool> ]`               | 実行前に各クエリを表示します。省略可能なブール値を指定できます。                                                                                                                                                                                                                                                                                      | 対話型モードでは `true`、非対話型 (バッチ) モードでは `false` |
| `--echo-formatted [ <bool> ]`     | 表示するクエリを整形します。省略可能なブール値を指定できます。                                                                                                                                                                                                                                                                                       | 対話型モードでは `true`、非対話型 (バッチ) モードでは `false` |
| `--echo-query-id [ <bool> ]`      | 実行前にクエリ ID を表示します。省略可能なブール値を指定できます。                                                                                                                                                                                                                                                                                   | 対話型モードでは `true`、非対話型 (バッチ) モードでは `false` |
| `--echo-query-separator <string>` | 整形して表示するクエリの前にこの区切り文字を表示します (`--echo-formatted` が必要) 。これにより、入力したクエリと整形後の表示とを区別しやすくなります。                                                                                                                                                                                                                               | 空 (無効)                                   |
| `--highlight [ --hilite ] <bool>` | コマンドプロンプトおよび表示するクエリの構文ハイライトをトグルします。                                                                                                                                                                                                                                                                                   | `true`                                   |
| `--hints <bool>`                  | カーソルが入力の末尾にあるとき、最も一致する候補について入力中のオートコンプリートヒント (インラインの「ゴースト」テキスト) を表示します。ヒントは Up/Down (または Ctrl-Up/Ctrl-Down) で移動でき、インラインヒントは Tab または Right で受け入れます。`Enter` は、ヒントが明示的に選択されている場合にのみそれを受け入れ、それ以外ではクエリを実行します。`Tab` では従来の補完リストも開きます。`--highlight` (ヒントの表示には色が必要) と候補生成機能が必要です (そのため `--disable_suggestion` を指定しても無効になります) 。 | `true`                                   |

<div id="command-line-options-execution-details">
  ### 実行の詳細
</div>

| Option                           | Description                                                                                                                                                                                                                                                                                | Default                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- |
| `--chime [N]`                    | クエリの実行時間が少なくとも `N` 秒に達した場合、完了時 (成功時・エラー時の両方) に `BEL` 制御文字を `stderr` に書き込みます。`stderr` が端末 (TTY) に接続されている場合にのみ出力されます。`stderr` をリダイレクトすると (例: `2>err.log`) 出力は抑止されますが、`stdout` をリダイレクトしても (例: `> result.tsv`) 抑止されません。値を指定せずに `--chime` を指定した場合は、デフォルトのしきい値が使われます。無効にするには `--chime 0` を設定します。 | `5` Seconds                             |
| `--enable-progress-table-toggle` | 制御キー (Space) を押して進捗テーブルをトグルできるようにします。進捗テーブルの表示が有効な対話型モードでのみ適用されます。                                                                                                                                                                                                                         | `enabled`                               |
| `--hardware-utilization`         | 進捗バーにハードウェア使用率の情報を表示します。                                                                                                                                                                                                                                                                   | -                                       |
| `--memory-usage`                 | 指定した場合、非対話型モードでメモリ使用量を `stderr` に出力します。 <br /><br />設定可能な値: <br />• `none` - メモリ使用量を出力しない <br />• `default` - バイト数を出力する <br />• `readable` - 可読形式でメモリ使用量を出力する                                                                                                                              | -                                       |
| `--print-profile-events`         | `ProfileEvents` パケットを出力します。                                                                                                                                                                                                                                                                | -                                       |
| `--progress`                     | クエリ実行の進捗を出力します。 <br /><br />設定可能な値: <br />• `tty\|on\|1\|true\|yes` - 対話型モードで端末に出力します <br />• `err` - 非対話型モードで `stderr` に出力します <br />• `off\|0\|false\|no` - 進捗の出力を無効にします                                                                                                                  | 対話型モードでは `tty`、非対話型 (batch) モードでは `off` |
| `--progress-table`               | クエリ実行中に変化するメトリクスを含む進捗テーブルを出力します。 <br /><br />設定可能な値: <br />• `tty\|on\|1\|true\|yes` - 対話型モードで端末に出力します <br />• `err` - 非対話型モードで `stderr` に出力します <br />• `off\|0\|false\|no` - 進捗テーブルを無効にします                                                                                                | 対話型モードでは `tty`、非対話型 (batch) モードでは `off` |
| `--stacktrace`                   | 例外のスタックトレースを出力します。                                                                                                                                                                                                                                                                         | -                                       |
| `-t [ --time ]`                  | 非対話型モードでクエリの実行時間を `stderr` に出力します (ベンチマーク用) 。                                                                                                                                                                                                                                              | -                                       |