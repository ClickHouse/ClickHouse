---
sidebar_label: JavaScript
sidebar_position: 4
keywords: [clickhouse, js, javascript, nodejs, web, browser, cloudflare, workers, client, connect, integrate]
slug: /ja/integrations/javascript
description: The official JS client for connecting to ClickHouse.
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# ClickHouse JS

ClickHouseに接続するための公式JSクライアントです。  
クライアントはTypeScriptで書かれており、クライアントの公開APIにタイピングを提供します。

依存関係ゼロで、最大限のパフォーマンスを発揮するよう最適化されており、様々なClickHouseのバージョンや構成（オンプレミスのシングルノード、オンプレミスクラスター、ClickHouse Cloud）でテストされています。

異なる環境に対応する二つのバージョンのクライアントが提供されています：
- `@clickhouse/client` - Node.js専用
- `@clickhouse/client-web` - ブラウザ（Chrome/Firefox）、Cloudflare workers対応

TypeScriptを使用する際は、[インラインインポートとエクスポート構文](https://www.typescriptlang.org/docs/handbook/release-notes/typescript-4-5.html#type-modifiers-on-import-names)を有効にするために、少なくとも[バージョン4.5](https://www.typescriptlang.org/docs/handbook/release-notes/typescript-4-5.html)であることを確認してください。

クライアントのソースコードは[ClickHouse-JS GitHubリポジトリ](https://github.com/ClickHouse/clickhouse-js)で確認できます。

## 環境要件（Node.js）

クライアントを実行するためには、Node.jsが環境で利用可能である必要があります。  
クライアントは、すべての[維持されている](https://github.com/nodejs/release#readme)Node.jsリリースと互換性があります。

Node.jsのバージョンがEnd-Of-Lifeに近づいた場合、そのバージョンのサポートはクライアントから削除されます。これは古くなり、安全ではないと考えられるためです。

現在のNode.jsバージョンのサポート:

| Node.js version | Supported?  |
|-----------------|-------------|
| 22.x            | ✔           |
| 20.x            | ✔           |
| 18.x            | ✔           |
| 16.x            | Best effort |

## 環境要件（Web）

クライアントのWebバージョンは、最新のChrome/Firefoxブラウザで公式にテストされており、たとえばReact/Vue/AngularアプリケーションやCloudflare workersに依存関係として使用できます。

## インストール

最新の安定版Node.jsクライアントバージョンをインストールするには、次のコマンドを実行します：

```sh
npm i @clickhouse/client
```

Web版のインストール：

```sh
npm i @clickhouse/client-web
```

## ClickHouseとの互換性

| Client version | ClickHouse |
|----------------|------------|
| 1.5.0          | 23.3+      |

クライアントはおそらく古いバージョンでも動作するでしょう。ただし、これはベストエフォートのサポートであり、保証はできません。ClickHouseのバージョンが23.3よりも古い場合、[ClickHouseのセキュリティポリシー](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md)を参照し、アップグレードを検討してください。

## 例

クライアントリポジトリ内の[例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples)でクライアント使用の様々なシナリオをカバーすることを目指しています。

概観は[例のREADME](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/README.md#overview)で確認できます。

例や以下のドキュメントで不明な点や不足している情報があれば、[お問い合わせ](./js.md#contact-us)ください。

# クライアントAPI

特に明示されていない限り、ほとんどの例はNode.jsとWeb版のクライアントの両方に互換性があります。

#### クライアントインスタンスの作成

必要に応じて多くのクライアントインスタンスを`createClient`ファクトリで作成できます。

```ts
import { createClient } from '@clickhouse/client' // または '@clickhouse/client-web'

const client = createClient({
  /* configuration */
})
```

ESMモジュールがサポートされていない環境では、CJS構文を使うこともできます：

```ts
const { createClient } = require('@clickhouse/client');

const client = createClient({
  /* configuration */
})
```

クライアントインスタンスは、インスタンス化時に[事前設定](./js.md#configuration)することができます。

#### 設定

クライアントインスタンスを作成する際、以下の接続設定を調整できます：

- **url?: string** - ClickHouseインスタンスURL。デフォルト値: `http://localhost:8123`。参考: [URL設定ドキュメント](./js.md#url-configuration)。
- **pathname?: string** - ClickHouse URLに追加するオプションのパス名。デフォルト値: `''`。参考: [パス名付きプロキシドキュメント](./js.md#proxy-with-a-pathname)。
- **request_timeout?: number** - リクエストタイムアウト（ミリ秒）。デフォルト値: `30_000`。
- **compression?: { response?: boolean; request?: boolean }** - 圧縮を有効にする。[圧縮ドキュメント](./js.md#compression)
- **username?: string** - リクエストを行うユーザーの名前。デフォルト値: `default`。
- **password?: string** - ユーザーパスワード。デフォルト: `''`。
- **application?: string** - Node.jsクライアントを使用するアプリケーションの名前。デフォルト値: `clickhouse-js`。
- **database?: string** - 使用するデータベース名。デフォルト値: `default`
- **clickhouse_settings?: ClickHouseSettings** - すべてのリクエストに適用されるClickHouse設定。デフォルト値: `{}`。
- **log?: { LoggerClass?: Logger, level?: ClickHouseLogLevel }** - 内部クライアントログの設定。[ログドキュメント](./js.md#logging-nodejs-only)
- **session_id?: string** - 各リクエストに送信されるオプションのClickHouseセッションID。
- **keep_alive?: { enabled?: boolean }** - Node.jsおよびWebバージョンでデフォルトで有効。
- **http_headers?: Record<string, string>** - ClickHouseリクエストのための追加のHTTPヘッダ。参考: [認証付きリバースプロキシドキュメント](./js.md#reverse-proxy-with-authentication)

#### Node.js特有の設定パラメータ

- **max_open_connections?: number** - ホストごとに許可される最大接続ソケット数。デフォルト値: `10`。
- **tls?: { ca_cert: Buffer, cert?: Buffer, key?: Buffer }** - TLS証明書を設定する。[TLSドキュメント](./js.md#tls-certificates-nodejs-only)
- **keep_alive?: { enabled?: boolean, idle_socket_ttl?: number }** - [Keep Aliveドキュメント](./js.md#keep-alive-configuration-nodejs-only)参照
- **http_agent?: http.Agent | https.Agent** - (エクスペリメンタル) クライアント用のカスタムHTTPエージェント。[HTTPエージェントドキュメント](./js.md#custom-httphttps-agent-experimental-nodejs-only)
- **set_basic_auth_header?: boolean** - (エクスペリメンタル) ベーシック認証資格情報で`Authorization`ヘッダを設定する。デフォルト値: `true`。参考: [この設定のHTTPエージェントドキュメントでの使用法](./js.md#custom-httphttps-agent-experimental-nodejs-only)

### URLの設定

:::important
URL設定は常にハードコーディングされた値を上書きし、その場合には警告がログに記録されます。
:::

クライアントインスタンスパラメータの多くをURLで設定することが可能です。URLのフォーマットは`http[s]://[username:password@]hostname:port[/database][?param1=value1&param2=value2]`です。ほとんどの場合、特定のパラメータの名前は、設定オプションインターフェース内のそのパスを反映していますが、いくつかの例外があります。サポートされているパラメータは次のとおりです：

| Parameter                                   | Type                                                              |
| ------------------------------------------- | ----------------------------------------------------------------- |
| `pathname`                                  | 任意の文字列。                                                 |
| `application_id`                            | 任意の文字列。                                                 |
| `session_id`                                | 任意の文字列。                                                 |
| `request_timeout`                           | 0以上の数値。                                                 |
| `max_open_connections`                      | 0より大きい0以上の数値。                                    |
| `compression_request`                       | boolean。 詳細は下記参照 [1]。                                   |
| `compression_response`                      | boolean。                                                          |
| `log_level`                                 | 許可される値: `OFF`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`。   |
| `keep_alive_enabled`                        | boolean。                                                          |
| `clickhouse_setting_*` や `ch_*`            | 以下参照 [2]。                                                    |
| `http_header_*`                             | 以下参照 [3]。                                                    |
| (Node.jsのみ) `keep_alive_idle_socket_ttl`  | 0以上の数値。                                                     |

[1] boolean の場合、有効な値は `true`/`1` および `false`/`0` になります。

[2] `clickhouse_setting_` または `ch_` で始まるパラメータは、この接頭辞が削除され、残りがクライアントの `clickhouse_settings` に追加されます。たとえば、 `?ch_async_insert=1&ch_wait_for_async_insert=1` は次のとおりです：

```ts
createClient({
  clickhouse_settings: {
    async_insert: 1,
    wait_for_async_insert: 1,
  },
})
```

注意: `clickhouse_settings` に対するboolean 値はURL内で `1`/`0` として渡されるべきです。

[3] [2]と同様ですが、 `http_header` 設定用です。たとえば、 `?http_header_x-clickhouse-auth=foobar` は次のように等価です：

```ts
createClient({
  http_headers: {
    'x-clickhouse-auth': 'foobar',
  },
})
```

### 接続

#### 接続詳細を収集

<ConnectionDetails />

#### 接続概観

クライアントはHTTP(s)プロトコル経由で接続を実装しています。RowBinaryのサポートは進行中です。[関連issue](https://github.com/ClickHouse/clickhouse-js/issues/216)を参照してください。

以下の例は、ClickHouse Cloudに対する接続を設定する方法を示しています。`host`（プロトコルとポートを含む）と`password`の値が環境変数で指定され、`default`ユーザーが使用されているものとします。

**例:** 環境変数を使用してNode.jsクライアントインスタンスを作成する。

```ts
import { createClient } from '@clickhouse/client'

const client = createClient({
  host: process.env.CLICKHOUSE_HOST ?? 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER ?? 'default',
  password: process.env.CLICKHOUSE_PASSWORD ?? '',
})
```

クライアントリポジトリには、環境変数を使用する複数の例が含まれています。たとえば、[ClickHouse Cloudでテーブルを作成する](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/create_table_cloud.ts)、[非同期インサートを使う](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/async_insert.ts)など、他にも多くの例があります。

#### コネクションプール（Node.jsのみ）

リクエストごとに接続を確立するオーバーヘッドを避けるために、クライアントはClickHouseへの接続プールを作成し、Keep-Aliveメカニズムを利用して再利用します。デフォルトでKeep-Aliveが有効で、接続プールのサイズは `10` に設定されていますが、`max_open_connections` [設定オプション](./js.md#configuration)で変更することができます。

ユーザーが `max_open_connections: 1` を設定しない限り、プール内の同じ接続が後続のクエリに使用される保証はありません。これはまれにしか必要とされませんが、一時的なテーブルを使用するユーザーにとって必要な場合があります。

また、[Keep-Alive設定](./js.md#keep-alive-configuration-nodejs-only)も参照してください。

### Query ID

`query`や文を送信するすべてのメソッド（`command`、`exec`、`insert`、`select`）は、結果においてユニークな`query_id`を提供します。この識別子はクライアントがクエリごとに割り当て、`system.query_log`からデータを取得したり、長時間実行されるクエリをキャンセルするために役立ちます（[例参照](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/cancel_query.ts)）。必要に応じて、`query_id`は`command`/`query`/`exec`/`insert`メソッドのパラメータでユーザーによって上書きできます。

:::tip
もし`query_id`パラメータを上書きする場合、各呼び出しに対してそのユニーク性を保証する必要があります。ランダムなUUIDがおすすめです。
:::

### すべてのクライアントメソッドの基本パラメータ

いくつかのパラメータはすべてのクライアントメソッド（[query](./js.md#query-method)/[command](./js.md#command-method)/[insert](./js.md#insert-method)/[exec](./js.md#exec-method)）に適用できます。

```ts
interface BaseQueryParams {
  // Queryレベルで適用できるClickHouse設定。
  clickhouse_settings?: ClickHouseSettings
  // クエリバインディング用のパラメータ。
  query_params?: Record<string, unknown>
  // 進行中のクエリをキャンセルするためのAbortSignalインスタンス。
  abort_signal?: AbortSignal
  // query_idの上書き；指定されていない場合、ランダムな識別子が自動的に生成されます。
  query_id?: string
  // session_idの上書き；指定されていない場合、クライアント設定からセッションIDが取得されます。
  session_id?: string
  // 認証情報の上書き；指定されていない場合、クライアントの認証情報が使用されます。
  auth?: { username: string, password: string }
}
```

### クエリメソッド

`SELECT`のような応答を持つ大部分の文、または`CREATE TABLE`のようなDDLを送信するために使用されます。これはアプリケーション内で消費されることを意図した戻り結果セットを伴います。

:::note
データ挿入には専用の[insert](./js.md#insert-method)メソッドがあり、DDL用には[command](./js.md#command-method)があります。
:::

```ts
interface QueryParams extends BaseQueryParams {
  // 実行するクエリで、何らかのデータを返す可能性があります。
  query: string
  // 結果データセットのフォーマット。デフォルト: JSON。
  format?: DataFormat
}

interface ClickHouseClient {
  query(params: QueryParams): Promise<ResultSet>
}
```

「すべてのクライアントメソッドの基本パラメータ」を参照してください。(./js.md#base-parameters-for-all-client-methods)。

:::tip
クエリ内にFORMAT句を指定せず、`format`パラメータを使用してください。
:::

#### ResultSetとRowの抽象化

ResultSetはアプリケーションでのデータ処理に便利なメソッドをいくつか提供します。

Node.jsのResultSet実装は内部で`Stream.Readable`を使用し、Web版はWeb APIの`ReadableStream`を使用します。

ResultSetを消費するには、`text`または`json`メソッドを呼び出してクエリが返すすべての行をメモリにロードします。

同時に、多すぎて一度にメモリに収まらない場合は、`stream`メソッドを呼び出してストリーミングモードでデータを処理出来ます。受け取った各応答チャンクは、特定のチャンクがサーバーからクライアントに送信されるサイズ（これにより個々の行のサイズが決まる）と同程度の比較的小さな配列の行に変換されます。

ストリーミング時に最適なフォーマットを決めるための[サポートされているデータフォーマット](./js.md#supported-data-formats)のリストを参照してください。JSONオブジェクトをストリーミングしたい場合は、[JSONEachRow](https://clickhouse.com/docs/ja/sql-reference/formats#jsoneachrow) を選択し、各行がJSオブジェクトとしてパースされます。あるいは、よりコンパクトな[JSONCompactColumns](https://clickhouse.com/docs/ja/sql-reference/formats#jsoncompactcolumns)フォーマットを選択して、各行をコンパクトな値の配列として受け取ることもできます。次に、[ファイルのストリーミング](./js.md#streaming-files-nodejs-only)を参照してください。

:::important
ResultSet またはそのストリームが完全に消費されない場合、`request_timeout` の非アクティブ期間の後、破棄されます。
:::

```ts
interface BaseResultSet<Stream> {
  // 前述の "Query ID" 部分を参照
  query_id: string

  // ストリーム全体を消費してその内容を文字列として取得
  // どの DataFormat でも使用可能
  // 1回だけ呼び出し
  text(): Promise<string>

  // ストリーム全体を消費し、JSオブジェクトとして内容を解析
  // JSON フォーマットのみで使用可能
  // 1回だけ呼び出し
  json<T>(): Promise<T>

  // ストリーミングできる応答用にリーダブルストリームを返す
  // ストリームの各反復処理では、選択した DataFormat のRow[]の配列を提供
  // 1回だけ呼び出し
  stream(): Stream
}

interface Row {
  // 行の内容をプレーン文字列として取得
  text: string

  // 行の内容をJSオブジェクトとして解析する
  json<T>(): T
}
```

**例:** (Node.js/Web) `JSONEachRow`形式の結果データセットを伴うクエリを実行し、ストリーム全体を消費して内容をJSオブジェクトとして解析します。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/array_json_each_row.ts)。

```ts
const resultSet = await client.query({
  query: 'SELECT * FROM my_table',
  format: 'JSONEachRow',
})
const dataset = await resultSet.json() // JSONの解析を避けるために`row.text`を使用することもできます
```

**例:** (Node.jsのみ) `on('data')` アプローチを使用した `JSONEachRow` フォーマットによるストリーミングクエリ結果。この方法は `for await const` 構文と交換可能です。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/select_streaming_json_each_row.ts)。

```ts
const rows = await client.query({
  query: 'SELECT number FROM system.numbers_mt LIMIT 5',
  format: 'JSONEachRow', // または、JSONCompactEachRow, JSONStringsEachRow など
})
const stream = rows.stream()
stream.on('data', (rows: Row[]) => {
  rows.forEach((row: Row) => {
    console.log(row.json()) // JSONの解析を避けるために `row.text`を使用
  })
})
await new Promise((resolve, reject) => {
  stream.on('end', () => {
    console.log('Completed!')
    resolve(0)
  })
  stream.on('error', reject)
})
```

**例:** (Node.jsのみ) `CSV`フォーマットでストリーミングクエリ結果を取得し、それを `on('data')` アプローチを使用してストリーム送りする。この方法は `for await const` 構文と交換可能です。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/select_streaming_text_line_by_line.ts)

```ts
const resultSet = await client.query({
  query: 'SELECT number FROM system.numbers_mt LIMIT 5',
  format: 'CSV', // または、TabSeparated, CustomSeparated など
})
const stream = resultSet.stream()
stream.on('data', (rows: Row[]) => {
  rows.forEach((row: Row) => {
    console.log(row.text)
  })
})
await new Promise((resolve, reject) => {
  stream.on('end', () => {
    console.log('Completed!')
    resolve(0)
  })
  stream.on('error', reject)
})
```

**例:** (Node.jsのみ) `for await const` 構文を使用して取得した `JSONEachRow` フォーマットのJSオブジェクトとしてストリーミングクエリ結果。 この方法は `on('data')` アプローチと交換可能です。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/select_streaming_json_each_row_for_await.ts)。

```ts
const resultSet = await client.query({
  query: 'SELECT number FROM system.numbers LIMIT 10',
  format: 'JSONEachRow', // または、JSONCompactEachRow, JSONStringsEachRow など
})
for await (const rows of resultSet.stream()) {
  rows.forEach(row => {
    console.log(row.json())
  })
}
```

:::note
`for await const` 構文は、`on('data')` アプローチに比べてコードが少ないですが、パフォーマンスに負の影響を与える可能性があります。詳細は[Node.jsリポジトリ内のこのissue](https://github.com/nodejs/node/issues/31979)を参照してください。
:::

**例:** (Webのみ) `ReadableStream`でのオブジェクトの反復処理。

```ts
const resultSet = await client.query({
  query: 'SELECT * FROM system.numbers LIMIT 10',
  format: 'JSONEachRow'
})

const reader = resultSet.stream().getReader()
while (true) {
  const { done, value: rows } = await reader.read()
  if (done) { break }
  rows.forEach(row => {
    console.log(row.json())
  })
}
```

### Insertメソッド

これはデータ挿入の主要な方法です。

```ts
export interface InsertResult {
  query_id: string
  executed: boolean
}

interface ClickHouseClient {
  insert(params: InsertParams): Promise<InsertResult>
}
```

戻り値の型は最小限であり、サーバーからのデータを返さないことを前提としており、応答ストリームを直ちにドレインします。

空の配列が Insert メソッドに提供された場合、インサート文はサーバーに送信されません。代わりに、`{ query_id: '...', executed: false }`で即座に解決されます。この場合、メソッドパラメータで `query_id` が指定されていない場合、結果は空文字列になります。これは、クエリログテーブルにそのような`query_id`を持つクエリが存在しないため、クライアントが生成したランダムUUIDが混乱を招く可能性があるためです。

インサート文がサーバーに送信された場合、`executed` フラグは `true` になります。

#### InsertメソッドとNode.jsでのストリーミング

これは、指定されたデータフォーマットに応じて `Stream.Readable` または単純な `Array<T>` のいずれかで動作します。[サポートされているデータフォーマット](./js.md#supported-data-formats)も参照してください。このセクションについては[ファイルストリーミング](./js.md#streaming-files-nodejs-only)にも関連します。

Insertメソッドはawaitされることを前提としています。ただし、入力ストリームを指定し、ストリームが完了した後にのみ`insert`操作をawaitすることが可能です（これにより`insert`プロミスが解決されます）。これは潜在的にイベントリスナーや類似のシナリオで有用ですが、エラーハンドリングはクライアント側で多くのエッジケースに対応するため簡単ではないかもしれません。それよりも、例えばこの[例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/async_insert_without_waiting.ts)のように[非同期インサート](https://clickhouse.com/docs/ja/optimize/asynchronous-inserts)を使用することを検討してください。

:::tip
カスタムのINSERTステートメントを持っていて、これをこのメソッドでモデル化するのが困難な場合、[command](./js.md#command-method)を使用することを検討してください。[INSERT INTO ... VALUES](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_values_and_functions.ts)や[INSERT INTO ... SELECT](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_from_select.ts)での使用方法を参照してください。
:::

```ts
interface InsertParams<T> extends BaseQueryParams {
  // データを挿入するテーブル名
  table: string
  // 挿入するデータセット。
  values: ReadonlyArray<T> | Stream.Readable
  // 挿入するデータセットのフォーマット。
  format?: DataFormat
  // データを挿入するカラムを指定できます。
  // - `['a', 'b']` のような配列は: `INSERT INTO table (a, b) FORMAT DataFormat` を生成
  // - `{ except: ['a', 'b']}` のようなオブジェクトは: `INSERT INTO table (* EXCEPT (a, b)) FORMAT DataFormat` を生成
  // デフォルトでは、データはテーブルのすべてのカラムに挿入されます。
  // 生成される文は: `INSERT INTO table FORMAT DataFormat` となります。
  columns?: NonEmptyArray<string> | { except: NonEmptyArray<string> }
}
```

また、参照: [すべてのクライアントメソッドの基本パラメータ](./js.md#base-parameters-for-all-client-methods)。

:::important
`abort_signal` でリクエストをキャンセルしても、データ挿入が行われなかったことを保証するものではありません。サーバーはキャンセル前にストリーミングされたデータの一部を受信している可能性があります。
:::

**例:** (Node.js/Web) 配列の値を挿入する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/array_json_each_row.ts)。

```ts
await client.insert({
  table: 'my_table',
  // 構造は望むフォーマットと一致している必要があります。この例では JSONEachRow
  values: [
    { id: 42, name: 'foo' },
    { id: 42, name: 'bar' },
  ],
  format: 'JSONEachRow',
})
```

**例:** (Node.jsのみ) CSVファイルからストリームを挿入する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/insert_file_stream_csv.ts)。次に参照: [ファイルストリーミング](./js.md#streaming-files-nodejs-only)。

```ts
await client.insert({
  table: 'my_table',
  values: fs.createReadStream('./path/to/a/file.csv'),
  format: 'CSV',
})
```

**例**: インサート文から特定のカラムを除外する。

テーブル定義を仮定して：

```sql
CREATE OR REPLACE TABLE mytable
(id UInt32, message String)
ENGINE MergeTree()
ORDER BY (id)
```

特定のカラムのみを挿入します。

```ts
// 生成された文: INSERT INTO mytable (message) FORMAT JSONEachRow
await client.insert({
  table: 'mytable',
  values: [{ message: 'foo' }],
  format: 'JSONEachRow',
  // この行の`id`カラム値はゼロになります（UInt32のデフォルト）
  columns: ['message'],
})
```

特定のカラムを除外：

```ts
// 生成された文: INSERT INTO mytable (* EXCEPT (message)) FORMAT JSONEachRow
await client.insert({
  table: tableName,
  values: [{ id: 144 }],
  format: 'JSONEachRow',
  // この行の`message`カラム値は空の文字列になります
  columns: {
    except: ['message'],
  },
})
```

[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_exclude_columns.ts) を参照して追加の詳細を確認してください。

**例**: クライアントインスタンスに提供されたものとは異なるデータベースに挿入する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_into_different_db.ts)。

```ts
await client.insert({
  table: 'mydb.mytable', // データベースを含む完全な名前
  values: [{ id: 42, message: 'foo' }],
  format: 'JSONEachRow',
})
```

#### Web版の制限事項

現在、`@clickhouse/client-web` での挿入は `Array<T>` および `JSON*` フォーマットでのみ機能します。  
ストリームの挿入はまだWeb版でサポートされていないため、ブラウザの互換性が低いです。

その結果、Web版の`InsertParams`インターフェースはNode.js版とは少し異なり、  
`values`は`ReadonlyArray<T>`型に制限されています：

```ts
interface InsertParams<T> extends BaseQueryParams {
  // データを挿入するテーブル名
  table: string
  // 挿入するデータセット。
  values: ReadonlyArray<T>
  // 挿入するデータセットのフォーマット。
  format?: DataFormat
  // データを挿入するカラムを指定することができます。
  // - `['a', 'b']` のような配列は: `INSERT INTO table (a, b) FORMAT DataFormat` を生成します
  // - `{ except: ['a', 'b']}` のようなオブジェクトは: `INSERT INTO table (* EXCEPT (a, b)) FORMAT DataFormat` を生成します
  // デフォルトでは、すべてのカラムにデータを挿入し、
  // 生成された文は: `INSERT INTO table FORMAT DataFormat` になります。
  columns?: NonEmptyArray<string> | { except: NonEmptyArray<string> }
}
```

これは将来的に変更される可能性があります。次も参照してください: [すべてのクライアントメソッドの基本パラメータ](./js.md#base-parameters-for-all-client-methods)。

### Commandメソッド

Commandメソッドは、FORMAT句が適用できない文や、応答を特に必要としない状況で使用します。  
例としては、`CREATE TABLE` や `ALTER TABLE` があります。

このメソッドはawaitすべきです。

応答ストリームは即座に破棄され、その結果、基盤となるソケットも解放されます。

```ts
interface CommandParams extends BaseQueryParams {
  // 実行する文。
  query: string
}

interface CommandResult {
  query_id: string
}

interface ClickHouseClient {
  command(params: CommandParams): Promise<CommandResult>
}
```

また、参照: [すべてのクライアントメソッドの基本パラメータ](./js.md#base-parameters-for-all-client-methods)。

**例:** (Node.js/Web) ClickHouse Cloudでテーブルを作成する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/create_table_cloud.ts)。

```ts
await client.command({
  query: `
    CREATE TABLE IF NOT EXISTS my_cloud_table
    (id UInt64, name String)
    ORDER BY (id)
  `,
  // クラスター使用時に、HTTPヘッダーが既にクライアントに送られた後にクエリ処理エラーが発生した状況を避けるために推奨されます。
  // https://clickhouse.com/docs/ja/interfaces/http/#response-bufferingを参照
  clickhouse_settings: {
    wait_end_of_query: 1,
  },
})
```

**例:** (Node.js/Web) セルフマネージドのClickHouseインスタンスでテーブルを作成する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/create_table_single_node.ts)。

```ts
await client.command({
  query: `
    CREATE TABLE IF NOT EXISTS my_table
    (id UInt64, name String)
    ENGINE MergeTree()
    ORDER BY (id)
  `,
})
```

**例:** (Node.js/Web) INSERT FROM SELECT

```ts
await client.command({
  query: `INSERT INTO my_table SELECT '42'`,
})
```

:::important
`abort_signal` でリクエストをキャンセルしても、サーバーがステートメントを実行していないことを保証するものではありません。
:::

### Execメソッド

カスタムクエリがあって、`query`/`insert`に適合せず、結果に関心がある場合、`exec`を`command`の代替として使用できます。

`exec`はリーダブルストリームを返し、アプリケーション側で消費または破棄しなければなりません。

```ts
interface ExecParams extends BaseQueryParams {
  // 実行する文。
  query: string
}

interface ClickHouseClient {
  exec(params: ExecParams): Promise<QueryResult>
}
```

また、参照: [すべてのクライアントメソッドの基本パラメータ](./js.md#base-parameters-for-all-client-methods)。

ストリームの戻り型は、Node.js版とWeb版で異なります。

Node.js:

```ts
export interface QueryResult {
  stream: Stream.Readable
  query_id: string
}
```

Web:

```ts
export interface QueryResult {
  stream: ReadableStream
  query_id: string
}
```

### Ping

`ping`メソッドはサーバに到達可能かチェックするために提供されており、サーバが到達可能であると `true` を返します。

サーバに到達できない場合、基盤となるエラーが結果に含まれます。

```ts
type PingResult =
  | { success: true }
  | { success: false; error: Error }

interface ClickHouseClient {
  ping(): Promise<PingResult>
}
```

Pingはアプリケーション開始時にサーバが使用可能か確認するための便利なツールであり、とりわけClickHouse Cloudでは、インスタンスがアイドル状態からPingで目覚めます。

**例:** (Node.js/Web) ClickHouseサーバインスタンスにPingする。注意：Web版では、捕捉されたエラーが異なります。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/ping.ts)。

```ts
const result = await client.ping();
if (!result.success) {
  // result.errorを処理する
}
```

注意: `/ping` エンドポイントがCORSを実装していないため、Web版では、簡単な `SELECT 1` を使用して類似した結果を達成します。

### クローズ (Node.jsのみ)

すべてのオープンな接続を閉じてリソースを解放します。Web版ではNo-opです。

```ts
await client.close()
```

## ファイルのストリーミング (Node.jsのみ)

クライアントリポジトリには、人気のあるデータフォーマット（NDJSON、CSV、Parquet）でのファイルストリーミングのいくつかの例があります。

- [NDJSONファイルからのストリーミング](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/insert_file_stream_ndjson.ts)
- [CSVファイルからのストリーミング](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/insert_file_stream_csv.ts)
- [Parquetファイルからのストリーミング](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/insert_file_stream_parquet.ts)
- [ファイルとしてのParquetへのストリーミング](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/select_parquet_as_file.ts)

別の形式をファイルにストリーミングすることはParquetの場合と同様です。  
`query`呼び出しに使用するフォーマット（`JSONEachRow`、`CSV`など）と出力ファイル名が異なるだけです。

## サポートされているデータフォーマット

クライアントはデータフォーマットとしてJSONまたはテキストを処理します。

もし`format`としてJSONファミリーのいずれかを指定した場合（`JSONEachRow`、`JSONCompactEachRow` など）、クライアントは通信中にデータをシリアル化およびデシリアル化します。

「生」のテキストフォーマットで提供されたデータ（`CSV`、`TabSeparated`および`CustomSeparated`ファミリー）は、追加の変換なしで通信線上を送信されます。

:::tip
JSON一般形式と[ClickHouse JSON形式](https://clickhouse.com/docs/ja/sql-reference/formats#json)の間で混乱する可能性があります。

クライアントは`JSONEachRow`などの形式でJSONオブジェクトをストリーミングすることをサポートしています（ストリーミングに適した他の形式の概要についてはこの表を参照してください；クライアントリポジトリの`select_streaming_`[例を参照してください](https://github.com/ClickHouse/clickhouse-js/tree/main/examples/node)）。

ただし、[ClickHouse JSON](https://clickhouse.com/docs/ja/sql-reference/formats#json)のような形式やいくつかの他の形式は、応答で単一オブジェクトとして表現され、クライアントによってストリーミングすることはできません。
:::

| Format                                     | Input (array) | Input (object) | Input/Output (Stream) | Output (JSON) | Output (text)  |
|--------------------------------------------|---------------|----------------|-----------------------|---------------|----------------|
| JSON                                       | ❌             | ✔️             | ❌                     | ✔️            | ✔️             |
| JSONCompact                                | ❌             | ✔️             | ❌                     | ✔️            | ✔️             |
| JSONObjectEachRow                          | ❌             | ✔️             | ❌                     | ✔️            | ✔️             |
| JSONColumnsWithMetadata                    | ❌             | ✔️             | ❌                     | ✔️            | ✔️             |
| JSONStrings                                | ❌             | ❌️             | ❌                     | ✔️            | ✔️             |
| JSONCompactStrings                         | ❌             | ❌              | ❌                     | ✔️            | ✔️             |
| JSONEachRow                                | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONStringsEachRow                         | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactEachRow                         | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactStringsEachRow                  | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactEachRowWithNames                | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactEachRowWithNamesAndTypes        | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactStringsEachRowWithNames         | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| JSONCompactStringsEachRowWithNamesAndTypes | ✔️            | ❌              | ✔️                    | ✔️            | ✔️             |
| CSV                                        | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| CSVWithNames                               | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| CSVWithNamesAndTypes                       | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| TabSeparated                               | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| TabSeparatedRaw                            | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| TabSeparatedWithNames                      | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| TabSeparatedWithNamesAndTypes              | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| CustomSeparated                            | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| CustomSeparatedWithNames                   | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| CustomSeparatedWithNamesAndTypes           | ❌             | ❌              | ✔️                    | ❌             | ✔️             |
| Parquet                                    | ❌             | ❌              | ✔️                    | ❌             | ✔️❗- 次を参照 |

Parquetの場合、selectの主なユースケースは結果ストリームをファイルに書き込むことです。[例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/select_parquet_as_file.ts)をクライアントリポジトリで参照してください。

ClickHouseの入力および出力形式の完全なリストは[ここ](https://clickhouse.com/docs/ja/interfaces/formats)で確認できます。

## サポートされているClickHouseデータ型

| Type           | Status         | JS type               |
|----------------|----------------|-----------------------|
| UInt8/16/32    | ✔️             | number                |
| UInt64/128/256 | ✔️❗- 次参照 | string                |
| Int8/16/32     | ✔️             | number                |
| Int64/128/256  | ✔️❗- 次参照 | string                |
| Float32/64     | ✔️             | number                |
| Decimal        | ✔️❗- 次参照 | number                |
| Boolean        | ✔️             | boolean               |
| String         | ✔️             | string                |
| FixedString    | ✔️             | string                |
| UUID           | ✔️             | string                |
| Date32/64      | ✔️              | string                |
| DateTime32/64  | ✔️❗- 次参照 | string                |
| Enum           | ✔️             | string                |
| LowCardinality | ✔️             | string                |
| Array(T)       | ✔️             | T[]                   |
| JSON           | ✔️             | object                |
| Nested         | ✔️             | T[]                   |
| Tuple          | ✔️             | Tuple                 |
| Nullable(T)    | ✔️             | JS type for T or null |
| IPv4           | ✔️             | string                |
| IPv6           | ✔️             | string                |
| Point          | ✔️             | [ number, number ]    |
| Ring           | ✔️             | Array<Point\>         |
| Polygon        | ✔️             | Array<Ring\>          |
| MultiPolygon   | ✔️             | Array<Polygon\>       |
| Map(K, V)      | ✔️             | Record<K, V\>         |

サポートされているClickHouse形式の完全なリストは、
[こちら](https://clickhouse.com/docs/ja/sql-reference/data-types/)で確認できます。

### Date/Date32型についての注意点

クライアントは追加の型変換をせずに値を挿入するため、`Date`/`Date32`型のカラムは文字列としてのみ挿入できます。

**例:** `Date`型の値を挿入する。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/ba387d7f4ce375a60982ac2d99cb47391cf76cec/__tests__/integration/date_time.test.ts)。

```ts
await client.insert({
  table: 'my_table',
  values: [ { date: '2022-09-05' } ],
  format: 'JSONEachRow',
})
```

ただし、`DateTime`または`DateTime64`カラムを使用している場合、文字列とJS Dateオブジェクトの両方を使用できます。JS Dateオブジェクトは`date_time_input_format`を`best_effort`として`insert`にそのまま渡すことができます。この[例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_js_dates.ts)を参照して詳細を確認してください。

### Decimal\* 型についての注意点

`JSON*`ファミリーフォーマットを用いてDecimalを挿入することが可能です。たとえば、以下のようなテーブルが定義されているとします：

```sql
CREATE TABLE my_table
(
  id     UInt32,
  dec32  Decimal(9, 2),
  dec64  Decimal(18, 3),
  dec128 Decimal(38, 10),
  dec256 Decimal(76, 20)
)
ENGINE MergeTree()
ORDER BY (id)
```

次のように、精度を失うことなくString表現を用いて値を挿入できます：

```ts
await client.insert({
  table: 'my_table',
  values: [{
    id: 1,
    dec32:  '1234567.89',
    dec64:  '123456789123456.789',
    dec128: '1234567891234567891234567891.1234567891',
    dec256: '12345678912345678912345678911234567891234567891234567891.12345678911234567891',
  }],
  format: 'JSONEachRow',
})
```

ただし、`JSON*`形式でデータをクエリする場合、ClickHouseはデフォルトでDecimalを数値として返すため、精度が失われる可能性があります。これを避けるには、クエリ内でDecimalを文字列にキャストします：

```ts
await client.query({
  query: `
    SELECT toString(dec32)  AS decimal32,
           toString(dec64)  AS decimal64,
           toString(dec128) AS decimal128,
           toString(dec256) AS decimal256
    FROM my_table
  `,
  format: 'JSONEachRow',
})
```

詳細については[この例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/insert_decimals.ts)を参照してください。

### 整数型：Int64、Int128、Int256、UInt64、UInt128、UInt256

サーバーは数値として受け入れることができますが、クライアントでは整数オーバーフローを避けるため、これらの型は`JSON*`ファミリー出力フォーマットで文字列として返されます。これらの型の最大値は `Number.MAX_SAFE_INTEGER` よりも大きいです。

ただし、この動作は
[`output_format_json_quote_64bit_integers` 設定](https://clickhouse.com/docs/ja/operations/settings/formats#output_format_json_quote_64bit_integers)を使用して変更できます。

**例:** 64ビット数値のためにJSON出力フォーマットを調整する。

```ts
const resultSet = await client.query({
  query: 'SELECT * from system.numbers LIMIT 1',
  format: 'JSONEachRow',
})

expect(await resultSet.json()).toEqual([ { number: '0' } ])
```

```ts
const resultSet = await client.query({
  query: 'SELECT * from system.numbers LIMIT 1',
  format: 'JSONEachRow',
  clickhouse_settings: { output_format_json_quote_64bit_integers: 0 },
})

expect(await resultSet.json()).toEqual([ { number: 0 } ])
```

## ClickHouse設定

クライアントは、[設定](https://clickhouse.com/docs/ja/operations/settings/settings/)メカニズムを介してClickHouseの動作を調整することができます。
これらの設定は、クライアントインスタンスレベルで設定でき、ClickHouseへのすべての要求に適用されます：

```ts
const client = createClient({
  clickhouse_settings: {}
})
```

または、リクエストレベルで設定を構成することもできます：

```ts
client.query({
  clickhouse_settings: {}
})
```

すべてのサポートされているClickHouse設定を含む型宣言ファイルは
[こちら](https://github.com/ClickHouse/clickhouse-js/blob/main/packages/client-common/src/settings.ts)で見つけることができます。

:::important
クエリが行われるユーザーが、設定を変更するための十分な権限を持っていることを確認してください。
:::

## 高度なトピック

### パラメータ付きクエリ

クエリにパラメータを作成し、クライアントアプリケーションからそれらに値を渡すことができます。これにより、クライアント側で特定の動的値でクエリをフォーマットする必要がなくなります。

通常のフォーマットでクエリを作成し、クエリのアプリパラメータから値を渡したい場所に次のフォーマットで中括弧に入れてください：

```
{<name>: <data_type>}
```

ここで：

- `name` — プレースホルダー識別子。
- `data_type` - アプリケーションパラメータ値の[データ型](https://clickhouse.com/docs/ja/sql-reference/data-types/)。

**例:**: パラメータ付きクエリ。  
[ソースコード](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/query_with_parameter_binding.ts)。

```ts
await client.query({
  query: 'SELECT plus({val1: Int32}, {val2: Int32})',
  format: 'CSV',
  query_params: {
    val1: 10,
    val2: 20,
  },
})
```

さらに詳しい情報は、https://clickhouse.com/docs/ja/interfaces/cli#cli-queries-with-parameters-syntax を確認してください。

### 圧縮

注意: リクエスト圧縮は現在Web版では利用できません。レスポンス圧縮は通常どおり機能します。Node.js版は両方をサポートしています。

大きなデータセットを通信するアプリケーションは、圧縮を有効にすることで利益を得ることができます。現在、[zlib](https://nodejs.org/docs/latest-v14.x/api/zlib.html)を使用した`GZIP`のみがサポートされています。

```typescript
createClient({
  compression: {
    response: true,
    request: true
  }
})
```

設定パラメータは次のとおりです：

- `response: true` はClickHouseサーバに圧縮されたレスポンスボディを返すよう指示します。デフォルト値: `response: false`
- `request: true` クライアントリクエストボディの圧縮を有効にします。デフォルト値: `request: false`

### ロギング (Node.jsのみ)

:::important
ロギングはエクスペリメンタルな機能であり、将来的に変更される可能性があります。
:::

デフォルトのロガー実装は、 `console.debug/info/warn/error` メソッドを介して`stdout`にログレコードを出力します。
`LoggerClass`を提供してログロジックをカスタマイズし、 `level`パラメータで希望するログレベルを選ぶことができます（デフォルトは`OFF`です）：

```typescript
import type { Logger } from '@clickhouse/client'

// クライアントによってエクスポートされる3つのすべてのLogParams型
interface LogParams {
  module: string
  message: string
  args?: Record<string, unknown>
}
type ErrorLogParams = LogParams & { err: Error }
type WarnLogParams = LogParams & { err?: Error }

class MyLogger implements Logger {
  trace({ module, message, args }: LogParams) {
    // ...
  }
  debug({ module, message, args }: LogParams) {
    // ...
  }
  info({ module, message, args }: LogParams) {
    // ...
  }
  warn({ module, message, args }: WarnLogParams) {
    // ...
  }
  error({ module, message, args, err }: ErrorLogParams) {
    // ...
  }
}

const client = createClient({
  log: {
    LoggerClass: MyLogger,
    level: ClickHouseLogLevel
  }
})
```

現在、クライアントは以下のイベントをログに記録します：

- `TRACE` - Keep-Aliveソケットのライフサイクルに関する低レベルの情報
- `DEBUG` - 応答情報（認証ヘッダーやホスト情報を除く）
- `INFO` - 主に使用されない、クライアントが初期化される時に現在のログレベルを表示する
- `WARN` - 致命的でないエラー；失敗した`ping`リクエストは警告としてログに記録され、基盤となるエラーは結果に含まれる
- `ERROR` - `query`/`insert`/`exec`/`command`メソッドからの致命的なエラー、例えば失敗したリクエスト

デフォルトのロガー実装は[ここ](https://github.com/ClickHouse/clickhouse-js/blob/main/packages/client-common/src/logger.ts)で確認できます。

### TLS証明書 (Node.jsのみ)

Node.jsクライアントはオプションで基本的な（認証局のみ）および相互（認証局とクライアント証明書）TLSをサポートしています。

`certs`フォルダ内に証明書を持ち、CAファイル名が`CA.pem`であることを前提とした基本的なTLS設定例：

```ts
const client = createClient({
  host: 'https://<hostname>:<port>',
  username: '<username>',
  password: '<password>', // 必要な場合
  tls: {
    ca_cert: fs.readFileSync('certs/CA.pem'),
  },
})
```

クライアント証明書を使った相互TLS設定例：

```ts
const client = createClient({
  host: 'https://<hostname>:<port>',
  username: '<username>',
  tls: {
    ca_cert: fs.readFileSync('certs/CA.pem'),
    cert: fs.readFileSync(`certs/client.crt`),
    key: fs.readFileSync(`certs/client.key`),
  },
})
```

クライアントリポジトリの[基本TLS例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/basic_tls.ts)および[相互TLS例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/node/mutual_tls.ts)を参照してください。

### Keep-Alive設定 (Node.jsのみ)

クライアントは基盤となるHTTPエージェントでKeep-Aliveをデフォルトで有効にします。つまり、接続されたソケットは後続のリクエストで再利用され、`Connection: keep-alive` ヘッダが送信されます。アイドル状態のソケットはデフォルトで2500ミリ秒間接続プールに残ります（[このオプションの調整についてのメモ](./js.md#adjusting-idle_socket_ttl)を参照してください）。

`keep_alive.idle_socket_ttl`はその値がサーバー/LB設定を大幅に下回る必要があります。主な理由は、HTTP/1.1がクライアントに通知せずにサーバーがソケットを閉じることを許可するため、サーバーやロードバランサーがクライアントより先にコネクションを閉じると、クライアントが閉じたソケットを再利用しようとして`socket hang up`エラーが発生する可能性があるからです。

`keep_alive.idle_socket_ttl`を変更する場合、常にサーバー/LBのKeep-Alive設定と同期している必要があり、常にそれより低くなければなりません。これにより、サーバーがクライアントより先にオープンな接続を閉じることはないことが保証されます。

#### `idle_socket_ttl`の調整

クライアントは `keep_alive.idle_socket_ttl` を 2500ミリ秒に設定しています。これは最も安全なデフォルトとみなされ、その間に`keep_alive_timeout`が[23.11より前のバージョンのClickHouseで設定なしで3秒ほどに設定されるかもしれない](https://github.com/ClickHouse/ClickHouse/commit/1685cdcb89fe110b45497c7ff27ce73cc03e82d1)ためです。

:::warning
パフォーマンスに満足していて、問題が発生していない場合、`keep_alive.idle_socket_ttl`設定の値を増やさないことをお勧めします。そうしないと"Socket hang-up"エラーが発生する可能性があるからです。また、アプリケーションが多くのクエリを送信し、それらの間に多くのダウンタイムがない場合、そのデフォルト値で十分であるはずです。ソケットは長時間アイドルすることなく、クライアントがそれらをプールに保持するからです。
:::

次のコマンドを実行することでサーバー応答ヘッダの中で正しいKeep-Aliveタイムアウト値を見つけることができます：

```sh
curl -v --data-binary "SELECT 1" <clickhouse_url>
```

応答の中の`Connection`と`Keep-Alive`ヘッダの値を確認してください。たとえば：

```
< Connection: Keep-Alive
< Keep-Alive: timeout=10
```

この場合、`keep_alive_timeout`は10秒です。アイドルソケットをデフォルトよりも少し長く開いておくために、`keep_alive.idle_socket_ttl`を9000または9500ミリ秒に増やしてみることができます。"Socket hang-up"エラーが発生しないか確認してください。これらが発生する場合、サーバーがクライアントより先に接続を閉じていることを示し、エラーが消えるまで値を下げます。

#### Keep-Aliveのトラブルシューティング

Keep-Aliveを使用中に`socket hang up`エラーが発生する場合、次のオプションでこの問題を解決できます：

* ClickHouseサーバー設定で `keep_alive.idle_socket_ttl` 設定をわずかに減少させる。特定の状況、たとえばクライアントとサーバーの間の高いネットワーク遅延がある場合、送信中のリクエストがサーバーが閉じる予定のソケットを取得する状況を避けるために、`keep_alive.idle_socket_ttl`を200〜500ミリ秒ほどさらに減少させることが有益かもしれません。
* このエラーがデータの入出力なしで長時間実行されるクエリ中に発生している場合（たとえば、長時間実行される `INSERT FROM SELECT`）、ロードバランサがアイドル状態の接続を閉じるためかもしれません。このような長時間実行されるクエリ中に一部のデータの入出力を強制することを試みるためには、以下のClickHouse設定を組み合わせて使用できます。

  ```ts
  const client = createClient({
    // ここでは、実行時間が5分以上かかるクエリがあると想定しています
    request_timeout: 400_000,
    /** これらの設定を組み合わせることで、データの入出力がない長時間実行されるクエリを実行する際のLBのタイムアウトの問題を回避できます。
     * 例えば、 `INSERT FROM SELECT` などのように。LBが接続をアイドル状態と見なして突然切断する可能性があります。
     * この場合、LBのアイドル接続タイムアウトが120秒であると想定し、安全な値として110秒に設定します。 */
    clickhouse_settings: {
      send_progress_in_http_headers: 1,
      http_headers_progress_interval_ms: '110000', // UInt64、文字列として渡す必要があります
    },
  })
  ```
  しかしながら、受信ヘッダーの総サイズには最近のNode.jsバージョンで16KBの制限があることを覚えておいてください。テストでは約70〜80の進捗ヘッダーを受信すると例外が発生しました。

  また、完全に異なる方法を使用することも可能です。これは、HTTPインターフェイスの「機能」を利用することで、接続が失われてもミューテーションがキャンセルされないため、待機時間を完全に回避できます。[この例（パート2）](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/long_running_queries_timeouts.ts)を参照してください。

* Keep-Alive機能を完全に無効にすることができます。この場合、クライアントはすべてのリクエストに `Connection: close` ヘッダーを追加し、基盤のHTTPエージェントは接続を再利用しないようにします。`keep_alive.idle_socket_ttl` 設定は無視されます。これは、アイドル状態のソケットがないためです。これにより、すべてのリクエストに対して新しい接続が確立されるため、追加のオーバーヘッドが発生します。

  ```ts
  const client = createClient({
    keep_alive: {
      enabled: false,
    },
  })
  ```

### 読み取り専用ユーザー

クライアントを[readonly=1のユーザー](https://clickhouse.com/docs/ja/operations/settings/permissions-for-queries#readonly)で使用する場合、`enable_http_compression`設定が必要なため、レスポンス圧縮を有効にすることはできません。以下の設定はエラーを引き起こします：

```ts
const client = createClient({
  compression: {
    response: true, // readonly=1のユーザーでは機能しません
  },
})
```

readonly=1のユーザーの制限についての詳細は[こちらの例](https://github.com/ClickHouse/clickhouse-js/blob/main/examples/read_only_user.ts)を参照してください。

### パスネームを持つプロキシ

ClickHouseインスタンスがプロキシの背後にあり、URLにパスネームがある場合（たとえば、http://proxy:8123/clickhouse_server）、`clickhouse_server` を `pathname` 設定オプションとして指定してください（先頭にスラッシュがあってもなくても構いません）。さもなければ、`url` に直接指定されている場合、それが `database` オプションとして考慮されます。複数のセグメントがサポートされています。例：`/my_proxy/db`。

```ts
const client = createClient({
  url: 'http://proxy:8123',
  pathname: '/clickhouse_server',
})
```

### 認証付きのリバースプロキシ

ClickHouse配置の前に認証付きのリバースプロキシがある場合、必要なヘッダーを提供するために `http_headers` 設定を使用できます：

```ts
const client = createClient({
  http_headers: {
    'My-Auth-Header': '...',
  },
})
```

### カスタムHTTP/HTTPSエージェント（エクスペリメンタル、Node.jsのみ）

:::warning
これは将来的なリリースでの後方互換性のない方法で変更される可能性のあるエクスペリメンタルな機能です。クライアントが提供するデフォルトの実装と設定は、ほとんどのユースケースで十分です。この機能は、必要があることを確信している場合にのみ使用してください。
:::

デフォルトでは、クライアントはクライアント設定で提供された設定（`max_open_connections`、`keep_alive.enabled`、`tls`など）を使用して基盤のHTTP(s)エージェントを構成し、ClickHouseサーバーへの接続を管理します。さらに、TLS証明書が使用される場合、基盤のエージェントは必要な証明書で構成され、正しいTLS認証ヘッダーが強制されます。

バージョン1.2.0以降、デフォルトの基盤エージェントを置き換えるカスタムHTTP(s)エージェントをクライアントに提供することが可能です。これは、ネットワーク構成が複雑な場合に役立ちます。カスタムエージェントが提供される場合、次の条件が適用されます：
- `max_open_connections`と`tls`オプションはクライアントによって無視され、効果はありません。これらは基盤エージェントの設定の一部です。
- `keep_alive.enabled`はデフォルトの`Connection`ヘッダーの値のみを規制します（`true` -> `Connection: keep-alive`、`false` -> `Connection: close`）。
- アイドル状態のキープアライブソケット管理は引き続き動作します（これはエージェントではなく特定のソケット自体に関連付けられています）が、`keep_alive.idle_socket_ttl`値を`0`に設定して完全に無効にすることもできます。

#### カスタムエージェント使用例

証明書なしのカスタムHTTP(s)エージェントを使用：

```ts
const agent = new http.Agent({ // または https.Agent
  keepAlive: true,
  keepAliveMsecs: 2500,
  maxSockets: 10,
  maxFreeSockets: 10,
})
const client = createClient({
  http_agent: agent,
})
```

基本的なTLSとCA証明書を使用したカスタムHTTPSエージェント：

```ts
const agent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 2500,
  maxSockets: 10,
  maxFreeSockets: 10,
  ca: fs.readFileSync('./ca.crt'),
})
const client = createClient({
  url: 'https://myserver:8443',
  http_agent: agent,
  // カスタムHTTPSエージェントを使用する場合、クライアントはデフォルトのHTTPS接続実装を使用しません。ヘッダーは手動で提供する必要があります
  http_headers: {
    'X-ClickHouse-User': 'username',
    'X-ClickHouse-Key': 'password',
  },
  // 重要：認可ヘッダーがTLSヘッダーと衝突します。これを無効にします。
  set_basic_auth_header: false,
})
```

相互TLSを使用したカスタムHTTPSエージェント：

```ts
const agent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 2500,
  maxSockets: 10,
  maxFreeSockets: 10,
  ca: fs.readFileSync('./ca.crt'),
  cert: fs.readFileSync('./client.crt'),
  key: fs.readFileSync('./client.key'),
})
const client = createClient({
  url: 'https://myserver:8443',
  http_agent: agent,
  // カスタムHTTPSエージェントを使用する場合、クライアントはデフォルトのHTTPS接続実装を使用しません。ヘッダーは手動で提供する必要があります
  http_headers: {
    'X-ClickHouse-User': 'username',
    'X-ClickHouse-Key': 'password',
    'X-ClickHouse-SSL-Certificate-Auth': 'on',
  },
  // 重要：認可ヘッダーがTLSヘッダーと衝突します。これを無効にします。
  set_basic_auth_header: false,
})
```

証明書およびカスタムHTTPSエージェントを使用する場合、TLSヘッダーと衝突するため、`set_basic_auth_header`設定（バージョン1.2.0で導入）を使用してデフォルトの認証ヘッダーを無効にする必要がある可能性があります。すべてのTLSヘッダーは手動で提供する必要があります。

## 既知の制限事項（Node.js/ウェブ）

- 結果セットのデータマッパーがなく、言語のプリミティブのみが使用されます。特定のデータタイプマッパーは[RowBinary形式サポート](https://github.com/ClickHouse/clickhouse-js/issues/216)が予定されています。
- [Decimal*とDate\* / DateTime\*データ型の制限事項](./js.md#datedate32-types-caveats)があります。
- JSON*ファミリ形式を使用する場合、Int32より大きな数値は文字列として表示されます。これは、Int64+型の最大値が`Number.MAX_SAFE_INTEGER`よりも大きいためです。[Integral types](./js.md#integral-types-int64-int128-int256-uint64-uint128-uint256)セクションを参照してください。

## 既知の制限事項（ウェブ）

- セレクトクエリのストリーミングは動作しますが、インサートでは無効になっています（型レベルでも）。
- リクエスト圧縮は無効化され、設定は無視されます。レスポンス圧縮は機能します。
- ロギングサポートはまだありません。

## パフォーマンス最適化のためのヒント

- アプリケーションのメモリ消費を削減するために、（ファイルからの）大規模なインサートやセレクトでストリームを使用することを検討してください。イベントリスナーや同様のユースケースでは、[非同期インサート](https://clickhouse.com/docs/ja/optimize/asynchronous-inserts)が良い選択肢になる可能性があります。これにより、クライアント側でのバッチ処理を最小化、または完全に回避できます。非同期インサートの例は、[クライアントリポジトリ](https://github.com/ClickHouse/clickhouse-js/tree/main/examples)のファイル名プレフィックス`async_insert_`で提供されています。
- クライアントはデフォルトでリクエストやレスポンスの圧縮を有効にしません。しかし、大規模なデータセットを選択または挿入する場合、`ClickHouseClientConfigOptions.compression`で圧縮を有効にすることができます（`request`または`response`のどちらか、または両方で）。
- 圧縮は重要なパフォーマンスのペナルティを伴います。`request`または`response`に対して有効にすると、それぞれ選択または挿入の速度に悪影響を及ぼしますが、アプリケーションによって転送されるネットワークトラフィックの量は減少します。

## ご相談・お問い合わせ

質問やサポートが必要な場合は、[Community Slack](https://clickhouse.com/slack)（`#clickhouse-js`チャンネル）または[GitHub issues](https://github.com/ClickHouse/clickhouse-js/issues)を通じてお気軽にご連絡ください。
