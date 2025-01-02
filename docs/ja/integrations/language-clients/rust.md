---
sidebar_label: Rust
sidebar_position: 4
keywords: [clickhouse, rs, rust, cargo, crate, http, client, connect, integrate]
slug: /ja/integrations/rust
description: ClickHouseに接続するための公式Rustクライアント。
---

# ClickHouse Rust クライアント

ClickHouseに接続するための公式Rustクライアントは、もともと[Paul Loyd](https://github.com/loyd)によって開発されました。クライアントのソースコードは[GitHubリポジトリ](https://github.com/ClickHouse/clickhouse-rs)で利用可能です。

## 概要

* 行をエンコード/デコードするために`serde`を使用します。
* `serde`属性をサポート：`skip_serializing`, `skip_deserializing`, `rename`。
* HTTPトランスポート上で[`RowBinary`](https://clickhouse.com/docs/ja/interfaces/formats#rowbinary)フォーマットを使用します。
    * TCP上で[`Native`](https://clickhouse.com/docs/ja/interfaces/formats#native)に切り替える計画があります。
* TLSをサポートします（`native-tls`および`rustls-tls`機能を通じて）。
* 圧縮と解凍（LZ4）をサポートします。
* データの選択や挿入、DDLの実行、クライアント側でのバッチ処理のためのAPIを提供します。
* 単体テスト用の便利なモックを提供します。

## インストール

クレートを使用するには、`Cargo.toml`に次のように追加してください：

```toml
[dependencies]
clickhouse = "0.12.2"

[dev-dependencies]
clickhouse = { version = "0.12.2", features = ["test-util"] }
```

参考： [crates.ioページ](https://crates.io/crates/clickhouse)。

## Cargo機能

* `lz4`（デフォルトで有効） — `Compression::Lz4`と`Compression::Lz4Hc(_)`のバリアントを有効にします。有効にすると、`Compression::Lz4`が`WATCH`を除くすべてのクエリに対してデフォルトで使用されます。
* `native-tls` — `HTTPS`スキーマを持つURLを`hyper-tls`を通じてサポートし、OpenSSLにリンクします。
* `rustls-tls` — `HTTPS`スキーマを持つURLを`hyper-rustls`を通じてサポートし、OpenSSLにリンクしません。
* `inserter` — `client.inserter()`を有効にします。
* `test-util` — モックを追加します。詳しくは[例](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs)を参照してください。`dev-dependencies`のみに使用してください。
* `watch` — `client.watch`機能を有効にします。詳細については該当セクションを参照してください。
* `uuid` — `serde::uuid`を追加し、[uuid](https://docs.rs/uuid)クレートと連携します。
* `time` — `serde::time`を追加し、[time](https://docs.rs/time)クレートと連携します。

:::important
`HTTPS` URL経由でClickHouseに接続する場合は、`native-tls`または`rustls-tls`機能を有効にする必要があります。
両方を有効にした場合、`rustls-tls`機能が優先されます。
:::

## ClickHouseバージョンの互換性

クライアントはClickHouseのLTSまたはそれ以降のバージョン、およびClickHouse Cloudと互換性があります。

ClickHouseサーバーv22.6より前のバージョンは、[いくつかの稀なケースでRowBinaryを正しく処理しません](https://github.com/ClickHouse/ClickHouse/issues/37420)。
この問題を解決するには、v0.11+を使用し、`wa-37420`機能を有効にできます。この機能は新しいClickHouseバージョンでは使用しないでください。

## 例

クライアントリポジトリの[例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples)でクライアント利用のさまざまなシナリオをカバーすることを目指しています。概要は[examples README](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/README.md#overview)で利用可能です。

例や以下のドキュメントで不明な点や不足がある場合は、[お問い合わせください](./rust.md#contact-us)。

## 使用方法

:::note
`ch2rs` crateは、ClickHouseから行タイプを生成するのに便利です。
:::

### クライアントインスタンスの作成

:::tip
作成されたクライアントを再利用するか、基礎のhyper接続プールを再利用するためにクローンしてください。
:::

```rust
use clickhouse::Client;

let client = Client::default()
    // プロトコルとポートの両方を含める必要があります
    .with_url("http://localhost:8123")
    .with_user("name")
    .with_password("123")
    .with_database("test");
```

### HTTPSまたはClickHouse Cloud接続

HTTPSは`rustls-tls`または`native-tls`のcargo機能で動作します。

その後、通常の方法でクライアントを作成します。この例では、環境変数を使用して接続の詳細を保存しています：

:::important
URLはプロトコルとポートの両方を含める必要があります。例：`https://instance.clickhouse.cloud:8443`。
:::

```rust
fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

let client = Client::default()
    .with_url(read_env_var("CLICKHOUSE_URL"))
    .with_user(read_env_var("CLICKHOUSE_USER"))
    .with_password(read_env_var("CLICKHOUSE_PASSWORD"));
```

参考：
- クライアントリポジトリの[ClickHouse CloudのHTTPS使用例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/clickhouse_cloud.rs)。これはオンプレミスのHTTPS接続でも適用可能です。

### 行の選択

```rust
use serde::Deserialize;
use clickhouse::Row;
use clickhouse::sql::Identifier;

#[derive(Row, Deserialize)]
struct MyRow<'a> {
    no: u32,
    name: &'a str,
}

let table_name = "some";
let mut cursor = client
    .query("SELECT ?fields FROM ? WHERE no BETWEEN ? AND ?")
    .bind(Identifier(table_name))
    .bind(500)
    .bind(504)
    .fetch::<MyRow<'_>>()?;

while let Some(row) = cursor.next().await? { .. }
```

* プレースホルダー`?fields`は、`Row`のフィールド`no, name`に置き換えられます。
* プレースホルダー`?`は、以下の`bind()`呼び出しで値に置き換えられます。
* 便利な`fetch_one::<Row>()`と`fetch_all::<Row>()`メソッドは、それぞれ最初の行またはすべての行を取得するために使用できます。
* `sql::Identifier`はテーブル名をバインドするために使用できます。

注意：応答全体がストリーミングされるため、カーソルは一部の行を生成した後でもエラーを返す場合があります。この場合、サーバー側の応答バッファリングを有効にするために`query(...).with_option("wait_end_of_query", "1")`を試すことができます。[詳細はこちら](https://clickhouse.com/docs/ja/interfaces/http/#response-buffering)。`buffer_size`オプションも役立ちます。

:::warning
行を選択するときに`wait_end_of_query`を慎重に使用してください。サーバー側でのメモリ消費が増加し、パフォーマンス全体が低下する可能性があります。
:::

### 行の挿入

```rust
use serde::Serialize;
use clickhouse::Row;

#[derive(Row, Serialize)]
struct MyRow {
    no: u32,
    name: String,
}

let mut insert = client.insert("some")?;
insert.write(&MyRow { no: 0, name: "foo".into() }).await?;
insert.write(&MyRow { no: 1, name: "bar".into() }).await?;
insert.end().await?;
```

* `end()`が呼び出されないと、`INSERT`は中止されます。
* 行はネットワーク負荷を分配するストリームとして順次送信されます。
* ClickHouseは、すべての行が同じパーティションに適合し、その数が[`max_insert_block_size`](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size)以下である場合にのみバッチをアトミックに挿入します。

### 非同期挿入（サーバー側バッチ処理）

クライアント側でデータのバッチ処理を避けるために[ClickHouse非同期挿入](https://clickhouse.com/docs/ja/optimize/asynchronous-inserts)を利用できます。これは、`async_insert`オプションを`insert`メソッドに（またはクライアントインスタンス自体に）提供することで実現できます。これにより、すべての`insert`呼び出しに影響します。

```rust
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_option("async_insert", "1")
    .with_option("wait_for_async_insert", "0");
```

参考：
- クライアントリポジトリの[非同期挿入の例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/async_insert.rs)。

### インサータ機能（クライアント側バッチ処理）

`inserter` cargo機能が必要です。

```rust
let mut inserter = client.inserter("some")?
    .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
    .with_max_bytes(50_000_000)
    .with_max_rows(750_000)
    .with_period(Some(Duration::from_secs(15)));

inserter.write(&MyRow { no: 0, name: "foo".into() })?;
inserter.write(&MyRow { no: 1, name: "bar".into() })?;
let stats = inserter.commit().await?;
if stats.rows > 0 {
    println!(
        "{} bytes, {} rows, {} transactions have been inserted",
        stats.bytes, stats.rows, stats.transactions,
    );
}

// アプリケーション終了時にinserterを最終化し、残りの行をコミットすることを忘れないでください。. `end()`は統計も提供します。
inserter.end().await?;
```

* `Inserter`は、いずれかのしきい値（`max_bytes`、`max_rows`、`period`）に達した場合に`commit()`でアクティブな挿入を終了します。
* パラレルインサータによる負荷スパイクを避けるために、`with_period_bias`を使用してアクティブな`INSERT`間の間隔がバイアスされます。
* `Inserter::time_left()`を使用して、現在の期間が終了した時を検出できます。項目がまれに生成される場合は、再び`Inserter::commit()`を呼び出して制限をチェックしてください。
* 時間しきい値は、インサータを高速にするために[quanta](https://docs.rs/quanta)クレートを使用して実装されています。`test-util`が有効な場合、使用されません（したがって、カスタムテストで`tokio::time::advance()`によって時間を管理できます）。
* `commit()`呼び出し間のすべての行は、同じ`INSERT`ステートメントで挿入されます。

:::warning
挿入を終了/最終化する場合にフラッシュすることを忘れないでください：
```rust
inserter.end().await?;
```
:::

### DDLの実行

単一ノードの展開では、次のようにDDLを実行するだけで十分です：

```rust
client.query("DROP TABLE IF EXISTS some").execute().await?;
```

しかし、負荷分散装置やClickHouse Cloudを使用している場合、すべてのレプリカでDDLが適用されるのを待つことが推奨されます。これは`wait_end_of_query`オプションを使用して行うことができます：

```rust
client
    .query("DROP TABLE IF EXISTS some")
    .with_option("wait_end_of_query", "1")
    .execute()
    .await?;
```

### ClickHouse設定

`with_option`メソッドを使用して、さまざまな[ClickHouse設定](https://clickhouse.com/docs/ja/operations/settings/settings)を適用できます。例えば：

```rust
let numbers = client
    .query("SELECT number FROM system.numbers")
    // この設定はこの特定のクエリにのみ適用されます。グローバルなクライアント設定を上書きします。
    .with_option("limit", "3")
    .fetch_all::<u64>()
    .await?;
```

`query`と同様に、`insert`および`inserter` メソッドでも同様に機能します。さらに、`Client`インスタンスでこのメソッドを呼び出して、すべてのクエリに対するグローバル設定を行うことができます。

### クエリID

`with_option`を使用して、ClickHouseのクエリログでクエリを識別するために`query_id`オプションを設定できます。

```rust
let numbers = client
    .query("SELECT number FROM system.numbers LIMIT 1")
    .with_option("query_id", "some-query-id")
    .fetch_all::<u64>()
    .await?;
```

`query`と同様に、`insert`および`inserter` メソッドでも同様に機能します。

:::danger
`query_id`を手動で設定する場合は、それが一意であることを確認してください。UUIDはそのための良い選択肢です。
:::

参考：クライアントリポジトリの[query_idの例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/query_id.rs)。

### セッションID

`query_id`と同様に、同じセッションでステートメントを実行するために`session_id`を設定できます。`session_id`は、クライアントレベルでグローバルに、または`query`、`insert`、`inserter`呼び出しごとに設定することができます。

```rust
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_option("session_id", "my-session");
```

:::danger
クラスター化されたデプロイメントでは、「スティッキーセッション」がないため、この機能を適切に利用するには特定のクラスター ノードに接続する必要があります。たとえば、ラウンドロビンの負荷分散装置は、後続のリクエストが同じ ClickHouse ノードによって処理されることを保証しません。
:::

参考：クライアントリポジトリの[session_idの例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/session_id.rs)。

### カスタムHTTPヘッダー

プロキシ認証を使用している場合やカスタムヘッダーを渡す必要がある場合、次のように行うことができます：

```rust
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_header("X-My-Header", "hello");
```

参考：クライアントリポジトリの[カスタムHTTPヘッダーの例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/custom_http_headers.rs)。

### カスタムHTTPクライアント

基礎となるHTTP接続プールの設定を微調整するのに役立ちます。

```rust
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

let connector = HttpConnector::new(); // またはHttpsConnectorBuilder
let hyper_client = HyperClient::builder(TokioExecutor::new())
    // クライアント側で特定のアイドルソケットを生かす時間（ミリ秒単位）。
    // これはClickHouseサーバーのKeepAliveタイムアウトよりもかなり短いことが想定されています。
    // これは、デフォルトで23.11バージョン以前の3秒、以降のバージョンの後10秒でした。
    .pool_idle_timeout(Duration::from_millis(2_500))
    // プール内で許可される最大のアイドルKeep-Alive接続。
    .pool_max_idle_per_host(4)
    .build(connector);

let client = Client::with_http_client(hyper_client).with_url("http://localhost:8123");
```

:::warning
この例はレガシーなHyper APIに依存しており、将来変更される可能性があります。
:::

参考：クライアントリポジトリの[カスタムHTTPクライアントの例](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/custom_http_client.rs)。

## データ型

:::info
追加の例も参照してください：
* [シンプルなClickHouseデータ型](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/data_types_derive_simple.rs)
* [コンテナ型のClickHouseデータ型](https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/data_types_derive_containers.rs)
:::

* `(U)Int(8|16|32|64|128)`は対応する`(u|i)(8|16|32|64|128)`型またはその周りの新しい型にマッピングします。
* `(U)Int256`は直接サポートされていませんが、[回避策があります](https://github.com/ClickHouse/clickhouse-rs/issues/48)。
* `Float(32|64)`は対応する`f(32|64)`またはその周りの新しい型にマッピングします。
* `Decimal(32|64|128)`は対応する`i(32|64|128)`またはその周りの新しい型にマッピングします。[fixnum](https://github.com/loyd/fixnum)や他の実装のサイン付き固定小数点数を使用することがより便利です。
* `Boolean`は`bool`またはその周りの新しい型にマッピングします。
* `String`は任意の文字列またはバイト型にマッピングします。例：`&str`, `&[u8]`, `String`, `Vec<u8>`または[`SmartString`](https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html)。新しい型もサポートされます。バイトを保存するには、[serde_bytes](https://docs.rs/serde_bytes/latest/serde_bytes/)を使用することを考慮してください。これはより効率的です。

```rust
#[derive(Row, Debug, Serialize, Deserialize)]
struct MyRow<'a> {
    str: &'a str,
    string: String,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    #[serde(with = "serde_bytes")]
    byte_slice: &'a [u8],
}
```

* `FixedString(N)`はバイトの配列としてサポートされます。例：`[u8; N]`。

```rust
#[derive(Row, Debug, Serialize, Deserialize)]
struct MyRow {
    fixed_str: [u8; 16], // FixedString(16)
}
```
* `Enum(8|16)`は[serde_repr](https://docs.rs/serde_repr/latest/serde_repr/)を使用してサポートされています。

```rust
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    level: Level,
}

#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
enum Level {
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}
```
* `UUID`は`serde::uuid`を使用して[`uuid::Uuid`](https://docs.rs/uuid/latest/uuid/struct.Uuid.html)にマッピングされます。`uuid`機能が必要です。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    #[serde(with = "clickhouse::serde::uuid")]
    uuid: uuid::Uuid,
}
```
* `IPv6`は[`std::net::Ipv6Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv6Addr.html)にマッピングされます。
* `IPv4`は`serde::ipv4`を使用して[`std::net::Ipv4Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv4Addr.html)にマッピングされます。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    #[serde(with = "clickhouse::serde::ipv4")]
    ipv4: std::net::Ipv4Addr,
}
```
* `Date`は`u16`またはその周りの新しい型にマッピングされ、 `1970-01-01`から経過した日数を表します。また、`serde::time::date`を使用して、[`time::Date`](https://docs.rs/time/latest/time/struct.Date.html)にマッピングされます。`time`機能が必要です。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    days: u16,
    #[serde(with = "clickhouse::serde::time::date")]
    date: Date,
}
```
* `Date32`は`i32`またはその周りの新しい型にマッピングされ、`1970-01-01`から経過した日数を表します。また、`serde::time::date32`を使用して、[`time::Date`](https://docs.rs/time/latest/time/struct.Date.html)にマッピングされます。`time`機能が必要です。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    days: i32,
    #[serde(with = "clickhouse::serde::time::date32")]
    date: Date,
}
```
* `DateTime`は`u32`またはその周りの新しい型にマッピングされ、UNIX時代から経過した秒数を表します。また、`serde::time::datetime`を使用して、[`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html)にマッピングされます。`time`機能が必要です。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    ts: u32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    dt: OffsetDateTime,
}
```

* `DateTime64(_)`は`i32`またはその周りの新しい型にマッピングされ、UNIX時代から経過した時間を表します。また、`serde::time::datetime64::*`を使用して、[`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html)にマッピングされます。`time`機能が必要です。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    ts: i64, // `DateTime64(X)`に応じた経過秒/μs/ms/ns
    #[serde(with = "clickhouse::serde::time::datetime64::secs")]
    dt64s: OffsetDateTime,  // `DateTime64(0)`
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    dt64ms: OffsetDateTime, // `DateTime64(3)`
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    dt64us: OffsetDateTime, // `DateTime64(6)`
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    dt64ns: OffsetDateTime, // `DateTime64(9)`
}
```

* `Tuple(A, B, ...)`は`(A, B, ...)`またはその周りの新しい型にマッピングされます。
* `Array(_)`は任意のスライス、例：`Vec<_>`, `&[_]`にマッピングされます。新しい型もサポートされます。
* `Map(K, V)`は`Array((K, V))`のように動作します。
* `LowCardinality(_)`はシームレスにサポートされます。
* `Nullable(_)`は`Option<_>`にマッピングされます。`clickhouse::serde::*`ヘルパーに対しては`::option`を追加します。

```rust
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    #[serde(with = "clickhouse::serde::ipv4::option")]
    ipv4_opt: Option<Ipv4Addr>,
}
```
* `Nested`はリネーミングを使用して複数の配列を提供することでサポートされます。
```rust
// CREATE TABLE test(items Nested(name String, count UInt32))
#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    #[serde(rename = "items.name")]
    items_name: Vec<String>,
    #[serde(rename = "items.count")]
    items_count: Vec<u32>,
}
```
* `Geo`型はサポートされています。 `Point`はタプル`(f64, f64)`として動作し、その他の型は単に点のスライスです。
```rust
type Point = (f64, f64);
type Ring = Vec<Point>;
type Polygon = Vec<Ring>;
type MultiPolygon = Vec<Polygon>;
type LineString = Vec<Point>;
type MultiLineString = Vec<LineString>;

#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    point: Point,
    ring: Ring,
    polygon: Polygon,
    multi_polygon: MultiPolygon,
    line_string: LineString,
    multi_line_string: MultiLineString,
}
```

* `Variant`、`Dynamic`、（新しい）`JSON`データ型はまだサポートされていません。

## モック

このクレートは、CHサーバーをモックし、DDL、`SELECT`、`INSERT`、`WATCH`クエリをテストするためのユーティリティを提供します。この機能は`test-util`機能で有効になります。**dev-dependency**としてのみ使用してください。

参考：[例](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs)。

## トラブルシューティング

### CANNOT_READ_ALL_DATA

`CANNOT_READ_ALL_DATA`エラーの最も一般的な原因は、アプリケーション側の行定義がClickHouseのものと一致していないことです。

次のようなテーブルを考えてみます：

```
CREATE OR REPLACE TABLE event_log (id UInt32)
ENGINE = MergeTree
ORDER BY timestamp
```

そして、アプリケーション側で`EventLog`が次のように定義されている場合、型が一致していません：

```rust
#[derive(Debug, Serialize, Deserialize, Row)]
struct EventLog {
    id: String, // <- 本来はu32であるべき！
}
```

データを挿入すると、次のエラーが発生する可能性があります：

```
Error: BadResponse("Code: 33. DB::Exception: Cannot read all data. Bytes read: 5. Bytes expected: 23.: (at row 1)\n: While executing BinaryRowInputFormat. (CANNOT_READ_ALL_DATA)")
```

この例を正確にするためには、`EventLog`構造体を次のように正しく定義します：

```rust
#[derive(Debug, Serialize, Deserialize, Row)]
struct EventLog {
    id: u32
}
```

## 既知の制限

* `Variant`、`Dynamic`、（新しい）`JSON`データ型はまだサポートされていません。
* サーバー側のパラメータバインディングはまだサポートされていません。追跡するために[この問題](https://github.com/ClickHouse/clickhouse-rs/issues/142)を参照してください。

## お問い合わせ

質問がある場合や手助けが必要な場合は、[Community Slack](https://clickhouse.com/slack)や[GitHub issues](https://github.com/ClickHouse/clickhouse-rs/issues)を通じてお気軽にお問い合わせください。
