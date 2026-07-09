---
description: 'WebAssembly ユーザー定義関数に関するドキュメント'
sidebar_label: 'WebAssembly UDF'
slug: /sql-reference/functions/wasm_udf
title: 'WebAssembly ユーザー定義関数'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # WebAssembly ユーザー定義関数
</div>

ClickHouse では、WebAssembly で記述したユーザー定義関数 (UDF) を作成できます。これにより、Rust、C、C++ などで記述したカスタムロジックを WebAssembly モジュールにコンパイルして実行できます。

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## 概要
</div>

WebAssembly モジュールは、ClickHouse から呼び出せる 1 つ以上の関数を含む、コンパイル済みのバイナリファイルです。
モジュールは、一度読み込んで何度も再利用するライブラリや共有オブジェクトのようなものと考えてください。

UDF を含む WebAssembly モジュールは、Rust、C、C++ など、WebAssembly にコンパイルできる任意の言語で記述できます。

WebAssembly にコンパイルされたコード (「guest」コード) と、ClickHouse によって実行される側 (「host」) は、専用のメモリ空間にのみアクセスできるサンドボックス化された環境で動作します。

ゲストコードは、ClickHouse が呼び出せる関数をエクスポートします。これには、独自のロジックを実装する関数 (UDF の定義に使用) だけでなく、メモリ管理や ClickHouse と WebAssembly コード間のデータ交換に必要なサポート関数も含まれます。

コードは、オペレーティングシステムや標準ライブラリに依存しない「freestanding」WebAssembly (別名 `wasm32-unknown-unknown`) としてコンパイルする必要があります。また、サポートされるのはデフォルトの 32 ビット WebAssembly ターゲットのみです (`wasm64` 拡張機能は非対応) 。
モジュールは、ClickHouse とやり取りするために、サポートされている通信プロトコル (ABI) のいずれかに従う必要があります。

コンパイル後、モジュールのバイナリコードは `system.webassembly_modules` テーブルに挿入することで ClickHouse に読み込まれます。
その後、`CREATE FUNCTION ... LANGUAGE WASM` ステートメントを使用して、モジュールがエクスポートした関数を参照する UDF を作成できます。

<div id="prerequisites">
  ## 前提条件
</div>

ClickHouse の設定で WebAssembly サポートを有効にします：

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

利用可能なエンジンの実装:

* `wasmtime` (デフォルト、推奨) — [WasmTime](https://github.com/bytecodealliance/wasmtime) を使用
* `wasmedge` — [WasmEdge](https://github.com/WasmEdge/WasmEdge) を使用

<div id="quick-start">
  ## クイックスタート
</div>

この例では、[Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture) の計算機を実装しながら、WebAssembly UDF を作成する一連のワークフロー全体を紹介します。

コードは WebAssembly Text format (WAT) で記述します。これは WebAssembly の人間が読める表現であるため、この段階では特定のプログラミング言語は必要ありません。
ClickHouse ではモジュールがバイナリ形式である必要があるため、トランスパイラを使用して WAT を WASM に変換します。
この変換を行うには、[WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) の `wat2wasm`、または [wasm-tools](https://github.com/bytecodealliance/wasm-tools) の `parse` コマンドを使用できます。

```bash
cat << 'EOF' | wasm-tools parse | clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
(module
  (func $next (param $n i32) (result i32)
    local.get $n i32.const 1 i32.and
    (if (result i32)
      (then local.get $n i32.const 3 i32.mul i32.const 1 i32.add)
      (else local.get $n i32.const 2 i32.div_u)))
  (func $steps (export "steps") (param $n i32) (result i32)
    (local $count i32)
    local.get $n i32.const 1 i32.lt_u
    (if (then i32.const 0 return))
    (block $done (loop $loop
      local.get $n i32.const 1 i32.eq br_if $done
      local.get $n call $next local.set $n
      local.get $count i32.const 1 i32.add local.set $count
      br $loop))
    local.get $count)
)
EOF
```

上のスニペットでは、`FORMAT RawBlob` を使用してバイナリの WASM コードを `clickhouse client` に直接パイプし、`system.webassembly_modules` テーブルに挿入しています。

次に、モジュールがエクスポートする `steps` 関数を参照する UDF を定義します。

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

UDF名とは異なるため、`::` の後にはモジュール内の関数名を指定している点に注意してください。

これで、クエリ内で `collatz_steps` 関数を使用できます。

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

`number` カラムは `UInt32` に明示的にキャストされています。これは、WebAssembly 関数が `CREATE FUNCTION` ステートメントで指定されたシグネチャどおりに、型が厳密に一致していることを前提としているためです。

その結果、1 から 100 までの数値に対する Collatz のステップ数からなる数列が得られ、これは [OEIS の数列 A006577](https://oeis.org/A006577) に対応しています。

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## system table 経由で WASM モジュールを管理する
</div>

WebAssembly モジュールは、次の構造を持つ `system.webassembly_modules` テーブルに保存されます。

* **カラム**
  * `name` String — モジュール名。空は不可で、使用できるのは英数字とアンダースコアのみです。
  * `code` String — 生の WASM バイナリコード。書き込み専用で、読み出すと空文字列が返されます。
  * `hash` UInt256 — モジュールバイナリの SHA256 (ディスク上には存在するものの、まだ読み込まれていない場合はゼロ) 。

モジュールの管理は、このテーブルに対する標準的な SQL 操作で行います。

<div id="insert-a-module">
  ### モジュールを追加する
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

必要に応じて、整合性確認用ハッシュを指定します:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

指定されたハッシュがモジュールコードから計算されたSHA256と一致しない場合、挿入に失敗します。これは、S3 や HTTP などの外部ソースからモジュールを読み込む場合に役立つことがあります。

<div id="distribute-a-module-across-a-cluster">
  ### クラスター全体にモジュールを配布する
</div>

`system.webassembly_modules` はインスタンス単位のテーブルであり、`INSERT` は接続を処理しているレプリカにしか反映されません。`INSERT` ステートメントには `ON CLUSTER` 形式がないため、その後に `CREATE FUNCTION ... ON CLUSTER` を実行すると、モジュールを持たないレプリカでは失敗します。

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

insertをすべてのノードに送るには、ローカルの`system.webassembly_modules`テーブルではなく、`cluster`テーブル関数に書き込みます。

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
このパターンは、基盤となる分散書き込みパスが各分片内のすべてのレプリカをたどることを前提としていますが、これが起きるのは、クラスターが `internal_replication=false` に設定されている場合だけです。`internal_replication=true` の場合 (`ReplicatedMergeTree` を使ってレプリケーションを自前で行うクラスターのデフォルト設定) 、インサートは各分片につき正常なレプリカ 1 つにだけ送られます。また、`system.webassembly_modules` はこの経路ではレプリケーションされないため、一部のレプリカにはモジュールがないままです。この構成では、各レプリカに対して個別にインサートする必要があります。たとえば、`system.clusters` を反復処理してホストごとに `remote(...)` 経由で書き込むか、すべてのホストの `user_scripts/wasm/` にバイナリをコピーします。

クラスターの `internal_replication` は、`SELECT cluster, shard_num, internal_replication FROM system.clusters` で確認できます。
:::

ファンアウトしたインサートの後は、モジュールがすべてのレプリカに存在するため、`CREATE FUNCTION ... ON CLUSTER` は成功します。

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

`clusterAllReplicas` を使うと、すべてのレプリカでモジュールが読み込まれていることを確認できます。

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

`system.webassembly_modules` への insert は、同じ `(name, hash)` の組み合わせに対しては冪等です。そのため、分散された insert を再実行しても安全であり、レプリカの置き換え後に状態を修復するための妥当な方法でもあります。新たに追加されたサーバーには既存のモジュールがさかのぼって配布されることはない点に注意してください。更新後のクラスターに対して insert を再実行するか、新しいホストの `user_scripts/wasm/` ディレクトリにバイナリを配置する必要があります。

<div id="list-modules">
  ### モジュールの一覧を表示
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### モジュールを削除する
</div>

削除は、`DELETE FROM system.webassembly_modules WHERE name = '...'` ステートメントで実行します。
条件式には、完全一致の場合は `name = 'literal'`、名前がパターンに一致するすべてのモジュールを削除する場合は `name LIKE 'pattern'` のいずれかを指定する必要があり、これ以外の形式は受け付けられません。

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

既存のUDFが該当するモジュールのいずれかを参照している場合、削除は失敗するため、先にそれらのUDFを削除する必要があります。

<div id="create-a-webassembly-udf">
  ## WebAssembly UDF を作成する
</div>

**構文**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1 | ABI ASSEMBLYSCRIPT]
[DETERMINISTIC]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**パラメーター**:

* `function_name`: ClickHouse 内の関数名。モジュール内のエクスポートされた関数名とは異なる場合があります。
* `FROM 'module_name' :: 'source_function_name'`: 使用する、読み込み済み WASM モジュール名と WASM モジュール内の関数名 (既定では function&#95;name)
* `ARGUMENTS`: argument 名と型の一覧 (名前は省略可能で、名前付きフィールドをサポートするシリアライゼーションフォーマットで使用されます)
* `ABI`: Application Binary Interface のバージョン
  * `ROW_DIRECT`: 直接の型マッピングによる、行単位の処理
  * `BUFFERED_V1`: シリアライゼーションを伴うブロックベースの処理
  * `ASSEMBLYSCRIPT`: [AssemblyScript](https://www.assemblyscript.org) コンパイラで生成されたモジュール向けの行単位処理。数値型は AssemblyScript の primitive 型にマッピングされ、ClickHouse `String` は AssemblyScript `string` にマッピングされます。
* `DETERMINISTIC`: 関数を決定論的として宣言します。つまり、同じ入力に対して常に同じ出力を返します。指定すると、すべての引数が定数である呼び出しについて、ClickHouse は定数畳み込みを行うことがあります。関数はクエリ分析時に一度だけ評価され、その結果がすべての行で再利用されます。
* `SHA256_HASH`: 検証用の想定モジュールハッシュです (省略した場合は自動設定されます) 。異なるレプリカ間で正しい WASM モジュールが読み込まれていることを確認するために使用できます。
* `SETTINGS`: 関数ごとの設定
  * `serialization_format` String — ABI で必要なシリアライゼーションフォーマットです。サポートされる値: `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary`, `Buffers`。既定値: `MsgPack`。`Buffers` などのブロックベースのフォーマットでは、宣言された関数シグネチャに一致する型の単一カラムを返す必要があります。
  * `webassembly_udf_enable_fuel` Bool — 関数の有限 fuel 予算を有効にします。既定値: `true`。`false` の場合、この関数ではクエリレベル設定 `webassembly_udf_max_fuel` は無視されます。fuel 制限を無効にすると、`wasmtime` engine 使用時のパフォーマンスが向上する場合があります。ただし、信頼できないゲストコードやバグのあるゲストコードでは、暴走実行のリスクが高まる可能性があります。

<div id="abis-versions">
  ## ABI のバージョン
</div>

ClickHouse と連携するには、WebAssembly モジュールがサポートされている ABI (Application Binary Interface) のいずれかに準拠している必要があります。

* `ROW_DIRECT`: 直接の型マッピング (primitive types `Int32`、`UInt32`、`Int64`、`UInt64`、`Float32`、`Float64` のみ)
* `BUFFERED_V1`: シリアライゼーションを伴う複雑な型
* `ASSEMBLYSCRIPT`: [AssemblyScript](https://www.assemblyscript.org) モジュールとの行単位の相互運用。数値型と `String` をサポートします。

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

エクスポートされたWASM関数を、各行に対して直接呼び出します。

* 引数と戻り値の型には、数値型 `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128` を使用します。
* このABIでは `String` はサポートされていません。
* シグネチャは、WASMのエクスポート (`i32/i64/f32/f64/v128`) と一致している必要があります。
* モジュールでエクスポートが必要な補助関数はありません。

たとえば、次のシグネチャを持つ関数です。

```
(func (param i32 i64 f32) (result f64) ...)
```

以下のように作成できます:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly では、符号付き引数と符号なし引数は区別されず、代わりに値の解釈に異なる命令が使われます。そのため、引数のサイズは正確に一致している必要があり、符号の有無は関数内の操作によって決まります。

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
この ABI は実験的なものであり、将来のリリースで変更される可能性があります。
:::

WASM メモリを介した (デ) シリアライゼーションによって、ブロック全体を一度に処理します。任意の引数と戻り値の型をサポートします。

シリアライズされたデータは wasm メモリにコピーされ、バッファへのポインタ (データへのポインタとデータサイズで構成されます) が入力の行数とともに UDF 関数に渡されます。したがって、wasm 上のユーザー定義関数は常に 2 つの `i32` 引数を受け取り、1 つの `i32` 値を返します。
ゲストコードはデータを処理し、シリアライズされた結果データを含む結果バッファへのポインタを返します。

ゲストコードは、これらのバッファを作成および破棄するための 2 つの関数を提供する必要があります。

```
(module
  ;; Allocate a new buffer of specified size
  ;; Returns: handle to Buffer structure (not direct data pointer!) with pointer to data and size
  (func (export "clickhouse_create_buffer")
    (param $size i32)    ;; Size of data to allocate
    (result i32))        ;; Returns buffer handle with enough space

  ;; Free a buffer by its handle
  (func (export "clickhouse_destroy_buffer")
    (param $handle i32)  ;; Buffer handle to free
    (result))            ;; No return value

    ;; User-defined function
    (func (export "user_defined_function1")
      (param $input_buffer_handle i32)  ;; Input buffer handle
      (param $n i32)                    ;; Number of rows in input
      (result i32))                     ;; Returns output buffer handle
)
```

C 定義の例:

```c
typedef struct {
    uint8_t * data;
    uint32_t size;
} ClickhouseBuffer;

ClickhouseBuffer * clickhouse_create_buffer(uint32_t size) { /* ... */ }

void clickhouse_destroy_buffer(ClickhouseBuffer * data) { /* ... */ }

/// Example user-defined functions
ClickhouseBuffer * user_defined_function1(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
ClickhouseBuffer * user_defined_function2(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
```

<div id="abi-assemblyscript">
  ### ABI ASSEMBLYSCRIPT
</div>

[AssemblyScript](https://www.assemblyscript.org) コンパイラで生成されたモジュールを対象とします。各行ごとにエクスポートされた関数が 1 回呼び出され、ClickHouse の値は AssemblyScript の基本型および文字列オブジェクトにマッピングされます。

**サポートされる型**:

* 数値: `Int8`/`UInt8`、`Int16`/`UInt16` (境界では `i32` に拡張) 、`Int32`/`UInt32`、`Int64`/`UInt64`、`Float32`、`Float64`

* `String` — AssemblyScript の `string` (WASM メモリ上では UTF-16) にマッピングされます。ClickHouse は UTF-8 ↔ UTF-16 の変換を自動的に処理します。

* カスタム AssemblyScript クラスは、引数型または戻り値型としてはサポートされていません。これらのランタイムクラス ID はコンパイルごとに安定しないためです ([AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982) を参照) 。

**モジュール要件**:

モジュールは、`__new`、`__pin`、`__unpin` がエクスポートされるように、AssemblyScript managed runtime でコンパイルする必要があります。標準の入出力文字列処理ではこれらが必要です。推奨される呼び出しは次のとおりです。

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript は、ランタイムトラップ (メモリ不足、境界チェックなど) 用に `env.abort` もインポートします。ClickHouse はこのインポートを自動的に提供します。`abort` がトリガーされると、実行中のクエリは `WASM_ERROR` 例外で失敗し、その例外にはデコードされた AssemblyScript のメッセージとソース位置が含まれます。

**例**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

`asc` でコンパイルし、生成された `.wasm` を `system.webassembly_modules` に読み込んだ後、UDF は次のように宣言します:

```sql
CREATE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;
```

<div id="note-for-developing-udfs-in-rust">
  ### RustでUDFを開発する際の注意点
</div>

Rustプログラム向けに、ClickHouse向けの WebAssembly UDF の開発を簡単にするヘルパー crate [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) を提供しています。この crate にはメモリ管理用の関数が含まれているため、`clickhouse_create_buffer` と `clickhouse_destroy_buffer` を手動で実装する必要はなく、依存関係として crate を追加するだけで済みます。また、通常の Rust 関数を必要な ABI 形式でラップするためのマクロ `#[clickhouse_wasm_udf]` も用意されています。

この crate を使うと、次のように UDF を記述できます。

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

マクロは、バッファ構造体を受け取り、返すラッパー関数を生成し、`serde` を使用してシリアライゼーションとデシリアライゼーションを自動的に処理します。

<div id="host-api-available-to-modules">
  ## モジュールで利用可能なホスト API
</div>

モジュールは、以下のホスト関数をインポートして使用できます。

* `clickhouse_server_version() -> i64` — ClickHouse server のバージョンを整数で返します (例: v25.11.1.1 の場合は 25011001) 。
* `clickhouse_throw(ptr: i32, size: i32)` — 指定されたメッセージでエラーを発生させます。エラーメッセージ文字列を含むメモリ位置へのポインタと、文字列サイズを受け取ります。
* `clickhouse_log(ptr: i32, size: i32)` — メッセージを ClickHouse server のテキストログに記録します。
* `clickhouse_random(ptr: i32, size: i32)` — メモリをランダムなバイトで埋めます。
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — AssemblyScript 互換モジュール向けに提供されています。これを呼び出すと (または、これを呼び出す AssemblyScript ランタイムトラップがトリガーされると) 、デコードされたメッセージとソース位置を含む `WASM_ERROR` 例外によって UDF が終了します。`env.abort` をインポートしないモジュールには影響しません。

<div id="settings">
  ## 設定
</div>

以下のクエリレベルの設定は、WebAssembly UDF の実行を制御します。

* `webassembly_udf_max_fuel` — WebAssembly UDF インスタンスの実行ごとの fuel 上限です。各 WebAssembly 命令は一定量の fuel を消費します。値は runtime に渡される前に 1024 倍されるため、`webassembly_udf_max_fuel = 1` はおよそ 1024 fuel 単位に相当します。有限の制限を設けない場合は 0 に設定します。これは、関数ごとの設定 `webassembly_udf_enable_fuel` が true の関数にのみ適用されます。この設定のデフォルト値は true です。

* `webassembly_udf_max_memory` — WebAssembly UDF インスタンスごとのバイト単位のメモリ制限です。

* `webassembly_udf_max_input_block_size` — 1 つの block で WebAssembly UDF に渡される最大行数です。すべての行を一度に処理するには 0 に設定します。

* `webassembly_udf_max_instances` — 関数ごとに並列に実行できる WebAssembly UDF インスタンスの最大数です。

使用例:

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## 関連項目
</div>

* [ClickHouse UDFの概要](/ja/sql-reference/functions/udf)