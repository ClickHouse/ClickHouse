---
slug: /ja/sql-reference/functions/other-functions
sidebar_position: 140
sidebar_label: Other
---

# その他の関数

## hostName

この関数が実行されたホスト名を返します。関数がリモートサーバー上で実行される場合（分散処理）には、リモートサーバーの名前が返されます。  
関数が分散テーブルのコンテキストで実行された場合、各シャードに関連する値を持つ通常のカラムを生成します。そうでない場合は定数値を生成します。

**構文**

```sql
hostName()
```

**戻り値**

- ホスト名。[String](../data-types/string.md)。

## getMacro {#getMacro}

サーバー構成の[マクロ](../../operations/server-configuration-parameters/settings.md#macros)セクションから名前付きの値を返します。

**構文**

```sql
getMacro(name);
```

**引数**

- `name` — `<macros>`セクションから取得するマクロ名。[String](../data-types/string.md#string)。

**戻り値**

- 指定されたマクロの値。[String](../data-types/string.md)。

**例**

サーバー構成ファイル内の`<macros>`セクションの例：

```xml
<macros>
    <test>Value</test>
</macros>
```

クエリ：

```sql
SELECT getMacro('test');
```

結果：

```text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

同じ値を以下のクエリでも取得できます：

```sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

```text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## fqdn

ClickHouseサーバーの完全修飾ドメイン名を返します。

**構文**

```sql
fqdn();
```

別名: `fullHostName`, `FQDN`.

**戻り値**

- 完全修飾ドメイン名の文字列。[String](../data-types/string.md)。

**例**

```sql
SELECT FQDN();
```

結果：

```text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename

文字列の最後のスラッシュまたはバックスラッシュの後ろの部分を抽出します。この関数は通常、パスからファイル名を抽出するのに使用されます。

```sql
basename(expr)
```

**引数**

- `expr` — [String](../data-types/string.md)型の値。バックスラッシュはエスケープする必要があります。

**戻り値**

入力文字列の最後のスラッシュまたはバックスラッシュの後の部分を含む文字列。入力文字列がスラッシュまたはバックスラッシュで終わっている場合（例：`/`や`c:\`）、関数は空の文字列を返します。スラッシュやバックスラッシュがない場合は元の文字列を返します。

**例**

クエリ：

```sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

結果：

```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

クエリ：

```sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

結果：

```text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

クエリ：

```sql
SELECT 'some-file-name' AS a, basename(a)
```

結果：

```text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth

テキスト形式（タブ区切り）で値をコンソールに出力するときの幅をおおよそ計算します。この関数はシステムによって[Prettyフォーマット](../../interfaces/formats.md)を実装するために使用されます。

`NULL`は`Pretty`フォーマットでの`NULL`に対応する文字列として表現されます。

**構文**

```sql
visibleWidth(x)
```

**例**

クエリ：

```sql
SELECT visibleWidth(NULL)
```

結果：

```text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName

渡された引数の型名を返します。

`NULL`が渡された場合、関数はClickHouseの内部`NULL`表現に対応する`Nullable(Nothing)`型を返します。

**構文**

```sql
toTypeName(value)
```

**引数**

- `value` — 任意の型の値。

**戻り値**

- 入力値のデータ型名。[String](../data-types/string.md)。

**例**

クエリ：

```sql
SELECT toTypeName(123);
```

結果：

```response
┌─toTypeName(123)─┐
│ UInt8           │
└─────────────────┘
```

## blockSize {#blockSize}

ClickHouseでは、[ブロック](../../development/architecture.md/#block-block)（チャンク）でクエリが処理されます。この関数は、関数が呼び出されたブロックのサイズ（行数）を返します。

**構文**

```sql
blockSize()
```

**例**

クエリ：

```sql
DROP TABLE IF EXISTS test;
CREATE TABLE test (n UInt8) ENGINE = Memory;

INSERT INTO test
SELECT * FROM system.numbers LIMIT 5;

SELECT blockSize()
FROM test;
```

結果：

```response
   ┌─blockSize()─┐
1. │           5 │
2. │           5 │
3. │           5 │
4. │           5 │
5. │           5 │
   └─────────────┘
```

## byteSize

引数の非圧縮バイトサイズの推定値を返します。

**構文**

```sql
byteSize(argument [, ...])
```

**引数**

- `argument` — 値。

**戻り値**

- メモリ内の引数のバイトサイズの推定値。[UInt64](../data-types/int-uint.md)。

**例**

[String](../data-types/string.md)引数の場合、関数は文字列の長さ+9（終了ゼロ+長さ）を返します。

クエリ：

```sql
SELECT byteSize('string');
```

結果：

```text
┌─byteSize('string')─┐
│                 15 │
└────────────────────┘
```

クエリ：

```sql
CREATE TABLE test
(
    `key` Int32,
    `u8` UInt8,
    `u16` UInt16,
    `u32` UInt32,
    `u64` UInt64,
    `i8` Int8,
    `i16` Int16,
    `i32` Int32,
    `i64` Int64,
    `f32` Float32,
    `f64` Float64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test VALUES(1, 8, 16, 32, 64,  -8, -16, -32, -64, 32.32, 64.64);

SELECT key, byteSize(u8) AS `byteSize(UInt8)`, byteSize(u16) AS `byteSize(UInt16)`, byteSize(u32) AS `byteSize(UInt32)`, byteSize(u64) AS `byteSize(UInt64)`, byteSize(i8) AS `byteSize(Int8)`, byteSize(i16) AS `byteSize(Int16)`, byteSize(i32) AS `byteSize(Int32)`, byteSize(i64) AS `byteSize(Int64)`, byteSize(f32) AS `byteSize(Float32)`, byteSize(f64) AS `byteSize(Float64)` FROM test ORDER BY key ASC FORMAT Vertical;
```

結果：

```text
Row 1:
──────
key:               1
byteSize(UInt8):   1
byteSize(UInt16):  2
byteSize(UInt32):  4
byteSize(UInt64):  8
byteSize(Int8):    1
byteSize(Int16):   2
byteSize(Int32):   4
byteSize(Int64):   8
byteSize(Float32): 4
byteSize(Float64): 8
```

関数に複数の引数がある場合、関数はそれらのバイトサイズを累積します。

クエリ：

```sql
SELECT byteSize(NULL, 1, 0.3, '');
```

結果：

```text
┌─byteSize(NULL, 1, 0.3, '')─┐
│                         19 │
└────────────────────────────┘
```

## materialize

定数を単一の値を含む完全なカラムに変換します。完全なカラムと定数はメモリ内で異なる形で表現されます。  
通常、関数は通常の引数と定数引数に対して異なるコードを実行しますが、結果は通常同じであるべきです。  
この関数はこの動作をデバッグするために使用できます。

**構文**

```sql
materialize(x)
```

**パラメータ**

- `x` — 定数。[Constant](../functions/index.md/#constants)。

**戻り値**

- 単一値`x`を含むカラム。

**例**

以下の例では、`countMatches`関数は定数の第2引数を期待しています。この動作は、定数を完全なカラムに変換する`materialize`関数を使用してデバッグできます。この関数は定数でない引数に対してエラーを投げます。

クエリ：

```sql
SELECT countMatches('foobarfoo', 'foo');
SELECT countMatches('foobarfoo', materialize('foo'));
```

結果：

```response
2
Code: 44. DB::Exception: Received from localhost:9000. DB::Exception: Illegal type of argument #2 'pattern' of function countMatches, expected constant String, got String
```

## ignore

任意の引数を受け取り、無条件に`0`を返します。引数は内部的に評価され続けるため、たとえばベンチマークに役立ちます。

**構文**

```sql
ignore([arg1[, arg2[, ...]])
```

**引数**

- 任意の型、`NULL`を含む任意の多数の引数を受け入れることができます。

**戻り値**

- `0`を返します。

**例**

クエリ：

```sql
SELECT ignore(0, 'ClickHouse', NULL);
```

結果：

```response
┌─ignore(0, 'ClickHouse', NULL)─┐
│                             0 │
└───────────────────────────────┘
```

## sleep

クエリの実行に遅延や一時停止を導入するために使われます。主にテストやデバッグの目的で使用されます。

**構文**

```sql
sleep(seconds)
```

**引数**

- `seconds`: [UInt*](../data-types/int-uint.md) または [Float](../data-types/float.md) クエリ実行を一時停止する秒数。最大3秒。小数秒を指定するために浮動小数点値を使用できます。

**戻り値**

この関数は値を返しません。

**例**

```sql
SELECT sleep(2);
```

この関数は値を返しません。しかし、`clickhouse client`で関数を実行すると、以下のような情報が表示されます：

```response
SELECT sleep(2)

Query id: 8aa9943e-a686-45e1-8317-6e8e3a5596ac

┌─sleep(2)─┐
│        0 │
└──────────┘

1 row in set. Elapsed: 2.012 sec.
```

このクエリは2秒間の一時停止の後に完了します。この間、結果は返されず、クエリは停止しているか非応答であるように見えます。

**実装の詳細**

`sleep()`関数は通常、クエリのパフォーマンスやシステムの応答性に悪影響を及ぼすため、本番環境では使用されません。しかし、以下のような場合に役立つことがあります：

1. **テスト**：ClickHouseをテストやベンチマークする際に、遅延をシミュレーションしたり、一時停止を導入してシステムが特定の条件下でどのように動作するかを観察できます。
2. **デバッグ**：システムの状態やクエリの実行を特定の時点で調べる必要がある場合、`sleep()`を使って一時停止を導入し、関連情報を調べたり収集したりすることができます。
3. **シミュレーション**：ネットワーク遅延や外部システム依存など、本番環境での遅延や一時停止をシミュレートしたい場合があります。

ClickHouseシステムの全体的なパフォーマンスや応答性に影響を与える可能性があるため、`sleep()`関数は必要な場合にのみ、慎重に使用してください。

## sleepEachRow

各行の結果セットに対して指定した秒数だけクエリの実行を一時停止します。

**構文**

```sql
sleepEachRow(seconds)
```

**引数**

- `seconds`: [UInt*](../data-types/int-uint.md) または [Float*](../data-types/float.md) 各行の結果セットのクエリ実行を一時停止する秒数。最大3秒。小数秒を指定するために浮動小数点値を使用できます。

**戻り値**

この関数は入力値を変更せずにそのまま返します。

**例**

```sql
SELECT number, sleepEachRow(0.5) FROM system.numbers LIMIT 5;
```

```response
┌─number─┬─sleepEachRow(0.5)─┐
│      0 │                 0 │
│      1 │                 0 │
│      2 │                 0 │
│      3 │                 0 │
│      4 │                 0 │
└────────┴───────────────────┘
```

ただし、出力は各行ごとに0.5秒の遅延付きで表示されます。

`sleepEachRow()`関数は、主に`sleep()`関数と同様にテストやデバッグの目的で使用されます。  
各行の処理に遅延や一時停止をシミュレーションすることができ、次のようなシナリオで役立ちます：

1. **テスト**：特定の条件下でのClickHouseのパフォーマンスをテストまたはベンチマークする際に、`sleepEachRow()`を使用して各行処理に遅延や一時停止をシミュレーションできます。
2. **デバッグ**：各行処理時のシステムの状態やクエリの実行を確認する際に、`sleepEachRow()`を使用して一時停止を挿入し、関連情報を調べたり収集したりできます。
3. **シミュレーション**：外部システムやネットワーク遅延など、各行処理時の遅延や停止をシミュレートしたい場合があります。

[`sleep()`関数](#sleep)と同様に、特に大きな結果セットを扱う場合ClickHouseシステムのパフォーマンスや応答性に大きく影響する可能性があるため、`sleepEachRow()`の使用には注意が必要です。


## currentDatabase

現在のデータベースの名前を返します。  
`CREATE TABLE`クエリのテーブルエンジンのパラメータでデータベースを指定する必要がある場合に便利です。

**構文**

```sql
currentDatabase()
```

**戻り値**

- 現在のデータベースの名前を返します。[String](../data-types/string.md)。

**例**

クエリ：

```sql
SELECT currentDatabase()
```

結果：

```response
┌─currentDatabase()─┐
│ default           │
└───────────────────┘
```

## currentUser {#currentUser}

現在のユーザーの名前を返します。分散クエリの場合、クエリを開始したユーザーの名前が返されます。

**構文**

```sql
currentUser()
```

別名: `user()`, `USER()`, `current_user()`。別名は大文字小文字を区別しません。

**戻り値**

- 現在のユーザーの名前。[String](../data-types/string.md)。
- 分散クエリの場合、クエリを開始したユーザーのログイン。[String](../data-types/string.md)。

**例**

```sql
SELECT currentUser();
```

結果：

```text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## currentSchemas

現在のデータベーススキーマの名前を含む単一要素の配列を返します。

**構文**

```sql
currentSchemas(bool)
```

別名: `current_schemas`.

**引数**

- `bool`: ブール値。[Bool](../data-types/boolean.md)。

:::note
ブール引数は無視されます。これは、PostgreSQLでの[実装](https://www.postgresql.org/docs/7.3/functions-misc.html)との互換性のために存在します。
:::

**戻り値**

- 現在のデータベース名を含む単一要素の配列を返します。

**例**

```sql
SELECT currentSchemas(true);
```

結果：

```response
['default']
```

## isConstant

引数が定数式かどうかを返します。

定数式とはクエリ解析中、つまり実行前に結果が判明している式を指します。  
例えば、[リテラル](../../sql-reference/syntax.md#literals)に関する式は定数式です。

この関数は主に開発、デバッグ、およびデモンストレーションを目的としています。

**構文**

```sql
isConstant(x)
```

**引数**

- `x` — チェックする式。

**戻り値**

- `x`が定数であれば`1`。[UInt8](../data-types/int-uint.md)。
- `x`が非定数であれば`0`。[UInt8](../data-types/int-uint.md)。

**例**

クエリ：

```sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

結果：

```text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

クエリ：

```sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

結果：

```text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

クエリ：

```sql
SELECT isConstant(number) FROM numbers(1)
```

結果：

```text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## hasColumnInTable

データベース名、テーブル名、カラム名を定数文字列として指定すると、指定カラムが存在する場合は1、存在しない場合は0を返します。

**構文**

```sql
hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’)
```

**パラメータ**

- `database` : データベース名。[文字列リテラル](../syntax#syntax-string-literal)
- `table` : テーブル名。[文字列リテラル](../syntax#syntax-string-literal)
- `column` : カラム名。[文字列リテラル](../syntax#syntax-string-literal)
- `hostname` : チェックを行うリモートサーバーの名前。[文字列リテラル](../syntax#syntax-string-literal)
- `username` : リモートサーバーのユーザー名。[文字列リテラル](../syntax#syntax-string-literal)
- `password` : リモートサーバーのパスワード。[文字列リテラル](../syntax#syntax-string-literal)

**戻り値**

- 指定カラムが存在する場合は`1`。
- そうでない場合は`0`。

**実装の詳細**

ネストされたデータ構造の要素の場合、関数はカラムの存在を確認します。ネストされたデータ構造そのものについては、関数は0を返します。

**例**

クエリ：

```sql
SELECT hasColumnInTable('system','metrics','metric')
```

```response
1
```

```sql
SELECT hasColumnInTable('system','metrics','non-existing_column')
```

```response
0
```

## hasThreadFuzzer

スレッドファザーが有効であるかどうかを返します。テストで実行が長くなりすぎるのを防ぐために使用できます。

**構文**

```sql
hasThreadFuzzer();
```

## bar

棒グラフを構築します。

`bar(x, min, max, width)`は、幅が`(x - min)`に比例し、`x = max`のときに`width`文字となるバーを描画します。

**引数**

- `x` — 表示するサイズ。
- `min, max` — 整数の定数。値は`Int64`に収まる必要があります。
- `width` — 定数の正の整数であり、分数値を指定できます。

バーはシンボルの1/8の精度で描画されます。

例：

```sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

```text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## transform

要素のいくつかを他のものに明示的に定義されたマッピングに従って、値を変換します。  
この関数には2つのバリエーションがあります：

### transform(x, array_from, array_to, default)

`x` – 変換対象。

`array_from` – 変換する値の定数配列。

`array_to` – `from`の値を変換するための定数配列。

`default` – `x`が`from`内のいずれの値とも等しくない場合に使用する値。

`array_from`と`array_to`は同じ数の要素を持っている必要があります。

シグネチャ：

`x`が`array_from`の要素の一つに等しい場合、関数は`array_to`の対応する要素を返します。  
すなわち、同じ配列インデックスの要素です。そうでない場合、`default`を返します。`array_from`に複数の一致する要素が存在する場合、最初の要素に対応するものを返します。

`transform(T, Array(T), Array(U), U) -> U`

`T`と`U`は数値型、文字列型、またはDateやDateTime型のいずれかです。 同じ文字（TまたはU）とは、型が相互に互換性があるが、必ずしも等しいわけではないことを意味します。  
例として、最初の引数が`Int64`型で、2番目の引数が`Array(UInt16)`型であることができます。

例：

```sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

```text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### transform(x, array_from, array_to)

`default`引数がないことを除いて他のバリエーションと類似しています。一致が見つからない場合、`x`が返されます。

例：

```sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vkontakte.ru'], ['www.yandex', 'example.com', 'vk.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

```text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableDecimalSize

サイズ（バイト数）を指定すると、この関数は小数サイズを持つ読みやすいサイズを文字列として返します（KB、MBなどの単位付き）。

この関数の逆操作は[parseReadableSize](#parsereadablesize)、[parseReadableSizeOrZero](#parsereadablesizeorzero)、および[parseReadableSizeOrNull](#parsereadablesizeornull)です。

**構文**

```sql
formatReadableDecimalSize(x)
```

**例**

クエリ：

```sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableDecimalSize(filesize_bytes) AS filesize
```

結果：

```text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.02 KB   │
│        1048576 │ 1.05 MB   │
│      192851925 │ 192.85 MB │
└────────────────┴────────────┘
```

## formatReadableSize

サイズ（バイト数）を指定すると、この関数は読みやすく丸められたサイズを文字列として返します（KiB、MiBなどの単位付き）。

この関数の逆操作は[parseReadableSize](#parsereadablesize)、[parseReadableSizeOrZero](#parsereadablesizeorzero)、および[parseReadableSizeOrNull](#parsereadablesizeornull)です。

**構文**

```sql
formatReadableSize(x)
```
別名: `FORMAT_BYTES`.

**例**

クエリ：

```sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

結果：

```text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## formatReadableQuantity

数を指定すると、この関数は丸められた数を文字列として返します（千、百万、十億などの接尾辞付き）。

**構文**

```sql
formatReadableQuantity(x)
```

**例**

クエリ：

```sql
SELECT
    arrayJoin([1024, 1234 * 1000, (4567 * 1000) * 1000, 98765432101234]) AS number,
    formatReadableQuantity(number) AS number_for_humans
```

結果：

```text
┌─────────number─┬─number_for_humans─┐
│           1024 │ 1.02 thousand     │
│        1234000 │ 1.23 million      │
│     4567000000 │ 4.57 billion      │
│ 98765432101234 │ 98.77 trillion    │
└────────────────┴───────────────────┘
```

## formatReadableTimeDelta

秒単位の時間間隔（デルタ）を指定すると、この関数は年/月/日/時/分/秒/ミリ秒/マイクロ秒/ナノ秒として時間デルタを文字列として返します。

**構文**

```sql
formatReadableTimeDelta(column[, maximum_unit, minimum_unit])
```

**引数**

- `column` — 数値の時間デルタを持つカラム。
- `maximum_unit` — 任意。最大表示単位。
  - 受け入れられる値：`nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `months`, `years`.
  - デフォルト値：`years`.
- `minimum_unit` — 任意。最小表示単位。より小さい単位は切り捨てられます。
  - 受け入れられる値：`nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `months`, `years`.
  - 明示的に指定された値が`maximum_unit`より大きい場合、例外が投げられます。
  - デフォルト値：`maximum_unit`が`seconds`以上の場合は`seconds`, それ以外の場合は`nanoseconds`.

**例**

```sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed) AS time_delta
```

```text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 3 hours, 25 minutes and 45 seconds                              │
│  432546534 │ 13 years, 8 months, 17 days, 7 hours, 48 minutes and 54 seconds │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    arrayJoin([100, 12345, 432546534]) AS elapsed,
    formatReadableTimeDelta(elapsed, 'minutes') AS time_delta
```

```text
┌────elapsed─┬─time_delta ─────────────────────────────────────────────────────┐
│        100 │ 1 minute and 40 seconds                                         │
│      12345 │ 205 minutes and 45 seconds                                      │
│  432546534 │ 7209108 minutes and 54 seconds                                  │
└────────────┴─────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    arrayJoin([100, 12345, 432546534.00000006]) AS elapsed,
    formatReadableTimeDelta(elapsed, 'minutes', 'nanoseconds') AS time_delta
```

```text
┌────────────elapsed─┬─time_delta─────────────────────────────────────┐
│                100 │ 1 minute and 40 seconds                        │
│              12345 │ 205 minutes and 45 seconds                     │
│ 432546534.00000006 │ 7209108 minutes, 54 seconds and 60 nanoseconds │
└────────────────────┴────────────────────────────────────────────────┘
```

## parseReadableSize

バイトサイズと`B`、`KiB`、`KB`、`MiB`、`MB`などの単位（例：[ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) または小数バイト単位）を含む文字列を指定すると、この関数は対応するバイト数を返します。  
入力値を解析できない場合、例外がスローされます。

この関数の逆操作は[formatReadableSize](#formatreadablesize) と [formatReadableDecimalSize](#formatreadabledecimalsize)です。

**構文**

```sql
parseReadableSize(x)
```

**引数**

- `x` : ISO/IEC 80000-13または小数バイト単位での読みやすいサイズ ([String](../../sql-reference/data-types/string.md))。

**戻り値**

- バイト数、最も近い整数に切り上げられる ([UInt64](../../sql-reference/data-types/int-uint.md))。

**例**

```sql
SELECT
    arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB']) AS readable_sizes,  
    parseReadableSize(readable_sizes) AS sizes;
```

```text
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
└────────────────┴─────────┘
```

## parseReadableSizeOrNull

バイトサイズと`B`、`KiB`、`KB`、`MiB`、`MB`などの単位（例：[ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) 或いは小数バイト単位）を含む文字列を指定すると、この関数は対応するバイト数を返します。  
入力値を解析できない場合、`NULL`を返します。

この関数の逆操作は[formatReadableSize](#formatreadablesize) と[formatReadableDecimalSize](#formatreadabledecimalsize)です。

**構文**

```sql
parseReadableSizeOrNull(x)
```

**引数**

- `x` : ISO/IEC 80000-13或いは小数バイト単位での読みやすいサイズ ([String](../../sql-reference/data-types/string.md))。

**戻り値**

- バイト数，最も近い整数に切り上げられる，あるいは入力を解析できない場合はNULL（Nullable([UInt64](../../sql-reference/data-types/int-uint.md))）。

**例**

```sql
SELECT
    arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB', 'invalid']) AS readable_sizes,  
    parseReadableSizeOrNull(readable_sizes) AS sizes;
```

```text
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
│ invalid        │    ᴺᵁᴸᴸ │
└────────────────┴─────────┘
```

## parseReadableSizeOrZero

バイトサイズと`B`、`KiB`、`KB`、`MiB`、`MB`などの単位（例：[ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) または小数バイト単位）を含む文字列を指定すると、この関数は対応するバイト数を返します。入力値を解析できない場合、`0`を返します。

この関数の逆操作は[formatReadableSize](#formatreadablesize)と[formatReadableDecimalSize](#formatreadabledecimalsize)です。


**構文**

```sql
parseReadableSizeOrZero(x)
```

**引数**

- `x` : ISO/IEC 80000-13または小数バイト単位での読みやすいサイズ ([String](../../sql-reference/data-types/string.md))。

**戻り値**

- バイト数、最も近い整数に切り上げられる、または入力を解析できない場合は0 ([UInt64](../../sql-reference/data-types/int-uint.md))。

**例**

```sql
SELECT
    arrayJoin(['1 B', '1 KiB', '3 MB', '5.314 KiB', 'invalid']) AS readable_sizes,  
    parseReadableSizeOrZero(readable_sizes) AS sizes;
```

```text
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MB           │ 3000000 │
│ 5.314 KiB      │    5442 │
│ invalid        │       0 │
└────────────────┴─────────┘
```

## parseTimeDelta

数字の連続したものと、時間の単位に似たものを解析します。

**構文**

```sql
parseTimeDelta(timestr)
```

**引数**

- `timestr` — 数字の連続したものと、時間の単位に似たもの。

**戻り値**

- 浮動小数点数で表される秒数。

**例**

```sql
SELECT parseTimeDelta('11s+22min')
```

```text
┌─parseTimeDelta('11s+22min')─┐
│                        1331 │
└─────────────────────────────┘
```

```sql
SELECT parseTimeDelta('1yr2mo')
```

```text
┌─parseTimeDelta('1yr2mo')─┐
│                 36806400 │
└──────────────────────────┘
```

## least

aとbのうち、小さい値を返します。

**構文**

```sql
least(a, b)
```

## greatest

aとbのうち、大きい値を返します。

**構文**

```sql
greatest(a, b)
```

## uptime

サーバーの稼働時間を秒単位で返します。  
関数が分散テーブルのコンテキストで実行された場合、各シャードに関連する値を持つ通常のカラムを生成します。そうでなければ定数値を生成します。

**構文**

``` sql
uptime()
```

**戻り値**

- 秒数の時間値。[UInt32](../data-types/int-uint.md)。

**例**

クエリ：

``` sql
SELECT uptime() as Uptime;
```

結果：

``` response
┌─Uptime─┐
│  55867 │
└────────┘
```

## version

ClickHouseの現在のバージョンを以下の形式で文字列として返します：

- メジャーバージョン
- マイナーバージョン
- パッチバージョン
- 前の安定リリースからのコミット数

```plaintext
メジャーバージョン.マイナーバージョン.パッチバージョン.前の安定リリースからのコミット数
```

関数が分散テーブルのコンテキストで実行された場合、各シャードに関連する値を持つ通常のカラムを生成します。そうでなければ定数値を生成します。

**構文**

```sql
version()
```

**引数**

なし。

**戻り値**

- ClickHouseの現在バージョン。[String](../data-types/string)。

**実装の詳細**

なし。

**例**

クエリ：

```sql
SELECT version()
```

**結果**：

```response
┌─version()─┐
│ 24.2.1.1  │
└───────────┘
```

## buildId

現在動作しているClickHouseサーバーバイナリのコンパイラによって生成されたビルドIDを返します。  
分散テーブルのコンテキストで実行された場合、各シャードに関連する値を持つ通常のカラムを生成します。それ以外の場合は定数値を生成します。

**構文**

```sql
buildId()
```

## blockNumber

[ブロック](../../development/architecture.md#block) の行を含む単調に増加するシーケンス番号を返します。  
返されるブロック番号はベストエフォートベースで更新されます。つまり、完全に正確でない場合があります。

**構文**

```sql
blockNumber()
```

**戻り値**

- 行が存在するデータブロックのシーケンス番号。[UInt64](../data-types/int-uint.md)。

**例**

クエリ：

```sql
SELECT blockNumber()
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 10
) SETTINGS max_block_size = 2
```

結果：

```response
┌─blockNumber()─┐
│             7 │
│             7 │
└───────────────┘
┌─blockNumber()─┐
│             8 │
│             8 │
└───────────────┘
┌─blockNumber()─┐
│             9 │
│             9 │
└───────────────┘
┌─blockNumber()─┐
│            10 │
│            10 │
└───────────────┘
┌─blockNumber()─┐
│            11 │
│            11 │
└───────────────┘
```

## rowNumberInBlock {#rowNumberInBlock}

各処理された[ブロック](../../development/architecture.md#block)に対して、現在の行の番号を返します。この番号は各ブロックで0から始まります。

**構文**

```sql
rowNumberInBlock()
```

**戻り値**

- データブロックの行の序数で0から始まります。[UInt64](../data-types/int-uint.md)。

**例**

クエリ：

```sql
SELECT rowNumberInBlock()
FROM
(
    SELECT *
    FROM system.numbers_mt
    LIMIT 10
) SETTINGS max_block_size = 2
```

結果：

```response
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
```

## rowNumberInAllBlocks

`rowNumberInAllBlocks`によって処理された各行に対して一意の行番号を返します。返される番号は0から始まります。

**構文**

```sql
rowNumberInAllBlocks()
```

**戻り値**

- データブロックの行の序数で0から始まります。[UInt64](../data-types/int-uint.md)。

**例**

クエリ：

```sql
SELECT rowNumberInAllBlocks()
FROM
(
    SELECT *
    FROM system.numbers_mt
    LIMIT 10
)
SETTINGS max_block_size = 2
```

結果：

```response
┌─rowNumberInAllBlocks()─┐
│                      0 │
│                      1 │
└────────────────────────┘
┌─rowNumberInAllBlocks()─┐
│                      4 │
│                      5 │
└────────────────────────┘
┌─rowNumberInAllBlocks()─┐
│                      2 │
│                      3 │
└────────────────────────┘
┌─rowNumberInAllBlocks()─┐
│                      6 │
│                      7 │
└────────────────────────┘
┌─rowNumberInAllBlocks()─┐
│                      8 │
│                      9 │
└────────────────────────┘
```

## neighbor

指定されたオフセット前または後の行を特定のカラムから取得するウィンドウ関数です。

**構文**

```sql
neighbor(column, offset[, default_value])
```

関数の結果は影響を受けるデータブロックとブロック内のデータの順序によります。

:::note
現在処理しているデータブロックの中でのみ近傍を返します。誤りやすい動作のため、関数は非推奨です。  
適切なウィンドウ関数を使用してください。
:::

`neighbor()`の計算中の行の順序は、ユーザーに返される行の順序と異なる場合があります。  
これを防ぐためには、[ORDER BY](../../sql-reference/statements/select/order-by.md)を使用してサブクエリを作成し、サブクエリの外部から関数を呼び出すことができます。

**引数**

- `column` — カラム名またはスカラー式。
- `offset` — `column`内で現在の行から前または先の行を探す行数。[Int64](../data-types/int-uint.md)。
- `default_value` — 任意。オフセットがブロック境界を超えた場合の返される値。影響を受けたデータブロックの型。

**戻り値**

- オフセットがブロック境界を超えなければ、現在の行からオフセット距離の`column`の値。
- オフセットがブロック境界を超えるときの`column`のデフォルト値または`default_value`（指定されている場合）。

:::note
戻り値の型は影響を受けたデータブロックの型またはデフォルト値の型です。
:::

**例**

クエリ：

```sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

結果：

```text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

クエリ：

```sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

結果：

```text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

この関数は年々のメトリクス値を計算するために使用できます：

クエリ：

```sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

結果：

```text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## runningDifference {#runningDifference}

データブロック内の2つの連続する行の値の差を計算します。  
最初の行には0を返し、以降の行には1つ前の行との差を返します。

:::note
現在処理しているデータブロックの中でのみ差分を返します。  
誤りやすい動作のため、関数は非推奨です。適切なウィンドウ関数を使用してください。
:::

関数の結果は影響を受けるデータブロックとブロック内のデータの順序によります。

`runningDifference()`の計算中の行の順序は、ユーザーに返される行の順序と異なる場合があります。  
これを防ぐためには、[ORDER BY](../../sql-reference/statements/select/order-by.md)を使用してサブクエリを作成し、サブクエリの外部から関数を呼び出すことができます。

**構文**

```sql
runningDifference(x)
```

**例**

クエリ：

```sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

結果：

```text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

ブロックサイズが結果に影響を与えることに注意してください。  
`runningDifference`の内部状態は新しいブロックごとにリセットされます。

クエリ：

```sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

結果：

```text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

クエリ：

```sql
set max_block_size=100000 -- デフォルト値は65536です！

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

結果：

```text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstValue

:::note
この関数はDEPRECATEDです（`runningDifference`の注釈を参照）。
:::

最初の行の値を最初の行に返す以外は、[runningDifference](./other-functions.md#other_functions-runningdifference)と同じです。

## runningConcurrency

同時に発生するイベントの数を計算します。  
各イベントには開始時間と終了時間があります。  
開始時間はイベントに含まれていますが、終了時間は除外されています。列の開始時間と終了時間は同じデータ型でなければなりません。  
関数は、各イベントの開始時間に対してアクティブ（並行）のイベントの総数を計算します。

:::tip
イベントは開始時間で昇順に並べられている必要があります。この要件が満たされない場合、関数は例外をスローします。  
すべてのデータブロックは別々に処理されます。異なるデータブロックからのイベントが重なる場合、それらは正しく処理されません。
:::

**構文**

```sql
runningConcurrency(start, end)
```

**引数**

- `start` — イベントの開始時間を持つカラム。[Date](../data-types/date.md), [DateTime](../data-types/datetime.md), または [DateTime64](../data-types/datetime64.md)。
- `end` — イベントの終了時間を持つカラム。[Date](../data-types/date.md), [DateTime](../data-types/datetime.md), または [DateTime64](../data-types/datetime64.md)。

**戻り値**

- 各イベントの開始時間における同時に発生しているイベントの数。[UInt32](../data-types/int-uint.md)

**例**

表は：

```text
┌──────start─┬────────end─┐
│ 2021-03-03 │ 2021-03-11 │
│ 2021-03-06 │ 2021-03-12 │
│ 2021-03-07 │ 2021-03-08 │
│ 2021-03-11 │ 2021-03-12 │
└────────────┴────────────┘
```

クエリ：

```sql
SELECT start, runningConcurrency(start, end) FROM example_table;
```

結果：

```text
┌──────start─┬─runningConcurrency(start, end)─┐
│ 2021-03-03 │                              1 │
│ 2021-03-06 │                              2 │
│ 2021-03-07 │                              3 │
│ 2021-03-11 │                              2 │
└────────────┴────────────────────────────────┘
```

## MACNumToString

UInt64数値をビッグエンディアン形式のMACアドレスとして解釈します。  
対応するMACアドレスをAA:BB:CC:DD:EE:FF（コロンで区切った16進数）形式で文字列として返します。

**構文**

```sql
MACNumToString(num)
```

## MACStringToNum

MACNumToStringの逆関数です。MACアドレスが無効な形式の場合、0を返します。

**構文**

```sql
MACStringToNum(s)
```

## MACStringToOUI

MACアドレスをAA:BB:CC:DD:EE:FF（コロンで区切った16進数）形式で指定すると、最初の3オクテットをUInt64数値として返します。  
MACアドレスが無効な形式の場合、0を返します。

**構文**

```sql
MACStringToOUI(s)
```

## getSizeOfEnumType

[Enum](../data-types/enum.md)型のフィールドの数を返します。  
型が`Enum`でない場合、例外がスローされます。

**構文**

```sql
getSizeOfEnumType(value)
```

**引数:**

- `value` — `Enum`型の値。

**戻り値**

- `Enum`型入力値のフィールド数を返します。

**例**

```sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

```text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize

圧縮を考慮せずにディスク上のサイズを返します。

```sql
blockSerializedSize(value[, value[, ...]])
```

**引数**

- `value` — 任意の値。

**戻り値**

- ブロックの値を書き込むためにディスクに書き込まれるバイト数を返します。ただし、圧縮を考慮しません。

**例**

クエリ：

```sql
SELECT blockSerializedSize(maxState(1)) as x
```

結果：

```text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName

値を表しているデータ型の内部名を返します。

**構文**

```sql
toColumnTypeName(value)
```

**引数:**

- `value` — 任意の型の値。

**戻り値**

- `value`を表すために使用される内部データ型名。

**例**

`toTypeName`と`toColumnTypeName`の違い:

```sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

結果：

```text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

クエリ：

```sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

結果：

```text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

例は`DateTime`データ型が内部的に`Const(UInt32)`として格納されていることを示しています。

## dumpColumnStructure

RAM内のデータ構造の詳細な説明を出力します。

```sql
dumpColumnStructure(value)
```

**引数:**

- `value` — 任意の型の値。

**戻り値**

- `value`を表すために使用されるカラム構造の説明。

**例**

```sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

```text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType

指定されたデータ型に対するデフォルト値を返します。

ユーザーが設定したカスタムカラムのデフォルト値は含まれません。

**構文**

```sql
defaultValueOfArgumentType(expression)
```

**引数:**

- `expression` — 任意の型か任意の型の値を結果とする式。

**戻り値**

- 数値の場合は`0`。
- 文字列の場合は空文字列。
- [Nullable](../data-types/nullable.md)の場合は`ᴺᵁᴸᴸ`。

**例**

クエリ：

```sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

結果：

```text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

クエリ：

```sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

結果：

```text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## defaultValueOfTypeName

指定された型名に対するデフォルト値を返します。

ユーザーが設定したカスタムカラムのデフォルト値は含まれません。

```sql
defaultValueOfTypeName(type)
```

**引数:**

- `type` — 型名を表す文字列。

**戻り値**

- 数値の場合は`0`。
- 文字列の場合は空文字列。
- [Nullable](../data-types/nullable.md)の場合は`ᴺᵁᴸᴸ`。

**例**

クエリ：

```sql
SELECT defaultValueOfTypeName('Int8')
```

結果：

```text
┌─defaultValueOfTypeName('Int8')─┐
│                              0 │
└────────────────────────────────┘
```

クエリ：

```sql
SELECT defaultValueOfTypeName('Nullable(Int8)')
```

結果：

```text
┌─defaultValueOfTypeName('Nullable(Int8)')─┐
│                                     ᴺᵁᴸᴸ │
└──────────────────────────────────────────┘
```

## indexHint

この関数はデバッグとイントロスペクションを目的としています。引数を無視して常に1を返します。引数は評価されません。

ただし、インデックス分析中にこの関数の引数は`indexHint`でラップされていないと仮定されます。  
これにより、対応する条件でインデックス範囲のデータを選択できますが、その条件によるさらなるフィルタリングは行いません。ClickHouseのインデックスはスパースであり、`indexHint`を使用すると、同じ条件を直接指定した場合よりも多くのデータが得られます。

**構文**

```sql
SELECT * FROM table WHERE indexHint(<expression>)
```

**戻り値**

- `1`。[Uint8](../data-types/int-uint.md)。

**例**

次は、テーブル[ontime](../../getting-started/example-datasets/ontime.md)のテストデータの例です。

テーブル：

```sql
SELECT count() FROM ontime
```

```text
┌─count()─┐
│ 4276457 │
└─────────┘
```

テーブルはフィールド`(FlightDate, (Year, FlightDate))`にインデックスを持っています。

インデックスを使用しないクエリを作成します：

```sql
SELECT FlightDate AS k, count() FROM ontime GROUP BY k ORDER BY k
```

ClickHouseはテーブル全体を処理します（`約428万行処理済み`）。

結果：

```text
┌──────────k─┬─count()─┐
│ 2017-01-01 │   13970 │
│ 2017-01-02 │   15882 │
........................
│ 2017-09-28 │   16411 │
│ 2017-09-29 │   16384 │
│ 2017-09-30 │   12520 │
└────────────┴─────────┘
```

インデックスを適用するには、特定の日付を選択します：

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k
```

ClickHouse現在ではインデックスを利用し、はるかに少ない行数（`32,740行処理済み`）を処理します。

結果：

```text
┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘
```

今、式`k = '2017-09-15'`を関数`indexHint`でラップします：

クエリ：

```sql
SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE indexHint(k = '2017-09-15')
GROUP BY k
ORDER BY k ASC
```

ClickHouseは前回と同様にインデックスを使用しました（`32,740行処理済み`）。  
結果を生成する際に`k = '2017-09-15'`の式は使用されませんでした。例では、`indexHint`関数を使用して隣接する日付を確認できます。

結果：

```text
┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
└────────────┴─────────┘
```

## replicate

単一の値を持つ配列を作成します。

:::note
この関数は[配列結合](../../sql-reference/functions/array-join.md#functions_arrayjoin)の内部実装に使用されます。
:::

**構文**

```sql
replicate(x, arr)
```

**引数**

- `x` — 結果配列を埋める値。
- `arr` — 配列。[Array](../data-types/array.md)。

**戻り値**

`arr`と同じ長さで値`x`で満たされた配列。[Array](../data-types/array.md)。

**例**

クエリ：

```sql
SELECT replicate(1, ['a', 'b', 'c']);
```

結果：

```text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## revision

現在のClickHouse [サーバリビジョン](../../operations/system-tables/metrics#revision)を返します。

**構文**

```sql
revision()
```

**戻り値**

- 現在のClickHouseサーバーリビジョン。[UInt32](../data-types/int-uint.md)。

**例**

クエリ：

```sql
SELECT revision();
```

結果：

```response
┌─revision()─┐
│      54485 │
└────────────┘
```


## filesystemAvailable

データベースの永続化をホスティングしているファイルシステムの空きスペースを返します。返される値は、オペレーティングシステムのために予約されているため、常に合計の空きスペース ([filesystemFree](#filesystemfree)) よりも小さくなります。

**構文**

```sql
filesystemAvailable()
```

**返り値**

- バイト単位での残りの空きスペースの量です。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space";
```

結果:

```text
┌─Available space─┐
│ 30.75 GiB       │
└─────────────────┘
```

## filesystemUnreserved

データベースの永続化をホスティングしているファイルシステムの合計空きスペースを返します。（以前は `filesystemFree`）。[`filesystemAvailable`](#filesystemavailable) も参照してください。

**構文**

```sql
filesystemUnreserved()
```

**返り値**

- 空きスペースの量（バイト単位）です。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT formatReadableSize(filesystemUnreserved()) AS "Free space";
```

結果:

```text
┌─Free space─┐
│ 32.39 GiB  │
└────────────┘
```

## filesystemCapacity

ファイルシステムの容量をバイト単位で返します。データディレクトリの[パス](../../operations/server-configuration-parameters/settings.md#path)を設定する必要があります。

**構文**

```sql
filesystemCapacity()
```

**返り値**

- バイト単位でのファイルシステムの容量。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity";
```

結果:

```text
┌─Capacity──┐
│ 39.32 GiB │
└───────────┘
```

## initializeAggregation

単一の値に基づいて集計関数の結果を計算します。この関数は、[-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state) コンビネータを使用して集計関数を初期化するために使用できます。集計関数の状態を作成し、それらを [AggregateFunction](../data-types/aggregatefunction.md#data-type-aggregatefunction) 型のカラムに挿入するか、初期化された集計をデフォルト値として使用できます。

**構文**

```sql
initializeAggregation (aggregate_function, arg1, arg2, ..., argN)
```

**引数**

- `aggregate_function` — 初期化する集計関数の名前。[String](../data-types/string.md)。
- `arg` — 集計関数の引数。

**返り値**

- 関数に渡された各行に対する集計の結果。

返り値の型は、最初の引数として `initializeAggregation` が取る関数の返り値の型と同じです。

**例**

クエリ:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM numbers(10000));
```

結果:

```text
┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
```

クエリ:

```sql
SELECT finalizeAggregation(state), toTypeName(state) FROM (SELECT initializeAggregation('sumState', number % 3) AS state FROM numbers(5));
```

結果:

```text
┌─finalizeAggregation(state)─┬─toTypeName(state)─────────────┐
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
│                          2 │ AggregateFunction(sum, UInt8) │
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
└────────────────────────────┴───────────────────────────────┘
```

`AggregatingMergeTree` テーブルエンジンと `AggregateFunction` カラムを使用した例:

```sql
CREATE TABLE metrics
(
    key UInt64,
    value AggregateFunction(sum, UInt64) DEFAULT initializeAggregation('sumState', toUInt64(0))
)
ENGINE = AggregatingMergeTree
ORDER BY key
```

```sql
INSERT INTO metrics VALUES (0, initializeAggregation('sumState', toUInt64(42)))
```

**関連情報**

- [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)

## finalizeAggregation

集計関数の状態を与えると、この関数は集計の結果（または [-State](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-state) コンビネータを使用する際の最終化された状態）を返します。

**構文**

```sql
finalizeAggregation(state)
```

**引数**

- `state` — 集計の状態。[AggregateFunction](../data-types/aggregatefunction.md#data-type-aggregatefunction)。

**返り値**

- 集計された値/値。

:::note
返り値の型は集計された任意の型と等しいです。
:::

**例**

クエリ:

```sql
SELECT finalizeAggregation(( SELECT countState(number) FROM numbers(10)));
```

結果:

```text
┌─finalizeAggregation(_subquery16)─┐
│                               10 │
└──────────────────────────────────┘
```

クエリ:

```sql
SELECT finalizeAggregation(( SELECT sumState(number) FROM numbers(10)));
```

結果:

```text
┌─finalizeAggregation(_subquery20)─┐
│                               45 │
└──────────────────────────────────┘
```

`NULL` 値は無視されることに注意してください。

クエリ:

```sql
SELECT finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]));
```

結果:

```text
┌─finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]))─┐
│                                                          2 │
└────────────────────────────────────────────────────────────┘
```

複合的な例:

クエリ:

```sql
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(10);
```

結果:

```text
┌─number─┬─one_row_sum─┬─cumulative_sum─┐
│      0 │           0 │              0 │
│      1 │           1 │              1 │
│      2 │           2 │              3 │
│      3 │           3 │              6 │
│      4 │           4 │             10 │
│      5 │           5 │             15 │
│      6 │           6 │             21 │
│      7 │           7 │             28 │
│      8 │           8 │             36 │
│      9 │           9 │             45 │
└────────┴─────────────┴────────────────┘
```

**関連情報**

- [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)
- [initializeAggregation](#initializeaggregation)

## runningAccumulate

データブロックの各行に対する集計関数の状態を累積します。

:::note
状態は新しいデータブロックごとにリセットされます。
この誤りやすい動作のため、この関数は推奨されません。代わりに適切なウィンドウ関数を使用してください。
:::

**構文**

```sql
runningAccumulate(agg_state[, grouping]);
```

**引数**

- `agg_state` — 集計関数の状態。[AggregateFunction](../data-types/aggregatefunction.md#data-type-aggregatefunction)。
- `grouping` — グルーピングキー。オプション。`grouping` の値が変わると関数の状態がリセットされます。サポートされているデータ型の中で等価演算子が定義されているものを使用できます。

**返り値**

- 結果の各行には、入力行の0から現在の位置までのすべての行に対する集計関数の結果が含まれます。`runningAccumulate` は、各新しいデータブロックまたは `grouping` 値が変わったときに状態をリセットします。

型は使用された集計関数に依存します。

**例**

グルーピングを使用せずに数値の累積合計を求める方法と、グルーピングを使用する方法を考えてみます。

クエリ:

```sql
SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k);
```

結果:

```text
┌─k─┬─res─┐
│ 0 │   0 │
│ 1 │   1 │
│ 2 │   3 │
│ 3 │   6 │
│ 4 │  10 │
│ 5 │  15 │
│ 6 │  21 │
│ 7 │  28 │
│ 8 │  36 │
│ 9 │  45 │
└───┴─────┘
```

サブクエリは、`0` から `9` までの各数値に関して `sumState` を生成します。`sumState` は、単一の数の合計を含む [sum](../../sql-reference/aggregate-functions/reference/sum.md) 関数の状態を返します。

全体のクエリは次のことを行います:

1. 最初の行では、`runningAccumulate` は `sumState(0)` を取得し `0` を返します。
2. 2番目の行では、関数は `sumState(0)` と `sumState(1)` をマージし、`sumState(0 + 1)` を生成し、結果として `1` を返します。
3. 3番目の行では、関数は `sumState(0 + 1)` と `sumState(2)` をマージし、`sumState(0 + 1 + 2)` を生成し、結果として `3` を返します。
4. ブロックが終了するまでこれらの操作が繰り返されます。

以下の例は、`grouping` パラメータの使用法を示しています:

クエリ:

```sql
SELECT
    grouping,
    item,
    runningAccumulate(state, grouping) AS res
FROM
(
    SELECT
        toInt8(number / 4) AS grouping,
        number AS item,
        sumState(number) AS state
    FROM numbers(15)
    GROUP BY item
    ORDER BY item ASC
);
```

結果:

```text
┌─grouping─┬─item─┬─res─┐
│        0 │    0 │   0 │
│        0 │    1 │   1 │
│        0 │    2 │   3 │
│        0 │    3 │   6 │
│        1 │    4 │   4 │
│        1 │    5 │   9 │
│        1 │    6 │  15 │
│        1 │    7 │  22 │
│        2 │    8 │   8 │
│        2 │    9 │  17 │
│        2 │   10 │  27 │
│        2 │   11 │  38 │
│        3 │   12 │  12 │
│        3 │   13 │  25 │
│        3 │   14 │  39 │
└──────────┴──────┴─────┘
```

ご覧のとおり、`runningAccumulate` は各行のグループごとに状態を結合します。

## joinGet

関数は[Dictionary](../../sql-reference/dictionaries/index.md)のようにテーブルからデータを抽出します。指定された結合キーを使用して[Join](../../engines/table-engines/special/join.md#creating-a-table)テーブルからデータを取得します。

:::note
`ENGINE = Join(ANY, LEFT, <join_keys>)` ステートメントで作成されたテーブルのみをサポートします。
:::

**構文**

```sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**引数**

- `join_storage_table_name` — 検索が実行される場所を示す[識別子](../../sql-reference/syntax.md#syntax-identifiers)。
- `value_column` — 必要なデータが含まれるテーブルのカラム名。
- `join_keys` — キーのリスト。

:::note
識別子はデフォルトデータベース（コンフィグファイルの `default_database` 設定を参照）で検索されます。デフォルトデータベースを上書きするためには、`USE db_name`または`db_name.db_table`の形式でデータベースとテーブルを指定します。
:::

**返り値**

- キーのリストに対応する値のリストを返します。

:::note
あるキーがソーステーブルに存在しない場合、テーブル作成時の[join_use_nulls](../../operations/settings/settings.md#join_use_nulls)設定に基づいて`0`または`null`が返されます。
`join_use_nulls`の詳細については[Join operation](../../engines/table-engines/special/join.md)を参照してください。
:::

**例**

入力テーブル:

```sql
CREATE DATABASE db_test;
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id);
INSERT INTO db_test.id_val VALUES (1, 11)(2, 12)(4, 13);
SELECT * FROM db_test.id_val;
```

```text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

クエリ:

```sql
SELECT number, joinGet(db_test.id_val, 'val', toUInt32(number)) from numbers(4);
```

結果:

```text
   ┌─number─┬─joinGet('db_test.id_val', 'val', toUInt32(number))─┐
1. │      0 │                                                  0 │
2. │      1 │                                                 11 │
3. │      2 │                                                 12 │
4. │      3 │                                                  0 │
   └────────┴────────────────────────────────────────────────────┘
```

`join_use_nulls` を使用して、ソーステーブルにキーが存在しない場合に返される動作を変更できます。

```sql
CREATE DATABASE db_test;
CREATE TABLE db_test.id_val_nulls(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls=1;
INSERT INTO db_test.id_val_nulls VALUES (1, 11)(2, 12)(4, 13);
SELECT * FROM db_test.id_val_nulls;
```

```text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

クエリ:

```sql
SELECT number, joinGet(db_test.id_val_nulls, 'val', toUInt32(number)) from numbers(4);
```

結果:

```text
   ┌─number─┬─joinGet('db_test.id_val_nulls', 'val', toUInt32(number))─┐
1. │      0 │                                                     ᴺᵁᴸᴸ │
2. │      1 │                                                       11 │
3. │      2 │                                                       12 │
4. │      3 │                                                     ᴺᵁᴸᴸ │
   └────────┴──────────────────────────────────────────────────────────┘
```

## joinGetOrNull

[joinGet](#joinget) と同様ですが、キーが見つからない場合にはデフォルト値を返す代わりに `NULL` を返します。

**構文**

```sql
joinGetOrNull(join_storage_table_name, `value_column`, join_keys)
```

**引数**

- `join_storage_table_name` — 検索が実行される場所を示す[識別子](../../sql-reference/syntax.md#syntax-identifiers)。
- `value_column` — 必要なデータが含まれるテーブルのカラム名。
- `join_keys` — キーのリスト。

:::note
識別子はデフォルトデータベース（コンフィグファイルの `default_database` 設定を参照）で検索されます。デフォルトデータベースを上書きするためには、`USE db_name`または`db_name.db_table`の形式でデータベースとテーブルを指定します。
:::

**返り値**

- キーのリストに対応する値のリストを返します。

:::note
ソーステーブルに特定のキーが存在しない場合、そのキーに対して `NULL` が返されます。
:::

**例**

入力テーブル:

```sql
CREATE DATABASE db_test;
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id);
INSERT INTO db_test.id_val VALUES (1, 11)(2, 12)(4, 13);
SELECT * FROM db_test.id_val;
```

```text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

クエリ:

```sql
SELECT number, joinGetOrNull(db_test.id_val, 'val', toUInt32(number)) from numbers(4);
```

結果:

```text
   ┌─number─┬─joinGetOrNull('db_test.id_val', 'val', toUInt32(number))─┐
1. │      0 │                                                     ᴺᵁᴸᴸ │
2. │      1 │                                                       11 │
3. │      2 │                                                       12 │
4. │      3 │                                                     ᴺᵁᴸᴸ │
   └────────┴──────────────────────────────────────────────────────────┘
```

## catboostEvaluate

:::note
この関数は ClickHouse Cloud では利用できません。
:::

外部の catboost モデルを評価します。[CatBoost](https://catboost.ai) は、Yandex により開発された機械学習のためのオープンソースの勾配ブースティングライブラリです。
catboost モデルへのパスとモデルの引数（特徴量）を受け取り、Float64 を返します。

**構文**

```sql
catboostEvaluate(path_to_model, feature_1, feature_2, ..., feature_n)
```

**例**

```sql
SELECT feat1, ..., feat_n, catboostEvaluate('/path/to/model.bin', feat_1, ..., feat_n) AS prediction
FROM data_table
```

**前提条件**

1. catboost 評価ライブラリをビルドする

catboost モデルを評価する前に、`libcatboostmodel.<so|dylib>` ライブラリを利用可能にする必要があります。コンパイル方法については [CatBoost ドキュメンテーション](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html) を参照してください。

次に、clickhouse 設定に `libcatboostmodel.<so|dylib>` のパスを指定してください:

```xml
<clickhouse>
...
    <catboost_lib_path>/path/to/libcatboostmodel.so</catboost_lib_path>
...
</clickhouse>
```

セキュリティおよび隔離のため、モデル評価はサーバープロセスではなく clickhouse-library-bridge プロセスで実行されます。
`catboostEvaluate()` の最初の実行時に、サーバーは library bridge プロセスがすでに実行されていない場合にこれを開始します。両プロセスは HTTP インターフェースを使用して通信します。デフォルトでは、ポート `9012` が使用されます。ポート `9012` が他のサービスに割り当てられている場合に別のポートを指定することができます。

```xml
<library_bridge>
    <port>9019</port>
</library_bridge>
```

2. libcatboost を使用した catboost モデルのトレーニング

トレーニングデータセットから catboost モデルをトレーニングする方法については、[モデルのトレーニングと適用](https://catboost.ai/docs/features/training.html#training)を参照してください。

## throwIf

引数 `x` が正しければ例外をスローします。

**構文**

```sql
throwIf(x[, message[, error_code]])
```

**引数**

- `x` - チェックする条件。
- `message` - カスタムエラーメッセージを提供する定数文字列。オプション。
- `error_code` - カスタムエラーコードを提供する定数整数。オプション。

`error_code` 引数を使用するには、構成パラメータ `allow_custom_error_code_in_throwif` を有効にする必要があります。

**例**

```sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

結果:

```text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity

引数をそのまま返します。デバッグおよびテストを目的とします。インデックスを使わないことや全検索のクエリ実行性能を得ることができます。クエリがインデックスの使用可能性について分析される際に、分析者は `identity` 関数内のすべてのものを無視します。また、定数展開を無効にします。

**構文**

```sql
identity(x)
```

**例**

クエリ:

```sql
SELECT identity(42);
```

結果:

```text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## getSetting

[カスタム設定](../../operations/settings/index.md#custom_settings)の現在の値を返します。

**構文**

```sql
getSetting('custom_setting');
```

**パラメータ**

- `custom_setting` — 設定名。[String](../data-types/string.md)。

**返り値**

- 設定の現在の値。

**例**

```sql
SET custom_a = 123;
SELECT getSetting('custom_a');
```

結果:

```
123
```

**関連情報**

- [カスタム設定](../../operations/settings/index.md#custom_settings)

## getSettingOrDefault

[カスタム設定](../../operations/settings/index.md#custom_settings)の現在の値またはカスタム設定が現在のプロファイルに設定されていない場合に第2引数で指定されたデフォルト値を返します。

**構文**

```sql
getSettingOrDefault('custom_setting', default_value);
```

**パラメータ**

- `custom_setting` — 設定名。[String](../data-types/string.md)。
- `default_value` — カスタム設定が設定されていない場合に返される値。任意のデータ型または Null の値となる可能性があります。

**返り値**

- 設定の現在の値または設定されていない場合の default_value。

**例**

```sql
SELECT getSettingOrDefault('custom_undef1', 'my_value');
SELECT getSettingOrDefault('custom_undef2', 100);
SELECT getSettingOrDefault('custom_undef3', NULL);
```

結果:

```
my_value
100
NULL
```

**関連情報**

- [カスタム設定](../../operations/settings/index.md#custom_settings)

## isDecimalOverflow

[Decimal](../data-types/decimal.md) 値がその精度を超えているか、指定された精度を超えているかどうかをチェックします。

**構文**

```sql
isDecimalOverflow(d, [p])
```

**引数**

- `d` — 値。[Decimal](../data-types/decimal.md)。
- `p` — 精度。オプション。省略した場合、最初の引数の初期精度が使用されます。他のデータベースまたはファイルから/へのデータを移行する場合に有用です。[UInt8](../data-types/int-uint.md#uint-ranges)。

**返り値**

- `1` — Decimal 値がその精度によって許可される桁数を超えていること。
- `0` — Decimal 値が指定された精度を満たしていること。

**例**

クエリ:

```sql
SELECT isDecimalOverflow(toDecimal32(1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(1000000000, 0)),
       isDecimalOverflow(toDecimal32(-1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(-1000000000, 0));
```

結果:

```text
1	1	1	1
```

## countDigits

値を表現するのに必要な10進数の桁数を返します。

**構文**

```sql
countDigits(x)
```

**引数**

- `x` — [Int](../data-types/int-uint.md) または [Decimal](../data-types/decimal.md) の値。

**返り値**

- 桁数。[UInt8](../data-types/int-uint.md#uint-ranges)。

:::note
`Decimal` 値はそれらのスケールを考慮します: その基礎となる整数型 `value * scale` を計算します。例えば：`countDigits(42) = 2`, `countDigits(42.000) = 5`, `countDigits(0.04200) = 4`。つまり、`Decimal64` の小数点オーバーフローを `countDigits(x) > 18` でチェックできます。これは [isDecimalOverflow](#isdecimaloverflow) の遅いバリアントです。
:::

**例**

クエリ:

```sql
SELECT countDigits(toDecimal32(1, 9)), countDigits(toDecimal32(-1, 9)),
       countDigits(toDecimal64(1, 18)), countDigits(toDecimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
```

結果:

```text
10	10	19	19	39	39
```

## errorCodeToName

- エラーコードのテキスト名。[LowCardinality(String)](../data-types/lowcardinality.md)。

**構文**

```sql
errorCodeToName(1)
```

結果:

```text
UNSUPPORTED_METHOD
```

## tcpPort

このサーバーによってリッスンされている[native インターフェース](../../interfaces/tcp.md)の TCP ポート番号を返します。
分散テーブルのコンテキストで実行される場合、この関数は各シャードに関連する通常のカラムを生成します。それ以外の場合、定数値を生成します。

**構文**

```sql
tcpPort()
```

**引数**

- なし。

**返り値**

- TCP ポート番号。[UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT tcpPort();
```

結果:

```text
┌─tcpPort()─┐
│      9000 │
└───────────┘
```

**関連情報**

- [tcp_port](../../operations/server-configuration-parameters/settings.md#tcp_port)

## currentProfiles

現在のユーザーの[設定プロファイル](../../guides/sre/user-management/index.md#settings-profiles-management)のリストを返します。

現在の設定プロファイルを変更するには、[SET PROFILE](../../sql-reference/statements/set.md#query-set) コマンドを使用できます。`SET PROFILE` コマンドが使用されていない場合、この関数は現在のユーザー定義で指定されたプロファイルを返します（[CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement) 参照）。

**構文**

```sql
currentProfiles()
```

**返り値**

- 現在のユーザー設定プロファイルのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## enabledProfiles

明示的および暗黙的に現在のユーザーに割り当てられた設定プロファイルを返します。明示的に割り当てられたプロファイルは [currentProfiles](#currentprofiles) 関数によって返されるものと同じです。暗黙的に割り当てられたプロファイルには、他の割り当てられたプロファイル、付与されたロールを介して割り当てられたプロファイル、それ自体の設定を介して割り当てられたプロファイル、メインデフォルトプロファイル（メインサーバー設定ファイルの `default_profile` セクション参照）の親プロファイルが含まれます。

**構文**

```sql
enabledProfiles()
```

**返り値**

- 有効になっている設定プロファイルのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## defaultProfiles

現在のユーザー定義で指定されたすべてのプロファイルを返します（[CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement) ステートメント参照）。

**構文**

```sql
defaultProfiles()
```

**返り値**

- デフォルト設定プロファイルのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## currentRoles

現在のユーザーに割り当てられたロールを返します。ロールは [SET ROLE](../../sql-reference/statements/set-role.md#set-role-statement) ステートメントによって変更することができます。`SET ROLE` ステートメントが利用されていない場合、関数 `currentRoles` は `defaultRoles` と同じものを返します。

**構文**

```sql
currentRoles()
```

**返り値**

- 現在のユーザーに対する現在のロールのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## enabledRoles

現在のロールと現在のロールに付与されたロールの名前を返します。

**構文**

```sql
enabledRoles()
```

**返り値**

- 現在のユーザーに対する有効なロールのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## defaultRoles

ログイン時に現在のユーザーに対してデフォルトで有効にされるロールを返します。これらは通常、現在のユーザーに付与されたすべてのロールですが（[GRANT](../../sql-reference/statements/grant.md#select) 参照）、[SET DEFAULT ROLE](../../sql-reference/statements/set-role.md#set-default-role-statement) ステートメントで変更することができます。

**構文**

```sql
defaultRoles()
```

**返り値**

- 現在のユーザーに対するデフォルトロールのリスト。[Array](../data-types/array.md)([String](../data-types/string.md))。

## getServerPort

サーバーのポート番号を返します。ポートがサーバーによって使用されていない場合、例外をスローします。

**構文**

```sql
getServerPort(port_name)
```

**引数**

- `port_name` — サーバーポート名。[String](../data-types/string.md#string)。可能な値:

  - 'tcp_port'
  - 'tcp_port_secure'
  - 'http_port'
  - 'https_port'
  - 'interserver_http_port'
  - 'interserver_https_port'
  - 'mysql_port'
  - 'postgresql_port'
  - 'grpc_port'
  - 'prometheus.port'

**返り値**

- サーバーポートの番号。[UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT getServerPort('tcp_port');
```

結果:

```text
┌─getServerPort('tcp_port')─┐
│ 9000                      │
└───────────────────────────┘
```

## queryID

現在のクエリの ID を返します。他のクエリのパラメータは、`query_id` を使用して [system.query_log](../../operations/system-tables/query_log.md) テーブルから取得できます。

[initialQueryID](#initialqueryid) 関数とは異なり、`queryID` は異なるシャードで異なる結果を返すことができます。（例を参照）

**構文**

```sql
queryID()
```

**返り値**

- 現在のクエリの ID。[String](../data-types/string.md)

**例**

クエリ:

```sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT queryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

結果:

```text
┌─count()─┐
│ 3       │
└─────────┘
```

## initialQueryID

初期の現在のクエリの ID を返します。他のクエリのパラメータは、`initial_query_id` を使用して [system.query_log](../../operations/system-tables/query_log.md) テーブルから取得できます。

[queryID](#queryid) 関数とは異なり、`initialQueryID` は異なるシャードで同じ結果を返します。（例を参照）

**構文**

```sql
initialQueryID()
```

**返り値**

- 初期の現在のクエリの ID。[String](../data-types/string.md)

**例**

クエリ:

```sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

結果:

```text
┌─count()─┐
│ 1       │
└─────────┘
```

## partitionID

[パーティション ID](../../engines/table-engines/mergetree-family/custom-partitioning-key.md)を計算します。

:::note
この関数は遅いため、多数の行に対して呼び出すべきではありません。
:::

**構文**

```sql
partitionID(x[, y, ...]);
```

**引数**

- `x` — パーティション ID を返すカラム。
- `y, ...` — パーティション ID を返す残りの N 個のカラム（オプション）。

**返り値**

- 行が属するパーティション ID。[String](../data-types/string.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
  i int,
  j int
)
ENGINE = MergeTree
PARTITION BY i
ORDER BY tuple();

INSERT INTO tab VALUES (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);

SELECT i, j, partitionID(i), _partition_id FROM tab ORDER BY i, j;
```

結果:

```response
┌─i─┬─j─┬─partitionID(i)─┬─_partition_id─┐
│ 1 │ 1 │ 1              │ 1             │
│ 1 │ 2 │ 1              │ 1             │
│ 1 │ 3 │ 1              │ 1             │
└───┴───┴────────────────┴───────────────┘
┌─i─┬─j─┬─partitionID(i)─┬─_partition_id─┐
│ 2 │ 4 │ 2              │ 2             │
│ 2 │ 5 │ 2              │ 2             │
│ 2 │ 6 │ 2              │ 2             │
└───┴───┴────────────────┴───────────────┘
```

## shardNum

分散クエリで分散テーブルのデータの一部を処理するシャードのインデックスを返します。インデックスは `1` から始まります。
クエリが分散されていない場合、定数値 `0` が返されます。

**構文**

```sql
shardNum()
```

**返り値**

- シャードインデックスまたは定数 `0`。[UInt32](../data-types/int-uint.md)。

**例**

以下の例では、2つのシャードを使用した構成を使用します。クエリは各シャードの [system.one](../../operations/system-tables/one.md) テーブルで実行されます。

クエリ:

```sql
CREATE TABLE shard_num_example (dummy UInt8)
    ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT dummy, shardNum(), shardCount() FROM shard_num_example;
```

結果:

```text
┌─dummy─┬─shardNum()─┬─shardCount()─┐
│     0 │          2 │            2 │
│     0 │          1 │            2 │
└───────┴────────────┴──────────────┘
```

**関連情報**

- [分散テーブルエンジン](../../engines/table-engines/special/distributed.md)

## shardCount

分散クエリに対するシャードの総数を返します。
クエリが分散されていない場合、定数 `0` が返されます。

**構文**

```sql
shardCount()
```

**返り値**

- シャードの総数または `0`。[UInt32](../data-types/int-uint.md)。

**関連情報**

- [shardNum()](#shardnum) 関数の例にも `shardCount()` 関数呼び出しが含まれています。

## getOSKernelVersion

現在の OS カーネルバージョンを文字列で返します。

**構文**

```sql
getOSKernelVersion()
```

**引数**

- なし。

**返り値**

- 現在の OS カーネルバージョン。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT getOSKernelVersion();
```

結果:

```text
┌─getOSKernelVersion()────┐
│ Linux 4.15.0-55-generic │
└─────────────────────────┘
```

## zookeeperSessionUptime

現在の ZooKeeper セッションのアップタイムを秒単位で返します。

**構文**

```sql
zookeeperSessionUptime()
```

**引数**

- なし。

**返り値**

- 現在の ZooKeeper セッションのアップタイム（秒単位）。[UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT zookeeperSessionUptime();
```

結果:

```text
┌─zookeeperSessionUptime()─┐
│                      286 │
└──────────────────────────┘
```

## generateRandomStructure

テーブル構造を `column1_name column1_type, column2_name column2_type, ...` の形式でランダムに生成します。

**構文**

```sql
generateRandomStructure([number_of_columns, seed])
```

**引数**

- `number_of_columns` — 結果のテーブル構造での希望するカラム数。0 または `Null` に設定した場合、1 から 128 の範囲でランダムのカラム数になります。デフォルト値: `Null`。
- `seed` - 安定した結果を生成するためのランダムシード。シードが指定されていないか `Null` に設定されている場合、ランダムに生成されます。

すべての引数は定数でなければなりません。

**返り値**

- ランダムに生成されたテーブル構造。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT generateRandomStructure()
```

結果:

```text
┌─generateRandomStructure()─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ c1 Decimal32(5), c2 Date, c3 Tuple(LowCardinality(String), Int128, UInt64, UInt16, UInt8, IPv6), c4 Array(UInt128), c5 UInt32, c6 IPv4, c7 Decimal256(64), c8 Decimal128(3), c9 UInt256, c10 UInt64, c11 DateTime │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

```sql
SELECT generateRandomStructure(1)
```

結果:

```text
┌─generateRandomStructure(1)─┐
│ c1 Map(UInt256, UInt16)    │
└────────────────────────────┘
```

クエリ:

```sql
SELECT generateRandomStructure(NULL, 33)
```

結果:

```text
┌─generateRandomStructure(NULL, 33)─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ c1 DateTime, c2 Enum8('c2V0' = 0, 'c2V1' = 1, 'c2V2' = 2, 'c2V3' = 3), c3 LowCardinality(Nullable(FixedString(30))), c4 Int16, c5 Enum8('c5V0' = 0, 'c5V1' = 1, 'c5V2' = 2, 'c5V3' = 3), c6 Nullable(UInt8), c7 String, c8 Nested(e1 IPv4, e2 UInt8, e3 UInt16, e4 UInt16, e5 Int32, e6 Map(Date, Decimal256(70))) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**注**: 複雑な型（Array、Tuple、Map、Nested）の最大ネスト深度は 16 に制限されています。

この関数は、[generateRandom](../../sql-reference/table-functions/generate.md) と一緒に使用して完全にランダムなテーブルを生成することができます。

## structureToCapnProtoSchema {#structure_to_capn_proto_schema}

ClickHouse のテーブル構造を CapnProto スキーマに変換します。

**構文**

```sql
structureToCapnProtoSchema(structure)
```

**引数**

- `structure` — `column1_name column1_type, column2_name column2_type, ...` の形式でのテーブル構造。
- `root_struct_name` — CapnProto スキーマのルート構造の名前。デフォルト値 - `Message`;

**返り値**

- CapnProto スキーマ。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT structureToCapnProtoSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB
```

結果:

```text
@0xf96402dd754d0eb7;

struct Message
{
    column1 @0 : Data;
    column2 @1 : UInt32;
    column3 @2 : List(Data);
}
```

クエリ:

```sql
SELECT structureToCapnProtoSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB
```

結果:

```text
@0xd1c8320fecad2b7f;

struct Message
{
    struct Column1
    {
        union
        {
            value @0 : Data;
            null @1 : Void;
        }
    }
    column1 @0 : Column1;
    struct Column2
    {
        element1 @0 : UInt32;
        element2 @1 : List(Data);
    }
    column2 @1 : Column2;
    struct Column3
    {
        struct Entry
        {
            key @0 : Data;
            value @1 : Data;
        }
        entries @0 : List(Entry);
    }
    column3 @2 : Column3;
}
```

クエリ:

```sql
SELECT structureToCapnProtoSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB
```

結果:

```text
@0x96ab2d4ab133c6e1;

struct Root
{
    column1 @0 : Data;
    column2 @1 : UInt32;
}
```

## structureToProtobufSchema {#structure_to_protobuf_schema}

ClickHouse のテーブル構造を Protobuf スキーマに変換します。

**構文**

```sql
structureToProtobufSchema(structure)
```

**引数**

- `structure` — `column1_name column1_type, column2_name column2_type, ...` の形式でのテーブル構造。
- `root_message_name` — Protobuf スキーマのルートメッセージの名前。デフォルト値 - `Message`;

**返り値**

- Protobuf スキーマ。[String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT structureToProtobufSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB
```

結果:

```text
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    uint32 column2 = 2;
    repeated bytes column3 = 3;
}
```

クエリ:

```sql
SELECT structureToProtobufSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB
```

結果:

```text
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    message Column2
    {
        uint32 element1 = 1;
        repeated bytes element2 = 2;
    }
    Column2 column2 = 2;
    map<string, bytes> column3 = 3;
}
```

クエリ:

```sql
SELECT structureToProtobufSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB
```

結果:

```text
syntax = "proto3";

message Root
{
    bytes column1 = 1;
    uint32 column2 = 2;
}
```

## formatQuery

与えられた SQL クエリのフォーマットされた（複数行を含む可能性のある）バージョンを返します。

クエリが構文的に正しくない場合、例外をスローします。代わりに `NULL` を返すには、`formatQueryOrNull()` 関数を使用してください。

**構文**

```sql
formatQuery(query)
formatQueryOrNull(query)
```

**引数**

- `query` - フォーマット対象の SQL クエリ。[String](../data-types/string.md)

**返り値**

- フォーマットされたクエリ。[String](../data-types/string.md)。

**例**

```sql
SELECT formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3');
```

結果:

```result
┌─formatQuery('select a,    b FRom tab WHERE a > 3 and  b < 3')─┐
│ SELECT
    a,
    b
FROM tab
WHERE (a > 3) AND (b < 3)            │
└───────────────────────────────────────────────────────────────┘
```

## formatQuerySingleLine

`formatQuery()` と同様ですが、返されるフォーマットされた文字列には改行が含まれません。

クエリが構文的に正しくない場合、例外をスローします。代わりに `NULL` を返すには、`formatQuerySingleLineOrNull()` 関数を利用してください。

**構文**

```sql
formatQuerySingleLine(query)
formatQuerySingleLineOrNull(query)
```

**引数**

- `query` - フォーマット対象の SQL クエリ。[String](../data-types/string.md)

**返り値**

- フォーマットされたクエリ。[String](../data-types/string.md)。

**例**

```sql
SELECT formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3');
```

結果:

```result
┌─formatQuerySingleLine('select a,    b FRom tab WHERE a > 3 and  b < 3')─┐
│ SELECT a, b FROM tab WHERE (a > 3) AND (b < 3)                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## variantElement

`Variant` カラムから指定された型のカラムを抽出します。

**構文**

```sql
variantElement(variant, type_name, [, default_value])
```

**引数**

- `variant` — Variant カラム。[Variant](../data-types/variant.md)。
- `type_name` — 抽出する Variant 型の名前。[String](../data-types/string.md)。
- `default_value` - 指定された型の Variant が存在しない場合に使用されるデフォルト値。任意の型を持つことができます。オプション。

**返り値**

- 指定された型を持つ `Variant` カラムのサブカラム。

**例**

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM test;
```

```text
┌─v─────────────┬─variantElement(v, 'String')─┬─variantElement(v, 'UInt64')─┬─variantElement(v, 'Array(UInt64)')─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ []                                 │
│ 42            │ ᴺᵁᴸᴸ                        │                          42 │ []                                 │
│ Hello, World! │ Hello, World!               │                        ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ [1,2,3]                            │
└───────────────┴─────────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

## variantType

`Variant` カラムの各行に対してその Variant 型名を返します。行が NULL を含む場合、その行に対して `'None'` を返します。

**構文**

```sql
variantType(variant)
```

**引数**

- `variant` — Variant カラム。[Variant](../data-types/variant.md)。

**返り値**

- 各行の Variant 型名を持つ Enum8 カラム。

**例**

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) FROM test;
```

```text
┌─variantType(v)─┐
│ None           │
│ UInt64         │
│ String         │
│ Array(UInt64)  │
└────────────────┘
```

```sql
SELECT toTypeName(variantType(v)) FROM test LIMIT 1;
```

```text
┌─toTypeName(variantType(v))──────────────────────────────────────────┐
│ Enum8('None' = -1, 'Array(UInt64)' = 0, 'String' = 1, 'UInt64' = 2) │
└─────────────────────────────────────────────────────────────────────┘
```

## minSampleSizeConversion

2つのサンプルにおける変換率（割合）を比較する A/B テストのための最小必要サンプルサイズを計算します。

**構文**

```sql
minSampleSizeConversion(baseline, mde, power, alpha)
```

[こちらの記事](https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a)で説明されている式を使用します。治療群と対照群が同じサイズであることを想定します。返される値は一つのグループに必要なサンプルサイズ（つまり、実験全体に必要なサンプルサイズは返される値の2倍）です。

**引数**

- `baseline` — 基準変換率。[Float](../data-types/float.md)。
- `mde` — 検出可能最低効果（MDE）はパーセンテージポイントで（例：基準変換率 0.25 の MDE が 0.03 の場合、0.25 ± 0.03 への変化が予想されます）。[Float](../data-types/float.md)。
- `power` — テストの必要な統計力（1-タイプIIエラーの確率）。[Float](../data-types/float.md)。
- `alpha` — テストの必要な有意水準（タイプIエラーの確率）。[Float](../data-types/float.md)。

**返り値**

3つの要素を持つ名前つき [Tuple](../data-types/tuple.md)：

- `"minimum_sample_size"` — 必要なサンプルサイズ。[Float64](../data-types/float.md)。
- `"detect_range_lower"` — 指定されたサンプルサイズでは検出できない範囲の下限（つまり、`alpha` と `power` が提供された場合、`"detect_range_lower"` 以下のすべての値は検出可能です）。`baseline - mde` として計算されます。[Float64](../data-types/float.md)。
- `"detect_range_upper"` — 指定されたサンプルサイズでは検出できない範囲の上限（つまり、`alpha` と `power` が提供された場合、`"detect_range_upper"` 以上のすべての値は検出可能です）。`baseline + mde` として計算されます。[Float64](../data-types/float.md)。

**例**

以下のクエリは、基準変換率が 25%、MDE が 3%、有意水準が 5%、望ましい統計力が 80% の A/B テストに対する必要なサンプルサイズを計算します：

```sql
SELECT minSampleSizeConversion(0.25, 0.03, 0.80, 0.05) AS sample_size;
```

結果:

```text
┌─sample_size───────────────────┐
│ (3396.077603219163,0.22,0.28) │
└───────────────────────────────┘
```

## minSampleSizeContinuous

2つのサンプルにおける連続したメトリックの平均を比較する A/B テストのための最小必要サンプルサイズを計算します。

**構文**

```sql
minSampleSizeContinous(baseline, sigma, mde, power, alpha)
```

エイリアス: `minSampleSizeContinous`

[こちらの記事](https://towardsdatascience.com/required-sample-size-for-a-b-testing-6f6608dd330a)で説明されている式を使用します。治療群と対照群が同じサイズであることを想定します。返される値は一つのグループに必要なサンプルサイズ（つまり、実験全体に必要なサンプルサイズは返される値の2倍）です。また、テストメトリックの治療群と対照群の標準偏差が同じであることを前提としています。

**引数**

- `baseline` — メトリックの基準値。[Integer](../data-types/int-uint.md) または [Float](../data-types/float.md)。
- `sigma` — メトリックの基準標準偏差。[Integer](../data-types/int-uint.md) または [Float](../data-types/float.md)。
- `mde` — 基準値のパーセンテージとしての検出可能最低効果 (MDE) (例:基準値 112.25 の MDE は 0.03 これは、112.25 ± 112.25*0.03 への変化を意味します)。[Integer](../data-types/int-uint.md) または [Float](../data-types/float.md)。
- `power` — テストの必要な統計力（1-タイプIIエラーの確率）。[Integer](../data-types/int-uint.md) または [Float](../data-types/float.md)。
- `alpha` — テストの必要な有意水準（タイプIエラーの確率）。[Integer](../data-types/int-uint.md) または [Float](../data-types/float.md)。

**返り値**

3つの要素を持つ名前つき [Tuple](../data-types/tuple.md)：

- `"minimum_sample_size"` — 必要なサンプルサイズ。[Float64](../data-types/float.md)。
- `"detect_range_lower"` — 指定されたサンプルサイズでは検出できない範囲の下限（つまり、`alpha` と `power` が提供された場合、`"detect_range_lower"` 以下のすべての値は検出可能です）。`baseline * (1 - mde)` として計算されます。[Float64](../data-types/float.md)。
- `"detect_range_upper"` — 指定されたサンプルサイズでは検出できない範囲の上限（つまり、`alpha` と `power` が提供された場合、`"detect_range_upper"` 以上のすべての値は検出可能です）。`baseline * (1 + mde)` として計算されます。[Float64](../data-types/float.md)。

**例**

以下のクエリは、基準値 112.25、標準偏差 21.1、MDE 3%、有意水準 5%、望ましい統計力 80% のメトリックに対する A/B テストで必要なサンプルサイズを計算します：

```sql
SELECT minSampleSizeContinous(112.25, 21.1, 0.03, 0.80, 0.05) AS sample_size;
```

結果:

```text
┌─sample_size───────────────────────────┐
│ (616.2931945826209,108.8825,115.6175) │
└───────────────────────────────────────┘
```

## connectionId

現在のクエリを送信したクライアントの接続 ID を UInt64 整数として取得し、返します。

**構文**

```sql
connectionId()
```

エイリアス: `connection_id`.

**パラメータ**

なし。

**返り値**

現在の接続 ID。[UInt64](../data-types/int-uint.md)。

**実装の詳細**

この関数は、デバッグシナリオや MySQL ハンドラ内での内部目的で役立ちます。[MySQL の `CONNECTION_ID` 関数](https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_connection-id) と互換性があり、通常、プロダクションクエリでは使用されません。

**例**

クエリ:

```sql
SELECT connectionId();
```

```response
0
```

## getClientHTTPHeader

HTTP ヘッダーの値を取得します。

そのようなヘッダーが存在しないか、現在のリクエストが HTTP インターフェース経由で行われていない場合、この関数は空の文字列を返します。
特定の HTTP ヘッダー（例：`Authentication` および `X-ClickHouse-*`）は制限されています。

この関数を使用するには、設定 `allow_get_client_http_header` を有効にする必要があります。
セキュリティ上の理由から、この設定はデフォルトでは有効になっていません。一部のヘッダー、例えば `Cookie` は機密情報を含む可能性があるためです。

この関数における HTTP ヘッダーはケースセンシティブです。

関数が分散クエリのコンテキストで使用される場合、それはイニシエータノードでのみ非空結果を返します。

## showCertificate

設定されている現在のサーバーの SSL 証明書に関する情報を表示します。OpenSSL 証明書を使用して接続を検証するよう ClickHouse を設定する方法については、[SSL-TLS の設定](https://clickhouse.com/docs/ja/guides/sre/configuring-ssl)を参照してください。

**構文**

```sql
showCertificate()
```

**返り値**

- 設定された SSL 証明書に関連するキーと値のペアのマップ。[Map](../data-types/map.md)([String](../data-types/string.md), [String](../data-types/string.md))。

**例**

クエリ:

```sql
SELECT showCertificate() FORMAT LineAsString;
```

結果:

```response
{'version':'1','serial_number':'2D9071D64530052D48308473922C7ADAFA85D6C5','signature_algo':'sha256WithRSAEncryption','issuer':'/CN=marsnet.local CA','not_before':'May  7 17:01:21 2024 GMT','not_after':'May  7 17:01:21 2025 GMT','subject':'/CN=chnode1','pkey_algo':'rsaEncryption'}
```

## lowCardinalityIndices

[LowCardinality](../data-types/lowcardinality.md) カラムのDictionary内での値の位置を返します。位置は 1 から始まります。LowCardinality はパートごとにDictionaryを持っているため、この関数は異なるパートで同じ値に対して異なる位置を返すことがあります。

**構文**

```sql
lowCardinalityIndices(col)
```

**引数**

- `col` — 低カーディナリティカラム。[LowCardinality](../data-types/lowcardinality.md)。

**返り値**

- 現在のパートのDictionary内での値の位置。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test;
CREATE TABLE test (s LowCardinality(String)) ENGINE = Memory;

-- 二つのパートを作成します:

INSERT INTO test VALUES ('ab'), ('cd'), ('ab'), ('ab'), ('df');
INSERT INTO test VALUES ('ef'), ('cd'), ('ab'), ('cd'), ('ef');

SELECT s, lowCardinalityIndices(s) FROM test;
```

結果:

```response
   ┌─s──┬─lowCardinalityIndices(s)─┐
1. │ ab │                        1 │
2. │ cd │                        2 │
3. │ ab │                        1 │
4. │ ab │                        1 │
5. │ df │                        3 │
   └────┴──────────────────────────┘
    ┌─s──┬─lowCardinalityIndices(s)─┐
 6. │ ef │                        1 │
 7. │ cd │                        2 │
 8. │ ab │                        3 │
 9. │ cd │                        2 │
10. │ ef │                        1 │
    └────┴──────────────────────────┘
```
## lowCardinalityKeys

[LowCardinality](../data-types/lowcardinality.md) カラムのDictionaryの値を返します。ブロックがDictionaryのサイズより小さいまたは大きい場合、結果はデフォルト値で切り詰めまたは拡張されます。LowCardinalityはパートごとに異なるDictionaryを持っているため、この関数は異なるパートで異なるDictionaryの値を返すことがあります。

**構文**

```sql
lowCardinalityIndices(col)
```

**引数**

- `col` — 低カーディナリティカラム。[LowCardinality](../data-types/lowcardinality.md)。

**返り値**

- Dictionaryのキー。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test;
CREATE TABLE test (s LowCardinality(String)) ENGINE = Memory;

-- 二つのパートを作成します:

INSERT INTO test VALUES ('ab'), ('cd'), ('ab'), ('ab'), ('df');
INSERT INTO test VALUES ('ef'), ('cd'), ('ab'), ('cd'), ('ef');

SELECT s, lowCardinalityKeys(s) FROM test;
```

結果:

```response
   ┌─s──┬─lowCardinalityKeys(s)─┐
1. │ ef │                       │
2. │ cd │ ef                    │
3. │ ab │ cd                    │
4. │ cd │ ab                    │
5. │ ef │                       │
   └────┴───────────────────────┘
```plaintext
    ┌─s──┬─lowCardinalityKeys(s)─┐
 6. │ ab │                       │
 7. │ cd │ ab                    │
 8. │ ab │ cd                    │
 9. │ ab │ df                    │
10. │ df │                       │
    └────┴───────────────────────┘
```

## displayName

[config](../../operations/configuration-files.md/#configuration-files) の `display_name` の値、または設定されていない場合はサーバーの完全修飾ドメイン名 (FQDN) を返します。

**構文**

```sql
displayName()
```

**返される値**

- 設定されている場合は config の `display_name` の値、または設定されていない場合はサーバーの FQDN。[String](../data-types/string.md)。

**例**

`display_name` は `config.xml` で設定できます。例えば、`display_name` が 'production' に設定されているサーバーの場合：

```xml
<!-- clickhouse-client で表示される名前です。
     デフォルトでは "production" を含むものはクエリプロンプトで赤でハイライトされます。
-->
<display_name>production</display_name>
```

クエリ:

```sql
SELECT displayName();
```

結果:

```response
┌─displayName()─┐
│ production    │
└───────────────┘
```

## transactionID

[トランザクション](https://clickhouse.com/docs/ja/guides/developer/transactional#transactions-commit-and-rollback) のIDを返します。

:::note
この関数はエクスペリメンタルな機能セットの一部です。設定にエクスペリメンタルなトランザクションサポートを有効にするための設定を追加します：

```
<clickhouse>
  <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

詳細については [トランザクション (ACID) サポート](https://clickhouse.com/docs/ja/guides/developer/transactional#transactions-commit-and-rollback) のページを参照してください。
:::

**構文**

```sql
transactionID()
```

**返される値**

- `start_csn`、`local_tid` および `host_id` から成るタプルを返します。[Tuple](../data-types/tuple.md)。

- `start_csn`: このトランザクションが開始されたときに見られた最新のコミットタイムスタンプであるグローバルシーケンシャル番号。[UInt64](../data-types/int-uint.md)。
- `local_tid`: このホストが特定の `start_csn` 内で開始した各トランザクションにユニークであるローカルシーケンシャル番号。[UInt64](../data-types/int-uint.md)。
- `host_id`: このトランザクションを開始したホストの UUID。[UUID](../data-types/uuid.md)。

**例**

クエリ:

```sql
BEGIN TRANSACTION;
SELECT transactionID();
ROLLBACK;
```

結果:

```response
┌─transactionID()────────────────────────────────┐
│ (32,34,'0ee8b069-f2bb-4748-9eae-069c85b5252b') │
└────────────────────────────────────────────────┘
```

## transactionLatestSnapshot

読み取り可能なトランザクションの最新のスナップショット（コマンドシーケンシャル番号）を返します。

:::note
この関数はエクスペリメンタルな機能セットの一部です。設定にエクスペリメンタルなトランザクションサポートを有効にするための設定を追加します：

```
<clickhouse>
  <allow_experimental_transactions>1</allow_experimental_transactions>
</allow_experimental_transactions>
```

詳細については [トランザクション (ACID) サポート](https://clickhouse.com/docs/ja/guides/developer/transactional#transactions-commit-and-rollback) のページを参照してください。
:::

**構文**

```sql
transactionLatestSnapshot()
```

**返される値**

- トランザクションの最新スナップショット (CSN) を返します。[UInt64](../data-types/int-uint.md)

**例**

クエリ:

```sql
BEGIN TRANSACTION;
SELECT transactionLatestSnapshot();
ROLLBACK;
```

結果:

```response
┌─transactionLatestSnapshot()─┐
│                          32 │
└─────────────────────────────┘
```

## transactionOldestSnapshot

ある実行中の [トランザクション](https://clickhouse.com/docs/ja/guides/developer/transactional#transactions-commit-and-rollback) に対して表示可能な最古のスナップショット（コマンドシーケンス番号）を返します。

:::note
この関数はエクスペリメンタルな機能セットの一部です。設定にエクスペリメンタルなトランザクションサポートを有効にするための設定を追加します：

```
<clickhouse>
  <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

詳細については [トランザクション (ACID) サポート](https://clickhouse.com/docs/ja/guides/developer/transactional#transactions-commit-and-rollback) のページを参照してください。
:::

**構文**

```sql
transactionOldestSnapshot()
```

**返される値**

- トランザクションの最古のスナップショット (CSN) を返します。[UInt64](../data-types/int-uint.md)

**例**

クエリ:

```sql
BEGIN TRANSACTION;
SELECT transactionOldestSnapshot();
ROLLBACK;
```

結果:

```response
┌─transactionOldestSnapshot()─┐
│                          32 │
└─────────────────────────────┘
```

## getSubcolumn

テーブルの式または識別子とサブカラムの名前を持つ定数文字列を取り、式から抽出されたリクエストされたサブカラムを返します。

**構文**

```sql
getSubcolumn(col_name, subcol_name)
```

**引数**

- `col_name` — テーブルの式または識別子。[Expression](../syntax.md/#expressions)、[Identifier](../syntax.md/#identifiers)。
- `subcol_name` — サブカラムの名前。[String](../data-types/string.md)。

**返される値**

- 抽出されたサブカラムを返します。

**例**

クエリ:

```sql
CREATE TABLE t_arr (arr Array(Tuple(subcolumn1 UInt32, subcolumn2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT getSubcolumn(arr, 'subcolumn1'), getSubcolumn(arr, 'subcolumn2') FROM t_arr;
```

結果:

```response
   ┌─getSubcolumn(arr, 'subcolumn1')─┬─getSubcolumn(arr, 'subcolumn2')─┐
1. │ [1,2]                           │ ['Hello','World']               │
2. │ [3,4,5]                         │ ['This','is','subcolumn']       │
   └─────────────────────────────────┴─────────────────────────────────┘
```

## getTypeSerializationStreams

データ型のストリームパスを列挙します。

:::note
この関数は開発者向けに意図されています。
:::

**構文**

```sql
getTypeSerializationStreams(col)
```

**引数**

- `col` — データタイプが検出されるカラムまたはデータタイプの文字列表現。

**返される値**

- シリアル化されたサブストリームパスのすべてを含む配列を返します。[Array](../data-types/array.md)([String](../data-types/string.md))。

**例**

クエリ:

```sql
SELECT getTypeSerializationStreams(tuple('a', 1, 'b', 2));
```

結果:

```response
   ┌─getTypeSerializationStreams(('a', 1, 'b', 2))─────────────────────────────────────────────────────────────────────────┐
1. │ ['{TupleElement(1), Regular}','{TupleElement(2), Regular}','{TupleElement(3), Regular}','{TupleElement(4), Regular}'] │
   └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

```sql
SELECT getTypeSerializationStreams('Map(String, Int64)');
```

結果:

```response
   ┌─getTypeSerializationStreams('Map(String, Int64)')────────────────────────────────────────────────────────────────┐
1. │ ['{ArraySizes}','{ArrayElements, TupleElement(keys), Regular}','{ArrayElements, TupleElement(values), Regular}'] │
   └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## globalVariable

定数文字列引数を取り、その名前のグローバル変数の値を返します。この関数はMySQLとの互換性のために意図されており、ClickHouseの通常の操作には必要ではありません。定義されているのは少数のダミーのグローバル変数のみです。

**構文**

```sql
globalVariable(name)
```

**引数**

- `name` — グローバル変数名。[String](../data-types/string.md)。

**返される値**

- 変数 `name` の値を返します。

**例**

クエリ:

```sql
SELECT globalVariable('max_allowed_packet');
```

結果:

```response
┌─globalVariable('max_allowed_packet')─┐
│                             67108864 │
└──────────────────────────────────────┘
```
