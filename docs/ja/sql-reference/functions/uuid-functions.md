---
slug: /ja/sql-reference/functions/uuid-functions
sidebar_position: 205
sidebar_label: UUIDs
---

# UUID を扱う関数

## generateUUIDv4

[バージョン4](https://tools.ietf.org/html/rfc4122#section-4.4)の[UUID](../data-types/uuid.md)を生成します。

**構文**

``` sql
generateUUIDv4([expr])
```

**引数**

- `expr` — クエリ内で関数が複数回呼ばれる場合に[共通部分式の除去](../functions/index.md#common-subexpression-elimination)を回避するための任意の[式](../syntax.md#syntax-expressions)。式の値は返されるUUIDに影響を与えません。省略可能。

**返される値**

UUIDv4 型の値。

**例**

まず、UUID型のカラムを持つテーブルを作成し、そのテーブルに生成されたUUIDv4を挿入します。

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv4();

SELECT * FROM tab;
```

結果:

```response
┌─────────────────────────────────uuid─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

**行ごとに複数のUUIDを生成する例**

```sql
SELECT generateUUIDv4(1), generateUUIDv4(2);

┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 8abf8c13-7dea-4fdf-af3e-0e18767770e6 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7 {#generateUUIDv7}

[バージョン7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04)の[UUID](../data-types/uuid.md)を生成します。

生成されたUUIDは、ミリ秒単位の現在のUnixタイムスタンプ（48ビット）、バージョン「7」（4ビット）、ミリ秒内でUUIDを区別するためのカウンタ（42ビット）、およびランダムフィールド（32ビット）を含みます。
特定のタイムスタンプ（unix_ts_ms）の場合、カウンタはランダムな値から始まり、新しいUUIDごとに1ずつ増加します。
カウンタがオーバーフローした場合、タイムスタンプフィールドは1増加し、カウンタはランダムな新しい開始値にリセットされます。

関数`generateUUIDv7`は、並行して実行されているスレッドおよびクエリ間で、タイムスタンプ内のカウンタフィールドが単調に増加することを保証します。

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
```

:::note
2024年4月時点で、バージョン7のUUIDはドラフトステータスであり、将来そのレイアウトが変更される可能性があります。
:::

**構文**

``` sql
generateUUIDv7([expr])
```

**引数**

- `expr` — クエリ内で関数が複数回呼ばれる場合に[共通部分式の除去](../functions/index.md#common-subexpression-elimination)を回避するための任意の[式](../syntax.md#syntax-expressions)。式の値は返されるUUIDに影響を与えません。省略可能。

**返される値**

UUIDv7 型の値。

**例**

まず、UUID型のカラムを持つテーブルを作成し、そのテーブルに生成されたUUIDv7を挿入します。

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7();

SELECT * FROM tab;
```

結果:

```response
┌─────────────────────────────────uuid─┐
│ 018f05af-f4a8-778f-beee-1bedbc95c93b │
└──────────────────────────────────────┘
```

**行ごとに複数のUUIDを生成する例**

```sql
SELECT generateUUIDv7(1), generateUUIDv7(2);

┌─generateUUIDv7(1)────────────────────┬─generateUUIDv7(2)────────────────────┐
│ 018f05c9-4ab8-7b86-b64e-c9f03fbd45d1 │ 018f05c9-4ab8-7b86-b64e-c9f12efb7e16 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## empty

入力UUIDが空であるかどうかを確認します。

**構文**

```sql
empty(UUID)
```

UUIDはすべてのビットがゼロ（ゼロUUID）であるときに空と見なされます。

この関数は[Arrays](array-functions.md#function-empty)や[Strings](string-functions.md#empty)に対しても機能します。

**引数**

- `x` — UUID。 [UUID](../data-types/uuid.md)。

**返される値**

- 空のUUIDの場合は`1`、空でないUUIDの場合は`0`を返します。 [UInt8](../data-types/int-uint.md)。

**例**

UUID値を生成するために、ClickHouseは[generateUUIDv4](#generateuuidv4)関数を提供します。

クエリ:

```sql
SELECT empty(generateUUIDv4());
```

結果:

```response
┌─empty(generateUUIDv4())─┐
│                       0 │
└─────────────────────────┘
```

## notEmpty

入力UUIDが空でないかどうかを確認します。

**構文**

```sql
notEmpty(UUID)
```

UUIDはすべてのビットがゼロ（ゼロUUID）であるときに空と見なされます。

この関数は[Arrays](array-functions.md#function-notempty)や[Strings](string-functions.md#notempty)に対しても機能します。

**引数**

- `x` — UUID。 [UUID](../data-types/uuid.md)。

**返される値**

- 空でないUUIDの場合は`1`、空のUUIDの場合は`0`を返します。 [UInt8](../data-types/int-uint.md)。

**例**

UUID値を生成するために、ClickHouseは[generateUUIDv4](#generateuuidv4)関数を提供します。

クエリ:

```sql
SELECT notEmpty(generateUUIDv4());
```

結果:

```response
┌─notEmpty(generateUUIDv4())─┐
│                          1 │
└────────────────────────────┘
```

## toUUID

文字列型の値をUUIDに変換します。

``` sql
toUUID(string)
```

**返される値**

UUID型の値。

**使用例**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

結果:

```response
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## toUUIDOrDefault

**引数**

- `string` — 36文字の文字列またはFixedString(36)。 [String](../syntax.md#string)。
- `default` — 最初の引数がUUID型に変換できない場合に使用されるデフォルトのUUID。 [UUID](../data-types/uuid.md)。

**返される値**

UUID

``` sql
toUUIDOrDefault(string, default)
```

**返される値**

UUID型の値。

**使用例**

最初の例は、変換可能な場合、最初の引数をUUID型に変換して返します。

``` sql
SELECT toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
```

結果:

```response
┌─toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', CAST('59f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID'))─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                          │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

2つ目の例では、最初の引数がUUID型に変換できない場合、2つ目の引数（提供されたデフォルトのUUID）を返します。

```sql
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
```

結果:

```response
┌─toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', CAST('59f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID'))─┐
│ 59f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                               │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## toUUIDOrNull

文字列型の引数を受け取り、UUIDへの変換を試みます。失敗した場合、NULLを返します。

``` sql
toUUIDOrNull(string)
```

**返される値**

Nullable(UUID)型の値。

**使用例**

``` sql
SELECT toUUIDOrNull('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

結果:

```response
┌─uuid─┐
│ ᴺᵁᴸᴸ │
└──────┘
```

## toUUIDOrZero

文字列型の引数を受け取り、UUIDへの変換を試みます。失敗した場合、ゼロUUIDを返します。

``` sql
toUUIDOrZero(string)
```

**返される値**

UUID型の値。

**使用例**

``` sql
SELECT toUUIDOrZero('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

結果:

```response
┌─────────────────────────────────uuid─┐
│ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

36文字の形式`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`を含む`string`を受け取り、そのバイナリ表現として[FixedString(16)](../data-types/fixedstring.md)を返します。形式はオプションで`variant` (`ビッグエンディアン`がデフォルト)で指定可能です。

**構文**

``` sql
UUIDStringToNum(string[, variant = 1])
```

**引数**

- `string` — 36文字の[String](../syntax.md#syntax-string-literal)または[FixedString](../syntax.md#syntax-string-literal)。
- `variant` — [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1)に基づくバリアントを表す整数。1 = `ビッグエンディアン` (デフォルト), 2 = `マイクロソフト`。

**返される値**

FixedString(16)

**使用例**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

結果:

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid, 2) AS bytes
```

結果:

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

UUIDのバイナリ表現を含む`binary`を受け取り、オプションでその形式を`variant` (`ビッグエンディアン`がデフォルト)で指定し、テキスト形式の36文字を含む文字列を返します。

**構文**

``` sql
UUIDNumToString(binary[, variant = 1])
```

**引数**

- `binary` — UUIDのバイナリ表現としての[FixedString(16)](../data-types/fixedstring.md)。
- `variant` — [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1)に基づくバリアントを表す整数。1 = `ビッグエンディアン` (デフォルト), 2 = `マイクロソフト`。

**返される値**

文字列。

**使用例**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

結果:

```response
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

``` sql
SELECT
    '@</a;]~!p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16), 2) AS uuid
```

結果:

```response
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ @</a;]~!p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## UUIDToNum

[UUID](../data-types/uuid.md)を受け取り、`variant` (`ビッグエンディアン`がデフォルトで指定可能)を指定して、そのバイナリ表現を[FixedString(16)](../data-types/fixedstring.md)として返します。この関数は`UUIDStringToNum(toString(uuid))`の呼び出しを置き換え、UUIDからバイトを抽出する際にUUIDを文字列に中間変換する必要がなくなります。

**構文**

``` sql
UUIDToNum(uuid[, variant = 1])
```

**引数**

- `uuid` — [UUID](../data-types/uuid.md)。
- `variant` — [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1)に基づくバリアントを表す整数。1 = `ビッグエンディアン` (デフォルト), 2 = `マイクロソフト`。

**返される値**

UUIDのバイナリ表現。

**使用例**

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid) AS bytes
```

結果:

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid, 2) AS bytes
```

結果:

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDv7ToDateTime

バージョン7のUUIDのタイムスタンプコンポーネントを返します。

**構文**

``` sql
UUIDv7ToDateTime(uuid[, timezone])
```

**引数**

- `uuid` — バージョン7の[UUID](../data-types/uuid.md)。
- `timezone` — 返される値の[タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション） [String](../data-types/string.md)。

**返される値**

- ミリ秒精度のタイムスタンプ。UUIDが有効なバージョン7 UUIDでない場合、1970-01-01 00:00:00.000を返します。 [DateTime64(3)](../data-types/datetime64.md)。

**使用例**

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))
```

結果:

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))─┐
│                                          2024-04-22 15:30:29.048 │
└──────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')
```

結果:

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')─┐
│                                                              2024-04-22 08:30:29.048 │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## serverUUID

ClickHouseサーバーが最初に起動された時に生成されたランダムなUUIDを返します。このUUIDはClickHouseサーバーディレクトリ（例: `/var/lib/clickhouse/`）内の`uuid`ファイルに保存され、サーバーの再起動間で保持されます。

**構文**

```sql
serverUUID()
```

**返される値**

- サーバーのUUID。 [UUID](../data-types/uuid.md)。

## generateSnowflakeID

[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)を生成します。

生成されたスノーフレークIDは、ミリ秒単位の現在のUnixタイムスタンプ（41 + 1トップゼロビット）、マシンID（10ビット）、およびミリ秒内でIDを区別するためのカウンタ（12ビット）を含みます。
特定のタイムスタンプ（unix_ts_ms）の場合、カウンタは0から始まり、新しいスノーフレークIDごとに1ずつ増加します。
カウンタがオーバーフローした場合、タイムスタンプフィールドは1増加し、カウンタは0にリセットされます。

関数`generateSnowflakeID`は、並行して実行されているスレッドおよびクエリ間で、タイムスタンプ内のカウンタフィールドが単調に増加することを保証します。

:::note
生成されたスノーフレークIDはUNIXエポック1970-01-01に基づいています。
スノーフレークIDのエポックに関する標準や推奨は存在しませんが、他のシステムでの実装では異なるエポックが使用される可能性があります。例: Twitter/X（2010-11-04）またはMastodon（2015-01-01）。
:::

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|0|                         timestamp                           |
├─┼                 ┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                   |     machine_id    |    machine_seq_num    |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
```

**構文**

``` sql
generateSnowflakeID([expr, [machine_id]])
```

**引数**

- `expr` — クエリ内で関数が複数回呼ばれる場合に[共通部分式の除去](../../sql-reference/functions/index.md#common-subexpression-elimination)を回避するための任意の[式](../../sql-reference/syntax.md#syntax-expressions)。式の値は返されるスノーフレークIDに影響を与えません。省略可能。
- `machine_id` — マシンID、最下位10ビットが使用されます。 [Int64](../data-types/int-uint.md)。 省略可能。

**返される値**

UInt64型の値。

**例**

まず、UInt64型のカラムを持つテーブルを作成し、そのテーブルに生成されたスノーフレークIDを挿入します。

``` sql
CREATE TABLE tab (id UInt64) ENGINE = Memory;

INSERT INTO tab SELECT generateSnowflakeID();

SELECT * FROM tab;
```

結果:

```response
┌──────────────────id─┐
│ 7199081390080409600 │
└─────────────────────┘
```

**行ごとに複数のスノーフレークIDを生成する例**

```sql
SELECT generateSnowflakeID(1), generateSnowflakeID(2);

┌─generateSnowflakeID(1)─┬─generateSnowflakeID(2)─┐
│    7199081609652224000 │    7199081609652224001 │
└────────────────────────┴────────────────────────┘
```

**式とマシンIDを使った例**

```
SELECT generateSnowflakeID('expr', 1);

┌─generateSnowflakeID('expr', 1)─┐
│            7201148511606784002 │
└────────────────────────────────┘
```

## snowflakeToDateTime

:::warning
この関数は非推奨であり、設定[allow_deprecated_snowflake_conversion_functions](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions)を有効にした場合のみ使用できます。
この関数は将来的に削除されます。
:::

[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)のタイムスタンプコンポーネントを[DateTime](../data-types/datetime.md)形式で抽出します。

**構文**

``` sql
snowflakeToDateTime(value[, time_zone])
```

**引数**

- `value` — スノーフレークID。 [Int64](../data-types/int-uint.md)。
- `time_zone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。関数は`time_string`をタイムゾーンに従って解析します。省略可能。 [String](../data-types/string.md)。

**返される値**

- `value`のタイムスタンプコンポーネントを[DateTime](../data-types/datetime.md)値として返します。

**例**

クエリ:

``` sql
SELECT snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC');
```

結果:

```response

┌─snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC')─┐
│                                              2021-08-15 10:57:56 │
└──────────────────────────────────────────────────────────────────┘
```

## snowflakeToDateTime64

:::warning
この関数は非推奨であり、設定[allow_deprecated_snowflake_conversion_functions](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions)を有効にした場合のみ使用できます。
この関数は将来的に削除されます。
:::

[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)のタイムスタンプコンポーネントを[DateTime64](../data-types/datetime64.md)形式で抽出します。

**構文**

``` sql
snowflakeToDateTime64(value[, time_zone])
```

**引数**

- `value` — スノーフレークID。 [Int64](../data-types/int-uint.md)。
- `time_zone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。関数は`time_string`をタイムゾーンに従って解析します。省略可能。 [String](../data-types/string.md)。

**返される値**

- `value`のタイムスタンプコンポーネントを、スケールが3、つまりミリ秒精度の[DateTime64](../data-types/datetime64.md)で返します。

**例**

クエリ:

``` sql
SELECT snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC');
```

結果:

```response

┌─snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC')─┐
│                                            2021-08-15 10:58:19.841 │
└────────────────────────────────────────────────────────────────────┘
```

## dateTimeToSnowflake

:::warning
この関数は非推奨であり、設定[allow_deprecated_snowflake_conversion_functions](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions)を有効にした場合のみ使用できます。
この関数は将来的に削除されます。
:::

[DateTime](../data-types/datetime.md)値をその時点で最初の[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)に変換します。

**構文**

``` sql
dateTimeToSnowflake(value)
```

**引数**

- `value` — 時間を含む日付。[DateTime](../data-types/datetime.md)。

**返される値**

- 入力値を`Int64`データ型のその時点での最初のスノーフレークIDに変換します。

**例**

クエリ:

``` sql
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt SELECT dateTimeToSnowflake(dt);
```

結果:

```response
┌─dateTimeToSnowflake(dt)─┐
│     1426860702823350272 │
└─────────────────────────┘
```

## dateTime64ToSnowflake

:::warning
この関数は非推奨であり、設定[allow_deprecated_snowflake_conversion_functions](../../operations/settings/settings.md#allow_deprecated_snowflake_conversion_functions)を有効にした場合のみ使用できます。
この関数は将来的に削除されます。
:::

[DateTime64](../data-types/datetime64.md)をその時点で最初の[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)に変換します。

**構文**

``` sql
dateTime64ToSnowflake(value)
```

**引数**

- `value` — 時間を含む日付。[DateTime64](../data-types/datetime64.md)。

**返される値**

- 入力値を`Int64`データ型のその時点での最初のスノーフレークIDに変換します。

**例**

クエリ:

``` sql
WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64 SELECT dateTime64ToSnowflake(dt64);
```

結果:

```response
┌─dateTime64ToSnowflake(dt64)─┐
│         1426860704886947840 │
└─────────────────────────────┘
```

## snowflakeIDToDateTime

[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)のタイムスタンプコンポーネントを[DateTime](../data-types/datetime.md)型の値として返します。

**構文**

``` sql
snowflakeIDToDateTime(value[, epoch[, time_zone]])
```

**引数**

- `value` — スノーフレークID。 [UInt64](../data-types/int-uint.md)。
- `epoch` — 1970-01-01からのミリ秒単位のスノーフレークIDのエポック。デフォルトは0（1970-01-01）。Twitter/Xエポック（2015-01-01）の場合は1288834974657を指定します。省略可能。 [UInt*](../data-types/int-uint.md)。
- `time_zone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。関数は`time_string`をタイムゾーンに従って解析します。省略可能。 [String](../data-types/string.md)。

**返される値**

- `value`のタイムスタンプコンポーネントを[DateTime](../data-types/datetime.md)値として返します。

**例**

クエリ:

```sql
SELECT snowflakeIDToDateTime(7204436857747984384) AS res
```

結果:

```
┌─────────────────res─┐
│ 2024-06-06 10:59:58 │
└─────────────────────┘
```

## snowflakeIDToDateTime64

[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)のタイムスタンプコンポーネントを[DateTime64](../data-types/datetime64.md)型の値として返します。

**構文**

``` sql
snowflakeIDToDateTime64(value[, epoch[, time_zone]])
```

**引数**

- `value` — スノーフレークID。 [UInt64](../data-types/int-uint.md)。
- `epoch` — 1970-01-01からのミリ秒単位のスノーフレークIDのエポック。デフォルトは0（1970-01-01）。Twitter/Xエポック（2015-01-01）の場合は1288834974657を指定します。省略可能。 [UInt*](../data-types/int-uint.md)。
- `time_zone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。関数は`time_string`をタイムゾーンに従って解析します。省略可能。 [String](../data-types/string.md)。

**返される値**

- `value`のタイムスタンプコンポーネントをスケールが3、つまりミリ秒精度の[DateTime64](../data-types/datetime64.md)で返します。

**例**

クエリ:

```sql
SELECT snowflakeIDToDateTime64(7204436857747984384) AS res
```

結果:

```
┌─────────────────res─┐
│ 2024-06-06 10:59:58 │
└─────────────────────┘
```

## dateTimeToSnowflakeID

[DateTime](../data-types/datetime.md)値をその時点における最初の[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)に変換します。

**構文**

``` sql
dateTimeToSnowflakeID(value[, epoch])
```

**引数**

- `value` — 時間を含む日付。[DateTime](../data-types/datetime.md)。
- `epoch` — 1970-01-01からのミリ秒単位のスノーフレークIDのエポック。デフォルトは0（1970-01-01）。Twitter/Xエポック（2015-01-01）の場合は1288834974657を指定します。省略可能。 [UInt*](../data-types/int-uint.md)。

**返される値**

- 入力値を[UInt64](../data-types/int-uint.md)型のその時点の最初のスノーフレークIDに変換します。

**例**

クエリ:

```sql
SELECT toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt, dateTimeToSnowflakeID(dt) AS res;
```

結果:

```
┌──────────────────dt─┬─────────────────res─┐
│ 2021-08-15 18:57:56 │ 6832626392367104000 │
└─────────────────────┴─────────────────────┘
```

## dateTime64ToSnowflakeID

[DateTime64](../data-types/datetime64.md)をその時点における最初の[スノーフレークID](https://en.wikipedia.org/wiki/Snowflake_ID)に変換します。

**構文**

``` sql
dateTime64ToSnowflakeID(value[, epoch])
```

**引数**

- `value` — 時間を含む日付。[DateTime64](../data-types/datetime64.md)。
- `epoch` — 1970-01-01からのミリ秒単位のスノーフレークIDのエポック。デフォルトは0（1970-01-01）。Twitter/Xエポック（2015-01-01）の場合は1288834974657を指定します。省略可能。 [UInt*](../data-types/int-uint.md)。

**返される値**

- 入力値を[UInt64](../data-types/int-uint.md)型のその時点の最初のスノーフレークIDに変換します。

**例**

クエリ:

```sql
SELECT toDateTime('2021-08-15 18:57:56.493', 3, 'Asia/Shanghai') AS dt, dateTime64ToSnowflakeID(dt) AS res;
```

結果:

```
┌──────────────────────dt─┬─────────────────res─┐
│ 2021-08-15 18:57:56.493 │ 6832626394434895872 │
└─────────────────────────┴─────────────────────┘
```

## 参照

- [dictGetUUID](../functions/ext-dict-functions.md#ext_dict_functions-other)
