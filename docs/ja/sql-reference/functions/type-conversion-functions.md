---
slug: /ja/sql-reference/functions/type-conversion-functions
sidebar_position: 185
sidebar_label: 型変換
---

# 型変換関数

## データ変換の一般的な問題

ClickHouseは一般的に[C++のプログラムと同じ振る舞い](https://en.cppreference.com/w/cpp/language/implicit_conversion)をします。

`to<type>` 関数と [cast](#cast) は、いくつかのケースで異なる動作をします。例えば、[LowCardinality](../data-types/lowcardinality.md) の場合：[cast](#cast) は [LowCardinality](../data-types/lowcardinality.md)特性を削除しますが、`to<type>` 関数はしません。同様に [Nullable](../data-types/nullable.md) の場合、この動作はSQL標準とは互換性がなく、[cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable)設定を使用して変更可能です。

:::note
データ型の値が小さいデータ型（例：`Int64` から `Int32` ）に変換される場合や、互換性のないデータ型（例：`String` から `Int` ）の間での変換では、データロスの可能性があることに注意してください。結果が期待した通りであるかを注意深く確認してください。
:::

例：

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

## toBool

入力値を[`Bool`](../data-types/boolean.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toBool(expr)
```

**引数**

- `expr` — 数値または文字列を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値。
- Float32/64 型の値。
- 文字列 `true` または `false`（大文字小文字を区別しない）。

**返される値**

- 引数の評価に基づいて `true` または `false` を返します。[Bool](../data-types/boolean.md)。

**例**

クエリ：

```sql
SELECT
    toBool(toUInt8(1)),
    toBool(toInt8(-1)),
    toBool(toFloat32(1.01)),
    toBool('true'),
    toBool('false'),
    toBool('FALSE')
FORMAT Vertical
```

結果：

```response
toBool(toUInt8(1)):      true
toBool(toInt8(-1)):      true
toBool(toFloat32(1.01)): true
toBool('true'):          true
toBool('false'):         false
toBool('FALSE'):         false
```

## toInt8

入力値を[`Int8`](../data-types/int-uint.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toInt8(expr)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値またはその文字列表現。
- Float32/64 型の値。

サポートされない引数:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt8('0xc0fe');`。

:::note
入力値が[Int8](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。  
例：`SELECT toInt8(128) == -128;`。
:::

**返される値**

- 8ビット整数値。[Int8](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

```sql
SELECT
    toInt8(-8),
    toInt8(-8.8),
    toInt8('-8')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt8(-8):   -8
toInt8(-8.8): -8
toInt8('-8'): -8
```

**参照**

- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrNull`](#toint8ornull).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrZero

[`toInt8`](#toint8)のように、この関数は入力値を[Int8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt8OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`0`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt8OrZero('0xc0fe');`。

:::note
入力値が[Int8](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 8ビット整数値として成功すれば返され、そうでなければ`0`。[Int8](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt8OrZero('-8'),
    toInt8OrZero('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt8OrZero('-8'):  -8
toInt8OrZero('abc'): 0
```

**参照**

- [`toInt8`](#toint8).
- [`toInt8OrNull`](#toint8ornull).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrNull

[`toInt8`](#toint8)のように、この関数は入力値を[Int8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt8OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`\N`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt8OrNull('0xc0fe');`。

:::note
入力値が[Int8](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 8ビット整数値として成功すれば返され、そうでなければ`NULL`。[Int8](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt8OrNull('-8'),
    toInt8OrNull('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt8OrNull('-8'):  -8
toInt8OrNull('abc'): ᴺᵁᴸᴸ
```

**参照**

- [`toInt8`](#toint8).
- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrDefault

[`toInt8`](#toint8)のように、この関数は入力値を[Int8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が渡されない場合、エラーの場合には`0`が返されます。

**構文**

```sql
toInt8OrDefault(expr[, default])
```

**引数**

- `expr` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md)。
- `default` （オプション） — `Int8`型への解析が失敗した場合に返されるデフォルト値。[Int8](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 の値またはその文字列表現。
- Float32/64 の値。

デフォルト値を返す引数:
- NaN や Inf を含む Float32/64 の文字列表現。
- バイナリおよび16進数の文字列表現。例：`SELECT toInt8OrDefault('0xc0fe', CAST('-1', 'Int8'));`。

:::note
入力値が[Int8](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 8ビット整数値として成功すれば返され、そうでなければデフォルト値が返され（デフォルト値が渡されない場合は`0`が返されます）。[Int8](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
- デフォルト値の型は変換先の型と同じである必要があります。
:::

**例**

クエリ：

``` sql
SELECT
    toInt8OrDefault('-8', CAST('-1', 'Int8')),
    toInt8OrDefault('abc', CAST('-1', 'Int8'))
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt8OrDefault('-8', CAST('-1', 'Int8')):  -8
toInt8OrDefault('abc', CAST('-1', 'Int8')): -1
```

**参照**

- [`toInt8`](#toint8).
- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrNull`](#toint8ornull).

## toInt16

入力値を[`Int16`](../data-types/int-uint.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toInt16(expr)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値またはその文字列表現。
- Float32/64 型の値。

サポートされない引数:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt16('0xc0fe');`。

:::note
入力値が[Int16](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。  
例：`SELECT toInt16(32768) == -32768;`。
:::

**返される値**

- 16ビット整数値。[Int16](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

```sql
SELECT
    toInt16(-16),
    toInt16(-16.16),
    toInt16('-16')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt16(-16):    -16
toInt16(-16.16): -16
toInt16('-16'):  -16
```

**参照**

- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrNull`](#toint16ornull).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrZero

[`toInt16`](#toint16)のように、この関数は入力値を[Int16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt16OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`0`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt16OrZero('0xc0fe');`。

:::note
入力値が[Int16](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとしては見なされません。
:::

**返される値**

- 16ビット整数値として成功すれば返され、そうでなければ`0`。[Int16](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt16OrZero('-16'),
    toInt16OrZero('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt16OrZero('-16'): -16
toInt16OrZero('abc'): 0
```

**参照**

- [`toInt16`](#toint16).
- [`toInt16OrNull`](#toint16ornull).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrNull

[`toInt16`](#toint16)のように、この関数は入力値を[Int16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt16OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`\N`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt16OrNull('0xc0fe');`。

:::note
入力値が[Int16](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 16ビット整数値として成功すれば返され、そうでなければ`NULL`。[Int16](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt16OrNull('-16'),
    toInt16OrNull('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt16OrNull('-16'): -16
toInt16OrNull('abc'): ᴺᵁᴸᴸ
```

**参照**

- [`toInt16`](#toint16).
- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrDefault

[`toInt16`](#toint16)のように、この関数は入力値を[Int16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が渡されない場合、エラーの場合には`0`が返されます。

**構文**

```sql
toInt16OrDefault(expr[, default])
```

**引数**

- `expr` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md)。
- `default` （オプション） — `Int16`型への解析が失敗した場合に返されるデフォルト値。[Int16](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 の値またはその文字列表現。
- Float32/64 の値。

デフォルト値を返す引数:
- NaN や Inf を含む Float32/64 の文字列表現。
- バイナリおよび16進数の文字列表現。例：`SELECT toInt16OrDefault('0xc0fe', CAST('-1', 'Int16'));`。

:::note
入力値が[Int16](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 16ビット整数値として成功すれば返され、そうでなければデフォルト値が返され（デフォルト値が渡されない場合は`0`が返されます）。[Int16](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
- デフォルト値の型は変換先の型と同じである必要があります。
:::

**例**

クエリ：

``` sql
SELECT
    toInt16OrDefault('-16', CAST('-1', 'Int16')),
    toInt16OrDefault('abc', CAST('-1', 'Int16'))
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt16OrDefault('-16', CAST('-1', 'Int16')): -16
toInt16OrDefault('abc', CAST('-1', 'Int16')): -1
```

**参照**

- [`toInt16`](#toint16).
- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrNull`](#toint16ornull).

## toInt32

入力値を[`Int32`](../data-types/int-uint.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toInt32(expr)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値またはその文字列表現。
- Float32/64 型の値。

サポートされない引数:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt32('0xc0fe');`。

:::note
入力値が[Int32](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。  
例：`SELECT toInt32(2147483648) == -2147483648;`
:::

**返される値**

- 32ビット整数値。[Int32](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

```sql
SELECT
    toInt32(-32),
    toInt32(-32.32),
    toInt32('-32')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt32(-32):    -32
toInt32(-32.32): -32
toInt32('-32'):  -32
```

**参照**

- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrNull`](#toint32ornull).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrZero

[`toInt32`](#toint32)のように、この関数は入力値を[Int32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt32OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`0`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt32OrZero('0xc0fe');`。

:::note
入力値が[Int32](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 32ビット整数値として成功すれば返され、そうでなければ`0`。[Int32](../data-types/int-uint.md)

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt32OrZero('-32'),
    toInt32OrZero('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt32OrZero('-32'): -32
toInt32OrZero('abc'): 0
```
**参照**

- [`toInt32`](#toint32).
- [`toInt32OrNull`](#toint32ornull).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrNull

[`toInt32`](#toint32)のように、この関数は入力値を[Int32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt32OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`\N`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt32OrNull('0xc0fe');`。

:::note
入力値が[Int32](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 32ビット整数値として成功すれば返され、そうでなければ`NULL`。[Int32](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt32OrNull('-32'),
    toInt32OrNull('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt32OrNull('-32'): -32
toInt32OrNull('abc'): ᴺᵁᴸᴸ
```

**参照**

- [`toInt32`](#toint32).
- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrDefault

[`toInt32`](#toint32)のように、この関数は入力値を[Int32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が渡されない場合、エラーの場合には`0`が返されます。

**構文**

```sql
toInt32OrDefault(expr[, default])
```

**引数**

- `expr` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md)。
- `default` （オプション） — `Int32`型への解析が失敗した場合に返されるデフォルト値。[Int32](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 の値またはその文字列表現。
- Float32/64 の値。

デフォルト値を返す引数:
- NaN や Inf を含む Float32/64 の文字列表現。
- バイナリおよび16進数の文字列表現。例：`SELECT toInt32OrDefault('0xc0fe', CAST('-1', 'Int32'));`。

:::note
入力値が[Int32](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 32ビット整数値として成功すれば返され、そうでなければデフォルト値が返され（デフォルト値が渡されない場合は`0`が返されます）。[Int32](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
- デフォルト値の型は変換先の型と同じである必要があります。
:::

**例**

クエリ：

``` sql
SELECT
    toInt32OrDefault('-32', CAST('-1', 'Int32')),
    toInt32OrDefault('abc', CAST('-1', 'Int32'))
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt32OrDefault('-32', CAST('-1', 'Int32')): -32
toInt32OrDefault('abc', CAST('-1', 'Int32')): -1
```

**参照**

- [`toInt32`](#toint32).
- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrNull`](#toint32ornull).

## toInt64

入力値を[`Int64`](../data-types/int-uint.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toInt64(expr)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値またはその文字列表現。
- Float32/64 型の値。

サポートされないタイプ:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt64('0xc0fe');`。

:::note
入力値が[Int64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとしては見なされません。  
例：`SELECT toInt64(9223372036854775808) == -9223372036854775808;`
:::

**返される値**

- 64ビット整数値。[Int64](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

```sql
SELECT
    toInt64(-64),
    toInt64(-64.64),
    toInt64('-64')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt64(-64):    -64
toInt64(-64.64): -64
toInt64('-64'):  -64
```

**参照**

- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrNull`](#toint64ornull).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrZero

[`toInt64`](#toint64)のように、この関数は入力値を[Int64](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt64OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`0`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt64OrZero('0xc0fe');`。

:::note
入力値が[Int64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとしては見なされません。
:::

**返される値**

- 64ビット整数値として成功すれば返され、そうでなければ`0`。[Int64](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt64OrZero('-64'),
    toInt64OrZero('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt64OrZero('-64'): -64
toInt64OrZero('abc'): 0
```

**参照**

- [`toInt64`](#toint64).
- [`toInt64OrNull`](#toint64ornull).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrNull

[`toInt64`](#toint64)のように、この関数は入力値を[Int64](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt64OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md)。

サポートされる引数:
- (U)Int8/16/32/128/256 の文字列表現。

サポートされない引数（`\N`を返す）:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt64OrNull('0xc0fe');`。

:::note
入力値が[Int64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 64ビット整数値として成功すれば返され、そうでなければ`NULL`。[Int64](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

``` sql
SELECT
    toInt64OrNull('-64'),
    toInt64OrNull('abc')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt64OrNull('-64'): -64
toInt64OrNull('abc'): ᴺᵁᴸᴸ
```

**参照**

- [`toInt64`](#toint64).
- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrDefault

[`toInt64`](#toint64)のように、この関数は入力値を[Int64](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が渡されない場合、エラーの場合には`0`が返されます。

**構文**

```sql
toInt64OrDefault(expr[, default])
```

**引数**

- `expr` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md)。
- `default` （オプション） — `Int64`型への解析が失敗した場合に返されるデフォルト値。[Int64](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 の値またはその文字列表現。
- Float32/64 の値。

デフォルト値を返す引数:
- NaN や Inf を含む Float32/64 の文字列表現。
- バイナリおよび16進数の文字列表現。例：`SELECT toInt64OrDefault('0xc0fe', CAST('-1', 'Int64'));`。

:::note
入力値が[Int64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとは見なされません。
:::

**返される値**

- 64ビット整数値として成功すれば返され、そうでなければデフォルト値が返され（デフォルト値が渡されない場合は`0`が返されます）。[Int64](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
- デフォルト値の型は変換先の型と同じである必要があります。
:::

**例**

クエリ：

``` sql
SELECT
    toInt64OrDefault('-64', CAST('-1', 'Int64')),
    toInt64OrDefault('abc', CAST('-1', 'Int64'))
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt64OrDefault('-64', CAST('-1', 'Int64')): -64
toInt64OrDefault('abc', CAST('-1', 'Int64')): -1
```

**参照**

- [`toInt64`](#toint64).
- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrNull`](#toint64ornull).

## toInt128

入力値を[`Int128`](../data-types/int-uint.md)型の値に変換します。エラーが発生した場合は例外を投げます。

**構文**

```sql
toInt128(expr)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[式](../syntax.md/#syntax-expressions)。

サポートされる引数:
- (U)Int8/16/32/64/128/256 型の値またはその文字列表現。
- Float32/64 型の値。

サポートされない引数:
- NaN や Inf を含む Float32/64 値の文字列表現。
- バイナリおよび16進数値の文字列表現。例：`SELECT toInt128('0xc0fe');`。

:::note
入力値が[Int128](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとしては見なされません。
:::

**返される値**

- 128ビット整数値。[Int128](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用します。これは、数値の小数桁を切り捨てることを意味します。
:::

**例**

クエリ：

```sql
SELECT
    toInt128(-128),
    toInt128(-128.8),
    toInt128('-128')
FORMAT Vertical;
```

結果：

```response
Row 1:
──────
toInt128(-128):   -128
toInt128(-128.8): -128
toInt128('-128'): -128
```

**参照**

- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrNull`](#toint128ornull).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrZero

[`toInt128`](#toint128)のように、この関数は入力値を[Int128](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt128OrZero(expr)
```

**引数**


- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`0`を返す）:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt128OrZero('0xc0fe');`。

:::note
入力値が[Int128](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば128ビット整数値、それ以外の場合は`0`。[Int128](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toInt128OrZero('-128'),
    toInt128OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt128OrZero('-128'): -128
toInt128OrZero('abc'):  0
```

**関連項目**

- [`toInt128`](#toint128).
- [`toInt128OrNull`](#toint128ornull).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrNull

[`toInt128`](#toint128)と同様に、この関数は入力値を[Int128](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt128OrNull(x)
```

**引数**

- `x` — 数値の文字列表現。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`\N`を返す）
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt128OrNull('0xc0fe');`。

:::note
入力値が[Int128](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば128ビット整数値、それ以外の場合は`NULL`。[Int128](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toInt128OrNull('-128'),
    toInt128OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt128OrNull('-128'): -128
toInt128OrNull('abc'):  ᴺᵁᴸᴸ
```

**関連項目**

- [`toInt128`](#toint128).
- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrDefault

[`toInt128`](#toint128)と同様に、この関数は入力値を[Int128](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
`default`値が指定されていない場合は、エラー時に`0`を返します。

**構文**

```sql
toInt128OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).
- `default`（オプション）— 型`Int128`への解析が成功しなかった場合に返すデフォルト値。[Int128](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256。
- Float32/64。
- (U)Int8/16/32/128/256の文字列表現。

デフォルト値が返される引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt128OrDefault('0xc0fe', CAST('-1', 'Int128'));`。

:::note
入力値が[Int128](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば128ビット整数値、それ以外の場合はデフォルト値を指定した場合それを返し、指定していない場合は`0`を返します。[Int128](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
- デフォルト値の型はキャストする型と同じであるべきです。
:::

**例**

クエリ:

``` sql
SELECT
    toInt128OrDefault('-128', CAST('-1', 'Int128')),
    toInt128OrDefault('abc', CAST('-1', 'Int128'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt128OrDefault('-128', CAST('-1', 'Int128')): -128
toInt128OrDefault('abc', CAST('-1', 'Int128')):  -1
```

**関連項目**

- [`toInt128`](#toint128).
- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrNull`](#toint128ornull).

## toInt256

入力値を[`Int256`](../data-types/int-uint.md)型の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toInt256(expr)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされない引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt256('0xc0fe');`。

:::note
入力値が[Int256](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 256ビット整数値。[Int256](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toInt256(-256),
    toInt256(-256.256),
    toInt256('-256')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt256(-256):     -256
toInt256(-256.256): -256
toInt256('-256'):   -256
```

**関連項目**

- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrNull`](#toint256ornull).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrZero

[`toInt256`](#toint256)と同様に、この関数は入力値を[Int256](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toInt256OrZero(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`0`を返す）:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt256OrZero('0xc0fe');`。

:::note
入力値が[Int256](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば256ビット整数値、それ以外の場合は`0`。[Int256](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toInt256OrZero('-256'),
    toInt256OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt256OrZero('-256'): -256
toInt256OrZero('abc'):  0
```

**関連項目**

- [`toInt256`](#toint256).
- [`toInt256OrNull`](#toint256ornull).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrNull

[`toInt256`](#toint256)と同様に、この関数は入力値を[Int256](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toInt256OrNull(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`\N`を返す）
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toInt256OrNull('0xc0fe');`。

:::note
入力値が[Int256](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば256ビット整数値、それ以外の場合は`NULL`。[Int256](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toInt256OrNull('-256'),
    toInt256OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt256OrNull('-256'): -256
toInt256OrNull('abc'):  ᴺᵁᴸᴸ
```

**関連項目**

- [`toInt256`](#toint256).
- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrDefault

[`toInt256`](#toint256)と同様に、この関数は入力値を[Int256](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
`default`値が指定されていない場合は、エラー時に`0`を返します。

**構文**

```sql
toInt256OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).
- `default`（オプション）— 型`Int256`への解析が成功しなかった場合に返すデフォルト値。[Int256](../data-types/int-uint.md).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）
- バイナリおよび16進数の文字列表現、例: `SELECT toInt256OrDefault('0xc0fe', CAST('-1', 'Int256'));`

:::note
入力値が[Int256](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば256ビット整数値、それ以外の場合はデフォルト値を指定した場合それを返し、指定していない場合は`0`を返します。[Int256](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
- デフォルト値の型はキャストする型と同じであるべきです。
:::

**例**

クエリ:

``` sql
SELECT
    toInt256OrDefault('-256', CAST('-1', 'Int256')),
    toInt256OrDefault('abc', CAST('-1', 'Int256'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toInt256OrDefault('-256', CAST('-1', 'Int256')): -256
toInt256OrDefault('abc', CAST('-1', 'Int256')):  -1
```

**関連項目**

- [`toInt256`](#toint256).
- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrNull`](#toint256ornull).

## toUInt8

入力値を[`UInt8`](../data-types/int-uint.md)型の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt8(expr)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされない引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt8('0xc0fe');`。

:::note
入力値が[UInt8](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。  
例: `SELECT toUInt8(256) == 0;`。
:::

**返される値**

- 8ビット符号なし整数値。[UInt8](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt8(8),
    toUInt8(8.8),
    toUInt8('8')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt8(8):   8
toUInt8(8.8): 8
toUInt8('8'): 8
```

**関連項目**

- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrNull`](#touint8ornull).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrZero

[`toUInt8`](#touint8)と同様に、この関数は入力値を[UInt8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt8OrZero(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`0`を返す）:
- 通常のFloat32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt8OrZero('0xc0fe');`。

:::note
入力値が[UInt8](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば8ビット符号なし整数値、それ以外の場合は`0`。[UInt8](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt8OrZero('-8'),
    toUInt8OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt8OrZero('-8'):  0
toUInt8OrZero('abc'): 0
```

**関連項目**

- [`toUInt8`](#touint8).
- [`toUInt8OrNull`](#touint8ornull).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrNull

[`toUInt8`](#touint8)と同様に、この関数は入力値を[UInt8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt8OrNull(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`\N`を返す）
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt8OrNull('0xc0fe');`。

:::note
入力値が[UInt8](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば8ビット符号なし整数値、それ以外の場合は`NULL`。[UInt8](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt8OrNull('8'),
    toUInt8OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt8OrNull('8'):   8
toUInt8OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt8`](#touint8).
- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrDefault

[`toUInt8`](#touint8)と同様に、この関数は入力値を[UInt8](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
`default`値が指定されていない場合は、エラー時に`0`を返します。

**構文**

```sql
toUInt8OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).
- `default`（オプション）— 型`UInt8`への解析が成功しなかった場合に返すデフォルト値。[UInt8](../data-types/int-uint.md).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt8OrDefault('0xc0fe', CAST('0', 'UInt8'));`。

:::note
入力値が[UInt8](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば8ビット符号なし整数値、それ以外の場合はデフォルト値を指定した場合それを返し、指定していない場合は`0`を返します。[UInt8](../data-types/int-uint.md).

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
- デフォルト値の型はキャストする型と同じであるべきです。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt8OrDefault('8', CAST('0', 'UInt8')),
    toUInt8OrDefault('abc', CAST('0', 'UInt8'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt8OrDefault('8', CAST('0', 'UInt8')):   8
toUInt8OrDefault('abc', CAST('0', 'UInt8')): 0
```

**関連項目**

- [`toUInt8`](#touint8).
- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrNull`](#touint8ornull).

## toUInt16

入力値を[`UInt16`](../data-types/int-uint.md)型の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt16(expr)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされない引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt16('0xc0fe');`。

:::note
入力値が[UInt16](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。  
例: `SELECT toUInt16(65536) == 0;`。
:::

**返される値**

- 16ビット符号なし整数値。[UInt16](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt16(16),
    toUInt16(16.16),
    toUInt16('16')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt16(16):    16
toUInt16(16.16): 16
toUInt16('16'):  16
```

**関連項目**

- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrNull`](#touint16ornull).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrZero

[`toUInt16`](#touint16)と同様に、この関数は入力値を[UInt16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt16OrZero(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`0`を返す）:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt16OrZero('0xc0fe');`。

:::note
入力値が[UInt16](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば16ビット符号なし整数値、それ以外の場合は`0`。[UInt16](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt16OrZero('16'),
    toUInt16OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt16OrZero('16'):  16
toUInt16OrZero('abc'): 0
```

**関連項目**

- [`toUInt16`](#touint16).
- [`toUInt16OrNull`](#touint16ornull).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrNull

[`toUInt16`](#touint16)と同様に、この関数は入力値を[UInt16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt16OrNull(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`\N`を返す）
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt16OrNull('0xc0fe');`。

:::note
入力値が[UInt16](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば16ビット符号なし整数値、それ以外の場合は`NULL`。[UInt16](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt16OrNull('16'),
    toUInt16OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt16OrNull('16'):  16
toUInt16OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt16`](#touint16).
- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrDefault

[`toUInt16`](#touint16)と同様に、この関数は入力値を[UInt16](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
`default`値が指定されていない場合は、エラー時に`0`を返します。

**構文**

```sql
toUInt16OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).
- `default`（オプション）— 型`UInt16`への解析が成功しなかった場合に返すデフォルト値。[UInt16](../data-types/int-uint.md).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt16OrDefault('0xc0fe', CAST('0', 'UInt16'));`。

:::note
入力値が[UInt16](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば16ビット符号なし整数値、それ以外の場合はデフォルト値を指定した場合それを返し、指定していない場合は`0`を返します。[UInt16](../data-types/int-uint.md).

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
- デフォルト値の型はキャストする型と同じであるべきです。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt16OrDefault('16', CAST('0', 'UInt16')),
    toUInt16OrDefault('abc', CAST('0', 'UInt16'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt16OrDefault('16', CAST('0', 'UInt16')):  16
toUInt16OrDefault('abc', CAST('0', 'UInt16')): 0
```

**関連項目**

- [`toUInt16`](#touint16).
- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrNull`](#touint16ornull).

## toUInt32

入力値を[`UInt32`](../data-types/int-uint.md)型の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt32(expr)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされない引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt32('0xc0fe');`。

:::note
入力値が[UInt32](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。  
例: `SELECT toUInt32(4294967296) == 0;`
:::

**返される値**

- 32ビット符号なし整数値。[UInt32](../data-types/int-uint.md)。

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt32(32),
    toUInt32(32.32),
    toUInt32('32')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt32(32):    32
toUInt32(32.32): 32
toUInt32('32'):  32
```

**関連項目**

- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrNull`](#touint32ornull).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrZero

[`toUInt32`](#touint32)と同様に、この関数は入力値を[UInt32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt32OrZero(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`0`を返す）:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt32OrZero('0xc0fe');`。

:::note
入力値が[UInt32](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば32ビット符号なし整数値、それ以外の場合は`0`。[UInt32](../data-types/int-uint.md)

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt32OrZero('32'),
    toUInt32OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt32OrZero('32'):  32
toUInt32OrZero('abc'): 0
```
**関連項目**

- [`toUInt32`](#touint32).
- [`toUInt32OrNull`](#touint32ornull).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrNull

[`toUInt32`](#touint32)と同様に、この関数は入力値を[UInt32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt32OrNull(x)
```

**引数**

- `x` — 数値の文字列表現。[文字列](../data-types/string.md).

サポートされる引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされない引数（`\N`を返す）
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt32OrNull('0xc0fe');`。

:::note
入力値が[UInt32](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば32ビット符号なし整数値、それ以外の場合は`NULL`。[UInt32](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt32OrNull('32'),
    toUInt32OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt32OrNull('32'):  32
toUInt32OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt32`](#touint32).
- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrDefault

[`toUInt32`](#touint32)と同様に、この関数は入力値を[UInt32](../data-types/int-uint.md)型の値に変換しますが、エラーの場合はデフォルト値を返します。
`default`値が指定されていない場合は、エラー時に`0`を返します。

**構文**

```sql
toUInt32OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[式](../syntax.md/#syntax-expressions) / [文字列](../data-types/string.md).
- `default`（オプション）— 型`UInt32`への解析が成功しなかった場合に返すデフォルト値。[UInt32](../data-types/int-uint.md).

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現（`NaN`および`Inf`を含む）。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt32OrDefault('0xc0fe', CAST('0', 'UInt32'));`.

:::note
入力値が[UInt32](../data-types/int-uint.md)の範囲内で表現できない場合、結果のオーバーフローまたはアンダーフローが発生します。
これはエラーとは見なされません。
:::

**返される値**

- 成功すれば32ビット符号なし整数値、それ以外の場合はデフォルト値を指定した場合それを返し、指定していない場合は`0`を返します。[UInt32](../data-types/int-uint.md)。

:::note
- この関数は[ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用し、数値の小数点以下の桁を切り捨てます。
- デフォルト値の型はキャストする型と同じであるべきです。
:::

**例**

クエリ:

``` sql
SELECT
    toUInt32OrDefault('32', CAST('0', 'UInt32')),
    toUInt32OrDefault('abc', CAST('0', 'UInt32'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt32OrDefault('32', CAST('0', 'UInt32')):  32
toUInt32OrDefault('abc', CAST('0', 'UInt32')): 0
```

**関連項目**

- [`toUInt32`](#touint32).
- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrNull`](#touint32ornull).

## toUInt64

入力値を型[`UInt64`](../data-types/int-uint.md)の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt64(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされていない型:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt64('0xc0fe');`。

:::note
入力値が[UInt64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。  
例: `SELECT toUInt64(18446744073709551616) == 0;`
:::

**返される値**

- 64ビットの符号なし整数値。[UInt64](../data-types/int-uint.md)。

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt64(64),
    toUInt64(64.64),
    toUInt64('64')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt64(64):    64
toUInt64(64.64): 64
toUInt64('64'):  64
```

**関連項目**

- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrNull`](#touint64ornull).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrZero

[`toUInt64`](#touint64)と同様に、入力値を型[UInt64](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt64OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`0`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt64OrZero('0xc0fe');`。

:::note
入力値が[UInt64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合64ビットの符号なし整数値、失敗した場合は`0`。[UInt64](../data-types/int-uint.md)。

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt64OrZero('64'),
    toUInt64OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt64OrZero('64'):  64
toUInt64OrZero('abc'): 0
```

**関連項目**

- [`toUInt64`](#touint64).
- [`toUInt64OrNull`](#touint64ornull).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrNull

[`toUInt64`](#touint64)と同様に、入力値を型[UInt64](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt64OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`\N`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt64OrNull('0xc0fe');`。

:::note
入力値が[UInt64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合64ビットの符号なし整数値、失敗した場合は`NULL`。[UInt64](../data-types/int-uint.md) / [NULL](../data-types/nullable.md)。

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt64OrNull('64'),
    toUInt64OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt64OrNull('64'):  64
toUInt64OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt64`](#touint64).
- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrDefault

[`toUInt64`](#touint64)と同様に、入力値を型[UInt64](../data-types/int-uint.md)の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が指定されていない場合、エラーになると`0`が返されます。

**構文**

```sql
toUInt64OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).
- `default` (optional) — 型`UInt64`への変換が失敗した場合に返すデフォルト値。[UInt64](../data-types/int-uint.md)。

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt64OrDefault('0xc0fe', CAST('0', 'UInt64'));`。

:::note
入力値が[UInt64](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合64ビットの符号なし整数値、失敗した場合はデフォルト値が渡された場合それを返し、渡されていない場合は`0`を返す。[UInt64](../data-types/int-uint.md).

:::note
- この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
- デフォルト値の型は変換される型と一致している必要があります。
:::

**例**

クエリ:

```sql
SELECT
    toUInt64OrDefault('64', CAST('0', 'UInt64')),
    toUInt64OrDefault('abc', CAST('0', 'UInt64'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt64OrDefault('64', CAST('0', 'UInt64')):  64
toUInt64OrDefault('abc', CAST('0', 'UInt64')): 0
```

**関連項目**

- [`toUInt64`](#touint64).
- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrNull`](#touint64ornull).

## toUInt128

入力値を型[`UInt128`](../data-types/int-uint.md)の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt128(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされていない引数:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt128('0xc0fe');`。

:::note
入力値が[UInt128](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 128ビットの符号なし整数値。[UInt128](../data-types/int-uint.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt128(128),
    toUInt128(128.8),
    toUInt128('128')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt128(128):   128
toUInt128(128.8): 128
toUInt128('128'): 128
```

**関連項目**

- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrNull`](#touint128ornull).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrZero

[`toUInt128`](#touint128)と同様に、入力値を型[UInt128](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt128OrZero(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`0`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt128OrZero('0xc0fe');`。

:::note
入力値が[UInt128](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合128ビットの符号なし整数値、失敗した場合は`0`。[UInt128](../data-types/int-uint.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt128OrZero('128'),
    toUInt128OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt128OrZero('128'): 128
toUInt128OrZero('abc'): 0
```

**関連項目**

- [`toUInt128`](#touint128).
- [`toUInt128OrNull`](#touint128ornull).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrNull

[`toUInt128`](#touint128)と同様に、入力値を型[UInt128](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt128OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`\N`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt128OrNull('0xc0fe');`。

:::note
入力値が[UInt128](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合128ビットの符号なし整数値、失敗した場合は`NULL`。[UInt128](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt128OrNull('128'),
    toUInt128OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt128OrNull('128'): 128
toUInt128OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt128`](#touint128).
- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrDefault

[`toUInt128`](#toint128)と同様に、入力値を型[UInt128](../data-types/int-uint.md)の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が指定されていない場合、エラーになると`0`が返されます。

**構文**

```sql
toUInt128OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).
- `default` (optional) — 型`UInt128`への変換が失敗した場合に返すデフォルト値。[UInt128](../data-types/int-uint.md)。

サポートされている引数:
- (U)Int8/16/32/64/128/256.
- Float32/64.
- (U)Int8/16/32/128/256の文字列表現。

デフォルト値が返される引数:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt128OrDefault('0xc0fe', CAST('0', 'UInt128'));`。

:::note
入力値が[UInt128](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合128ビットの符号なし整数値、失敗した場合はデフォルト値が渡された場合それを返し、渡されていない場合は`0`を返す。[UInt128](../data-types/int-uint.md)。

:::note
- この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
- デフォルト値の型は変換される型と一致している必要があります。
:::

**例**

クエリ:

```sql
SELECT
    toUInt128OrDefault('128', CAST('0', 'UInt128')),
    toUInt128OrDefault('abc', CAST('0', 'UInt128'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt128OrDefault('128', CAST('0', 'UInt128')): 128
toUInt128OrDefault('abc', CAST('0', 'UInt128')): 0
```

**関連項目**

- [`toUInt128`](#touint128).
- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrNull`](#touint128ornull).

## toUInt256

入力値を型[`UInt256`](../data-types/int-uint.md)の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toUInt256(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

サポートされていない引数:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt256('0xc0fe');`。

:::note
入力値が[UInt256](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 256ビットの符号なし整数値。[Int256](../data-types/int-uint.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt256(256),
    toUInt256(256.256),
    toUInt256('256')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt256(256):     256
toUInt256(256.256): 256
toUInt256('256'):   256
```

**関連項目**

- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrNull`](#touint256ornull).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrZero

[`toUInt256`](#touint256)と同様に、入力値を型[UInt256](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toUInt256OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`0`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt256OrZero('0xc0fe');`。

:::note
入力値が[UInt256](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合256ビットの符号なし整数値、失敗した場合は`0`。[UInt256](../data-types/int-uint.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt256OrZero('256'),
    toUInt256OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt256OrZero('256'): 256
toUInt256OrZero('abc'): 0
```

**関連項目**

- [`toUInt256`](#touint256).
- [`toUInt256OrNull`](#touint256ornull).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrNull

[`toUInt256`](#touint256)と同様に、入力値を型[UInt256](../data-types/int-uint.md)の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toUInt256OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256の文字列表現。

サポートされていない引数（`\N`が返される）:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt256OrNull('0xc0fe');`。

:::note
入力値が[UInt256](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合256ビットの符号なし整数値、失敗した場合は`NULL`。[UInt256](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
:::

**例**

クエリ:

```sql
SELECT
    toUInt256OrNull('256'),
    toUInt256OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt256OrNull('256'): 256
toUInt256OrNull('abc'): ᴺᵁᴸᴸ
```

**関連項目**

- [`toUInt256`](#touint256).
- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrDefault

[`toUInt256`](#touint256)と同様に、入力値を型[UInt256](../data-types/int-uint.md)の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が指定されていない場合、エラーになると`0`が返されます。

**構文**

```sql
toUInt256OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).
- `default` (optional) — 型`UInt256`への変換が失敗した場合に返すデフォルト値。[UInt256](../data-types/int-uint.md).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値。

デフォルト値が返される引数:
- Float32/64の値の文字列表現、`NaN`や`Inf`を含む。
- バイナリおよび16進数の文字列表現、例: `SELECT toUInt256OrDefault('0xc0fe', CAST('0', 'UInt256'));`。

:::note
入力値が[UInt256](../data-types/int-uint.md)の範囲内で表現できない場合、結果がオーバーフローまたはアンダーフローします。
これはエラーとみなされません。
:::

**返される値**

- 成功した場合256ビットの符号なし整数値、失敗した場合はデフォルト値が渡された場合それを返し、渡されていない場合は`0`を返す。[UInt256](../data-types/int-uint.md).

:::note
- この関数は[0に向かって切り捨て丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)を使用しており、数値の小数部分を切り捨てます。
- デフォルト値の型は変換される型と一致している必要があります。
:::

**例**

クエリ:

```sql
SELECT
    toUInt256OrDefault('-256', CAST('0', 'UInt256')),
    toUInt256OrDefault('abc', CAST('0', 'UInt256'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toUInt256OrDefault('-256', CAST('0', 'UInt256')): 0
toUInt256OrDefault('abc', CAST('0', 'UInt256')):  0
```

**関連項目**

- [`toUInt256`](#touint256).
- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrNull`](#touint256ornull).

## toFloat32

入力値を型[`Float32`](../data-types/float.md)の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toFloat32(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値。
- (U)Int8/16/32/128/256の文字列表現。
- Float32/64型の値、`NaN`や`Inf`を含む。
- Float32/64の文字列表現、`NaN`や`Inf`(大文字小文字を区別しない)。

サポートされていない引数:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat32('0xc0fe');`。

**返される値**

- 32ビットの浮動小数点数値。[Float32](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat32(42.7),
    toFloat32('42.7'),
    toFloat32('NaN')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat32(42.7):   42.7
toFloat32('42.7'): 42.7
toFloat32('NaN'):  nan
```

**関連項目**

- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrNull`](#tofloat32ornull).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrZero

[`toFloat32`](#tofloat32)と同様に、入力値を型[Float32](../data-types/float.md)の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toFloat32OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256, Float32/64の文字列表現。

サポートされていない引数（`0`が返される）:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat32OrZero('0xc0fe');`.

**返される値**

- 成功した場合32ビットの浮動小数点数値、失敗した場合は`0`。[Float32](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat32OrZero('42.7'),
    toFloat32OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat32OrZero('42.7'): 42.7
toFloat32OrZero('abc'):  0
```

**関連項目**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrNull`](#tofloat32ornull).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrNull

[`toFloat32`](#tofloat32)と同様に、入力値を型[Float32](../data-types/float.md)の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toFloat32OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256, Float32/64の文字列表現。

サポートされていない引数（`\N`が返される）:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat32OrNull('0xc0fe');`.

**返される値**

- 成功した場合32ビットの浮動小数点数値、失敗した場合は`\N`。[Float32](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat32OrNull('42.7'),
    toFloat32OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat32OrNull('42.7'): 42.7
toFloat32OrNull('abc'):  ᴺᵁᴸᴸ
```

**関連項目**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrDefault

[`toFloat32`](#tofloat32)と同様に、入力値を型[Float32](../data-types/float.md)の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が指定されていない場合、エラーになると`0`が返されます。

**構文**

```sql
toFloat32OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).
- `default` (optional) — 型`Float32`への変換が失敗した場合に返すデフォルト値。[Float32](../data-types/float.md).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値。
- (U)Int8/16/32/128/256の文字列表現。
- Float32/64型の値、`NaN`や`Inf`を含む。
- Float32/64の文字列表現、`NaN`や`Inf`(大文字小文字を区別しない)。

デフォルト値が返される引数:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat32OrDefault('0xc0fe', CAST('0', 'Float32'));`.

**返される値**

- 成功した場合32ビットの浮動小数点数値、失敗した場合はデフォルト値が渡された場合それを返し、渡されていない場合は`0`を返す。[Float32](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat32OrDefault('8', CAST('0', 'Float32')),
    toFloat32OrDefault('abc', CAST('0', 'Float32'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat32OrDefault('8', CAST('0', 'Float32')):   8
toFloat32OrDefault('abc', CAST('0', 'Float32')): 0
```

**関連項目**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrNull`](#tofloat32ornull).

## toFloat64

入力値を型[`Float64`](../data-types/float.md)の値に変換します。エラーの場合は例外をスローします。

**構文**

```sql
toFloat64(expr)
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値。
- (U)Int8/16/32/128/256の文字列表現。
- Float32/64型の値、`NaN`や`Inf`を含む。
- Float32/64の文字列表現、`NaN`や`Inf`(大文字小文字を区別しない)。

サポートされていない引数:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat64('0xc0fe');`。

**返される値**

- 64ビットの浮動小数点数値。[Float64](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat64(42.7),
    toFloat64('42.7'),
    toFloat64('NaN')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat64(42.7):   42.7
toFloat64('42.7'): 42.7
toFloat64('NaN'):  nan
```

**関連項目**

- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrNull`](#tofloat64ornull).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrZero

[`toFloat64`](#tofloat64)と同様に、入力値を型[Float64](../data-types/float.md)の値に変換しますが、エラーの場合は`0`を返します。

**構文**

```sql
toFloat64OrZero(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256, Float32/64の文字列表現。

サポートされていない引数（`0`が返される）:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat64OrZero('0xc0fe');`。

**返される値**

- 成功した場合64ビットの浮動小数点数値、失敗した場合は`0`。[Float64](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat64OrZero('42.7'),
    toFloat64OrZero('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat64OrZero('42.7'): 42.7
toFloat64OrZero('abc'):  0
```

**関連項目**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrNull`](#tofloat64ornull).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrNull

[`toFloat64`](#tofloat64)と同様に、入力値を型[Float64](../data-types/float.md)の値に変換しますが、エラーの場合は`NULL`を返します。

**構文**

```sql
toFloat64OrNull(x)
```

**引数**

- `x` — 数字の文字列表現。[String](../data-types/string.md).

サポートされている引数:
- (U)Int8/16/32/128/256, Float32/64の文字列表現。

サポートされていない引数（`\N`が返される）:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat64OrNull('0xc0fe');`。

**返される値**

- 成功した場合64ビットの浮動小数点数値、失敗した場合は`\N`。[Float64](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat64OrNull('42.7'),
    toFloat64OrNull('abc')
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat64OrNull('42.7'): 42.7
toFloat64OrNull('abc'):  ᴺᵁᴸᴸ
```

**関連項目**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrDefault

[`toFloat64`](#tofloat64)と同様に、入力値を型[Float64](../data-types/float.md)の値に変換しますが、エラーの場合はデフォルト値を返します。
デフォルト値が指定されていない場合、エラーになると`0`が返されます。

**構文**

```sql
toFloat64OrDefault(expr[, default])
```

**引数**

- `expr` — 数値または数値の文字列を返す式。[Expression](../syntax.md/#syntax-expressions) / [String](../data-types/string.md).
- `default` (optional) — 型`Float64`への変換が失敗した場合に返すデフォルト値。[Float64](../data-types/float.md).

サポートされている引数:
- (U)Int8/16/32/64/128/256型の値。
- (U)Int8/16/32/128/256の文字列表現。
- Float32/64型の値、`NaN`や`Inf`を含む。
- Float32/64の文字列表現、`NaN`や`Inf`(大文字小文字を区別しない)。

デフォルト値が返される引数:
- バイナリおよび16進数の文字列表現、例: `SELECT toFloat64OrDefault('0xc0fe', CAST('0', 'Float64'));`.

**返される値**

- 成功した場合64ビットの浮動小数点数値、失敗した場合はデフォルト値が渡された場合それを返し、渡されていない場合は`0`を返す。[Float64](../data-types/float.md).

**例**

クエリ:

```sql
SELECT
    toFloat64OrDefault('8', CAST('0', 'Float64')),
    toFloat64OrDefault('abc', CAST('0', 'Float64'))
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
toFloat64OrDefault('8', CAST('0', 'Float64')):   8
toFloat64OrDefault('abc', CAST('0', 'Float64')): 0
```

**関連項目**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrNull`](#tofloat64ornull).

## toDate

引数を[Date](../data-types/date.md)データ型に変換します。

引数が[DateTime](../data-types/datetime.md)や[DateTime64](../data-types/datetime64.md)の場合、それを切り捨ててDateTimeの日付コンポーネントを残します。

```sql
SELECT
    now() AS x,
    toDate(x)
```

```response
┌───────────────────x─┬─toDate(now())─┐
│ 2022-12-30 13:44:17 │    2022-12-30 │
└─────────────────────┴───────────────┘
```

引数が[String](../data-types/string.md)の場合、[Date](../data-types/date.md)や[DateTime](../data-types/datetime.md)として解析されます。[DateTime](../data-types/datetime.md)として解析された場合、日付コンポーネントが使用されます。

```sql
SELECT
    toDate('2022-12-30') AS x,
    toTypeName(x)
```

```response
┌──────────x─┬─toTypeName(toDate('2022-12-30'))─┐
│ 2022-12-30 │ Date                             │
└────────────┴──────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```
```sql
SELECT
    toDate('2022-12-30 01:02:03') AS x,
    toTypeName(x)
```

```response
┌──────────x─┬─toTypeName(toDate('2022-12-30 01:02:03'))─┐
│ 2022-12-30 │ Date                                      │
└────────────┴───────────────────────────────────────────┘
```

引数が数値であり、UNIXタイムスタンプのように見える場合（65535より大きい場合）、現地のタイムゾーンで切り詰められた[DateTime](../data-types/datetime.md)として解釈されます。タイムゾーン引数は関数の第二引数として指定できます。切り詰めは、タイムゾーンに依存します。

```sql
SELECT
    now() AS current_time,
    toUnixTimestamp(current_time) AS ts,
    toDateTime(ts) AS time_Amsterdam,
    toDateTime(ts, 'Pacific/Apia') AS time_Samoa,
    toDate(time_Amsterdam) AS date_Amsterdam,
    toDate(time_Samoa) AS date_Samoa,
    toDate(ts) AS date_Amsterdam_2,
    toDate(ts, 'Pacific/Apia') AS date_Samoa_2
```

```response
Row 1:
──────
current_time:     2022-12-30 13:51:54
ts:               1672404714
time_Amsterdam:   2022-12-30 13:51:54
time_Samoa:       2022-12-31 01:51:54
date_Amsterdam:   2022-12-30
date_Samoa:       2022-12-31
date_Amsterdam_2: 2022-12-30
date_Samoa_2:     2022-12-31
```

上記の例は、同じUNIXタイムスタンプが異なるタイムゾーンで異なる日付として解釈されることを示しています。

引数が数値であり、65536より小さい場合、それは1970-01-01（最初のUNIX日）からの経過日数として解釈され、[Date](../data-types/date.md)に変換されます。これは、`Date`データ型の内部数値表現に対応しています。例：

```sql
SELECT toDate(12345)
```
```response
┌─toDate(12345)─┐
│    2003-10-20 │
└───────────────┘
```

この変換はタイムゾーンには依存しません。

引数がDate型の範囲に収まらない場合、それは実装定義の振る舞いを引き起こし、サポートされる最大日付まで飽和するか、オーバーフローします：
```sql
SELECT toDate(10000000000.)
```
```response
┌─toDate(10000000000.)─┐
│           2106-02-07 │
└──────────────────────┘
```

`toDate`関数は、以下のように他の形式でも記述できます：

```sql
SELECT
    now() AS time,
    toDate(time),
    DATE(time),
    CAST(time, 'Date')
```
```response
┌────────────────time─┬─toDate(now())─┬─DATE(now())─┬─CAST(now(), 'Date')─┐
│ 2022-12-30 13:54:58 │    2022-12-30 │  2022-12-30 │          2022-12-30 │
└─────────────────────┴───────────────┴─────────────┴─────────────────────┘
```


## toDateOrZero

[toDate](#todate)と同じですが、無効な引数が受け取られると[Date](../data-types/date.md)の下限を返します。引数としては[String](../data-types/string.md)のみがサポートされます。

**例**

クエリ:

``` sql
SELECT toDateOrZero('2022-12-30'), toDateOrZero('');
```

結果:

```response
┌─toDateOrZero('2022-12-30')─┬─toDateOrZero('')─┐
│                 2022-12-30 │       1970-01-01 │
└────────────────────────────┴──────────────────┘
```


## toDateOrNull

[toDate](#todate)と同じですが、無効な引数が受け取られると`NULL`を返します。引数としては[String](../data-types/string.md)のみがサポートされます。

**例**

クエリ:

``` sql
SELECT toDateOrNull('2022-12-30'), toDateOrNull('');
```

結果:

```response
┌─toDateOrNull('2022-12-30')─┬─toDateOrNull('')─┐
│                 2022-12-30 │             ᴺᵁᴸᴸ │
└────────────────────────────┴──────────────────┘
```


## toDateOrDefault

[toDate](#todate)と同様ですが、失敗した場合には、2番目の引数（指定されている場合）または[Date](../data-types/date.md)の下限を返します。

**構文**

``` sql
toDateOrDefault(expr [, default_value])
```

**例**

クエリ:

``` sql
SELECT toDateOrDefault('2022-12-30'), toDateOrDefault('', '2023-01-01'::Date);
```

結果:

```response
┌─toDateOrDefault('2022-12-30')─┬─toDateOrDefault('', CAST('2023-01-01', 'Date'))─┐
│                    2022-12-30 │                                      2023-01-01 │
└───────────────────────────────┴─────────────────────────────────────────────────┘
```


## toDateTime

入力値を[DateTime](../data-types/datetime.md)に変換します。

**構文**

``` sql
toDateTime(expr[, time_zone ])
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[Int](../data-types/int-uint.md)、[Date](../data-types/date.md)、または[DateTime](../data-types/datetime.md)。
- `time_zone` — タイムゾーン。[String](../data-types/string.md)。

:::note
`expr`が数値の場合、Unixエポックの開始からの秒数（Unixタイムスタンプとして）として解釈されます。  
`expr`が[String](../data-types/string.md)の場合、Unixタイムスタンプとして、または日付/時刻の文字列表現として解釈され得ます。  
したがって、短い数値列の文字列表現（4桁以下）はあいまいさのため明示的に無効化されています。例えば、文字列`'1999'`は年（Date/DateTimeの不完全な文字列表現）またはUnixタイムスタンプのいずれかである可能性があります。より長い数値文字列は許可されています。
:::

**返される値**

- 日付時間。[DateTime](../data-types/datetime.md)

**例**

クエリ:

``` sql
SELECT toDateTime('2022-12-30 13:44:17'), toDateTime(1685457500, 'UTC');
```

結果:

```response
┌─toDateTime('2022-12-30 13:44:17')─┬─toDateTime(1685457500, 'UTC')─┐
│               2022-12-30 13:44:17 │           2023-05-30 14:38:20 │
└───────────────────────────────────┴───────────────────────────────┘
```


## toDateTimeOrZero

[toDateTime](#todatetime)と同様ですが、無効な引数が受け取られると[DateTime](../data-types/datetime.md)の下限を返します。引数としては[String](../data-types/string.md)のみがサポートされます。

**例**

クエリ:

``` sql
SELECT toDateTimeOrZero('2022-12-30 13:44:17'), toDateTimeOrZero('');
```

結果:

```response
┌─toDateTimeOrZero('2022-12-30 13:44:17')─┬─toDateTimeOrZero('')─┐
│                     2022-12-30 13:44:17 │  1970-01-01 00:00:00 │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrNull

[toDateTime](#todatetime)と同様ですが、無効な引数が受け取られると`NULL`を返します。引数としては[String](../data-types/string.md)のみがサポートされます。

**例**

クエリ:

``` sql
SELECT toDateTimeOrNull('2022-12-30 13:44:17'), toDateTimeOrNull('');
```

結果:

```response
┌─toDateTimeOrNull('2022-12-30 13:44:17')─┬─toDateTimeOrNull('')─┐
│                     2022-12-30 13:44:17 │                 ᴺᵁᴸᴸ │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrDefault

[toDateTime](#todatetime)のようにしますが、失敗した場合には、第三の引数（指定されている場合）、または[DateTime](../data-types/datetime.md)の下限を返します。

**構文**

``` sql
toDateTimeOrDefault(expr [, time_zone [, default_value]])
```

**例**

クエリ:

``` sql
SELECT toDateTimeOrDefault('2022-12-30 13:44:17'), toDateTimeOrDefault('', 'UTC', '2023-01-01'::DateTime('UTC'));
```

結果:

```response
┌─toDateTimeOrDefault('2022-12-30 13:44:17')─┬─toDateTimeOrDefault('', 'UTC', CAST('2023-01-01', 'DateTime(\'UTC\')'))─┐
│                        2022-12-30 13:44:17 │                                                     2023-01-01 00:00:00 │
└────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘
```


## toDate32

引数を[Date32](../data-types/date32.md)データ型に変換します。値が範囲外の場合、`toDate32`は[Date32](../data-types/date32.md)でサポートされる境界値を返します。引数が[Date](../data-types/date.md)型の場合、その境界が考慮されます。

**構文**

``` sql
toDate32(expr)
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[UInt32](../data-types/int-uint.md)または[Date](../data-types/date.md)。

**返される値**

- カレンダーの日付。型[Date32](../data-types/date32.md)。

**例**

1. 値が範囲内の場合:

``` sql
SELECT toDate32('1955-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1925-01-01'))─┐
│ 1955-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

2. 値が範囲外の場合:

``` sql
SELECT toDate32('1899-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1899-01-01'))─┐
│ 1900-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

3. [Date](../data-types/date.md)引数の場合:

``` sql
SELECT toDate32(toDate('1899-01-01')) AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32(toDate('1899-01-01')))─┐
│ 1970-01-01 │ Date32                                     │
└────────────┴────────────────────────────────────────────┘
```

## toDate32OrZero

[toDate32](#todate32)と同様ですが、無効な引数が受け取られると[Date32](../data-types/date32.md)の最小値を返します。

**例**

クエリ:

``` sql
SELECT toDate32OrZero('1899-01-01'), toDate32OrZero('');
```

結果:

```response
┌─toDate32OrZero('1899-01-01')─┬─toDate32OrZero('')─┐
│                   1900-01-01 │         1900-01-01 │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrNull

[toDate32](#todate32)と同様ですが、無効な引数が受け取られると`NULL`を返します。

**例**

クエリ:

``` sql
SELECT toDate32OrNull('1955-01-01'), toDate32OrNull('');
```

結果:

```response
┌─toDate32OrNull('1955-01-01')─┬─toDate32OrNull('')─┐
│                   1955-01-01 │               ᴺᵁᴸᴸ │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrDefault

引数を[Date32](../data-types/date32.md)データ型に変換します。値が範囲外の場合、`toDate32OrDefault`は[Date32](../data-types/date32.md)でサポートされる下側の境界値を返します。引数が[Date](../data-types/date.md)型の場合、その境界が考慮されます。無効な引数が受け取られるとデフォルト値を返します。

**例**

クエリ:

``` sql
SELECT
    toDate32OrDefault('1930-01-01', toDate32('2020-01-01')),
    toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'));
```

結果:

```response
┌─toDate32OrDefault('1930-01-01', toDate32('2020-01-01'))─┬─toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'))─┐
│                                              1930-01-01 │                                                2020-01-01 │
└─────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

## toDateTime64

入力値を[DateTime64](../data-types/datetime64.md)型の値に変換します。

**構文**

``` sql
toDateTime64(expr, scale, [timezone])
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[UInt32](../data-types/int-uint.md)、[Float](../data-types/float.md)または[DateTime](../data-types/datetime.md)。
- `scale` - ティックサイズ（精度）: 10<sup>-精度</sup>秒。有効範囲: [ 0 : 9 ]。
- `timezone`（オプション）- 指定したdatetime64オブジェクトのタイムゾーン。

**返される値**

- カレンダーの日付と時刻、サブ秒精度あり。[DateTime64](../data-types/datetime64.md)。

**例**

1. 値が範囲内の場合:

``` sql
SELECT toDateTime64('1955-01-01 00:00:00.000', 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('1955-01-01 00:00:00.000', 3))─┐
│ 1955-01-01 00:00:00.000 │ DateTime64(3)                                          │
└─────────────────────────┴────────────────────────────────────────────────────────┘
```

2. 精度を持つ小数として:

``` sql
SELECT toDateTime64(1546300800.000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800., 3))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3)                            │
└─────────────────────────┴──────────────────────────────────────────┘
```

小数点なしでは、値は秒単位のUnixタイムスタンプとして扱われます:

``` sql
SELECT toDateTime64(1546300800000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800000, 3))─┐
│ 2282-12-31 00:00:00.000 │ DateTime64(3)                              │
└─────────────────────────┴────────────────────────────────────────────┘
```

3. `timezone`付き:

``` sql
SELECT toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul') AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul'))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3, 'Asia/Istanbul')                                      │
└─────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

## toDateTime64OrZero

[toDateTime64](#todatetime64)と同様に、この関数は入力値を[DateTime64](../data-types/datetime64.md)型の値に変換しますが、無効な引数が受け取られると[DateTime64](../data-types/datetime64.md)の最小値を返します。

**構文**

``` sql
toDateTime64OrZero(expr, scale, [timezone])
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[UInt32](../data-types/int-uint.md)、[Float](../data-types/float.md)または[DateTime](../data-types/datetime.md)。
- `scale` - ティックサイズ（精度）: 10<sup>-精度</sup>秒。有効範囲: [ 0 : 9 ]。
- `timezone`（オプション）- 指定したDateTime64オブジェクトのタイムゾーン。

**返される値**

- カレンダーの日付と時刻、サブ秒精度あり、それ以外は`DateTime64`の最小値: `1970-01-01 01:00:00.000`。[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

```sql
SELECT toDateTime64OrZero('2008-10-12 00:00:00 00:30:30', 3) AS invalid_arg
```

結果:

```response
┌─────────────invalid_arg─┐
│ 1970-01-01 01:00:00.000 │
└─────────────────────────┘
```

**参照**

- [toDateTime64](#todatetime64).
- [toDateTime64OrNull](#todatetime64ornull).
- [toDateTime64OrDefault](#todatetime64ordefault).

## toDateTime64OrNull

[toDateTime64](#todatetime64)と同様に、この関数は入力値を[DateTime64](../data-types/datetime64.md)型の値に変換しますが、無効な引数が受け取られると`NULL`を返します。

**構文**

``` sql
toDateTime64OrNull(expr, scale, [timezone])
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[UInt32](../data-types/int-uint.md)、[Float](../data-types/float.md)または[DateTime](../data-types/datetime.md)。
- `scale` - ティックサイズ（精度）: 10<sup>-精度</sup>秒。有効範囲: [ 0 : 9 ]。
- `timezone`（オプション）- 指定したDateTime64オブジェクトのタイムゾーン。

**返される値**

- カレンダーの日付と時刻、サブ秒精度あり、それ以外は`NULL`。[DateTime64](../data-types/datetime64.md)/[NULL](../data-types/nullable.md)。

**例**

クエリ:

```sql
SELECT
    toDateTime64OrNull('1976-10-18 00:00:00.30', 3) AS valid_arg,
    toDateTime64OrNull('1976-10-18 00:00:00 30', 3) AS invalid_arg
```

結果:

```response
┌───────────────valid_arg─┬─invalid_arg─┐
│ 1976-10-18 00:00:00.300 │        ᴺᵁᴸᴸ │
└─────────────────────────┴─────────────┘
```

**参照**

- [toDateTime64](#todatetime64).
- [toDateTime64OrZero](#todatetime64orzero).
- [toDateTime64OrDefault](#todatetime64ordefault).

## toDateTime64OrDefault

[toDateTime64](#todatetime64)と同様に、この関数は入力値を[DateTime64](../data-types/datetime64.md)型の値に変換しますが、
無効な引数が受け取られると、[DateTime64](../data-types/datetime64.md)のデフォルト値または指定されたデフォルトを返します。

**構文**

``` sql
toDateTime64OrNull(expr, scale, [timezone, default])
```

**引数**

- `expr` — 値。[String](../data-types/string.md)、[UInt32](../data-types/int-uint.md)、[Float](../data-types/float.md)または[DateTime](../data-types/datetime.md)。
- `scale` - ティックサイズ（精度）: 10<sup>-精度</sup>秒。有効範囲: [ 0 : 9 ]。
- `timezone`（オプション）- 指定したDateTime64オブジェクトのタイムゾーン。
- `default`（オプション）- 無効な引数が受け取られた場合に返すデフォルト値。[DateTime64](../data-types/datetime64.md)。

**返される値**

- カレンダーの日付と時刻、サブ秒精度あり、それ以外は`DateTime64`の最小値または指定されたデフォルト値。[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

```sql
SELECT
    toDateTime64OrDefault('1976-10-18 00:00:00 30', 3) AS invalid_arg,
    toDateTime64OrDefault('1976-10-18 00:00:00 30', 3, 'UTC', toDateTime64('2001-01-01 00:00:00.00',3)) AS invalid_arg_with_default
```

結果:

```response
┌─────────────invalid_arg─┬─invalid_arg_with_default─┐
│ 1970-01-01 01:00:00.000 │  2000-12-31 23:00:00.000 │
└─────────────────────────┴──────────────────────────┘
```

**参照**

- [toDateTime64](#todatetime64).
- [toDateTime64OrZero](#todatetime64orzero).
- [toDateTime64OrNull](#todatetime64ornull).

## toDecimal32

入力値を[`Decimal(9, S)`](../data-types/decimal.md)型の値に変換します。エラーが発生した場合に例外をスローします。

**構文**

```sql
toDecimal32(expr, S)
```

**引数**

- `expr` — 数値またはその文字列表現を返す式。[Expression](../syntax.md/#syntax-expressions)。
- `S` — 小数点以下の桁数を指定します。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256型の値または文字列表現。
- Float32/64型の値または文字列表現。

サポートされていない引数:
- Float32/64の`NaN`および`Inf`の文字列表現（大文字小文字を区別しない）。
- 2進数および16進数の文字列表現、例：`SELECT toDecimal32('0xc0fe', 1);`。

:::note
`expr`の値が`Decimal32`の範囲を超えると、オーバーフローが発生する可能性があります: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`。
小数点以下の過剰な桁は切り捨てられます（四捨五入しません）。
整数部の過剰な桁は例外を引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64入力で予期しない動作をする可能性があります。これは浮動小数点演算を使用しているためです。
例：`toDecimal32(1.15, 2)`は`1.14`となります。なぜなら1.15 * 100は浮動小数点では114.99となるためです。
内部整数型を使用するために文字列入力を使用することができます：`toDecimal32('1.15', 2) = 1.15`
:::

**返される値**

- `Decimal(9, S)`型の値。[Decimal32(S)](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal32(2, 1) AS a, toTypeName(a) AS type_a,
    toDecimal32(4.2, 2) AS b, toTypeName(b) AS type_b,
    toDecimal32('4.2', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:      2
type_a: Decimal(9, 1)
b:      4.2
type_b: Decimal(9, 2)
c:      4.2
type_c: Decimal(9, 3)
```

**参照**

- [`toDecimal32OrZero`](#todecimal32orzero).
- [`toDecimal32OrNull`](#todecimal32ornull).
- [`toDecimal32OrDefault`](#todecimal32ordefault).

## toDecimal32OrZero

[`toDecimal32`](#todecimal32)と同様に、この関数は入力値を[Decimal(9, S)](../data-types/decimal.md)型の値に変換しますが、エラーが発生した場合は`0`を返します。

**構文**

```sql
toDecimal32OrZero(expr, S)
```

**引数**

- `expr` — 数値の文字列表現。[String](../data-types/string.md)。
- `S` — 小数点以下の桁数を指定します。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256型の文字列表現。
- Float32/64型の文字列表現。

サポートされていない引数:
- Float32/64の`NaN`および`Inf`の文字列表現。
- 2進数および16進数の文字列表現、例：`SELECT toDecimal32OrZero('0xc0fe', 1);`。

:::note
`expr`の値が`Decimal32`の範囲を超えると、オーバーフローが発生する可能性があります: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`。
小数点以下の過剰な桁は切り捨てられます（四捨五入しません）。
整数部の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功した場合は`Decimal(9, S)`型の値、それ以外の場合は`S`小数点以下の桁を持つ`0`。[Decimal32(S)](../data-types/decimal.md)。

**例**

クエリ:

``` sql
SELECT
    toDecimal32OrZero(toString(-1.111), 5) AS a,
    toTypeName(a),
    toDecimal32OrZero(toString('Inf'), 5) as b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             -1.111
toTypeName(a): Decimal(9, 5)
b:             0
toTypeName(b): Decimal(9, 5)
```

**参照**

- [`toDecimal32`](#todecimal32).
- [`toDecimal32OrNull`](#todecimal32ornull).
- [`toDecimal32OrDefault`](#todecimal32ordefault).

## toDecimal32OrNull

[`toDecimal32`](#todecimal32)と同様に、この関数は入力値を[Nullable(Decimal(9, S))](../data-types/decimal.md)型の値に変換しますが、エラーが発生した場合は`NULL`を返します。

**構文**

```sql
toDecimal32OrNull(expr, S)
```

**引数**

- `expr` — 数値の文字列表現。[String](../data-types/string.md)。
- `S` — 小数点以下の桁数を指定します。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- (U)Int8/16/32/64/128/256型の文字列表現。
- Float32/64型の文字列表現。

サポートされていない引数:
- Float32/64の`NaN`および`Inf`の文字列表現。
- 2進数および16進数の文字列表現、例：`SELECT toDecimal32OrNull('0xc0fe', 1);`。

:::note
`expr`の値が`Decimal32`の範囲を超えると、オーバーフローが発生する可能性があります: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`。
小数点以下の過剰な桁は切り捨てられます（四捨五入しません）。
整数部の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功した場合は`Nullable(Decimal(9, S))`型の値、それ以外の場合は同じ型の`NULL`。[Decimal32(S)](../data-types/decimal.md)。

**例**

クエリ:

``` sql
SELECT
```
```sql
toDecimal64OrNull(toString(0.0001), 18) AS a,
toTypeName(a),
toDecimal64OrNull(toString('Inf'), 18) as b,
toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Nullable(Decimal(18, 18))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(18, 18))
```

**関連項目**

- [`toDecimal64`](#todecimal64).
- [`toDecimal64OrZero`](#todecimal64orzero).
- [`toDecimal64OrDefault`](#todecimal64ordefault).

## toDecimal64OrDefault

[`toDecimal64`](#todecimal64) と似ており、この関数は、入力値を [Decimal(18, S)](../data-types/decimal.md) 型の値に変換しますが、エラーの場合にはデフォルト値を返します。

**構文**

```sql
toDecimal64OrDefault(expr, S[, default])
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から18の範囲。[UInt8](../data-types/int-uint.md)。
- `default` (オプション) — `Decimal64(S)` への変換に失敗した場合に返すデフォルト値。[Decimal64(S)](../data-types/decimal.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal64OrDefault('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal64` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(18 - S), 1 * 10^(18 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64 入力で作業する際に期待しない動作をすることがあります。操作は浮動小数点命令を使用して実行されます。
例: `toDecimal64OrDefault(1.15, 2)` は `1.14` に等しくなります。なぜなら、1.15 * 100 は浮動小数点で 114.99 だからです。
文字列入力を使用することができます。これは、操作が基盤となる整数型を使用するようにします: `toDecimal64OrDefault('1.15', 2) = 1.15`
:::

**返される値**

- 成功時には `Decimal(18, S)` 型の値、失敗時にはデフォルト値が指定されていればその値、指定されていなければ `0`。 [Decimal64(S)](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal64OrDefault(toString(0.0001), 18) AS a,
    toTypeName(a),
    toDecimal64OrDefault('Inf', 0, CAST('-1', 'Decimal64(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(18, 18)
b:             -1
toTypeName(b): Decimal(18, 0)
```

**関連項目**

- [`toDecimal64`](#todecimal64).
- [`toDecimal64OrZero`](#todecimal64orzero).
- [`toDecimal64OrNull`](#todecimal64ornull).

## toDecimal128

入力値を [`Decimal(38, S)`](../data-types/decimal.md) 型に変換し、スケール `S` を持つ値にします。エラーの場合には例外をスローします。

**構文**

```sql
toDecimal128(expr, S)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[Expression](../syntax.md/#syntax-expressions)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から38の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の値または文字列表現。
- 型 Float32/64 の値または文字列表現。

サポートされない引数:
- 値または文字列表現の Float32/64 値 `NaN` および `Inf`（大文字小文字を区別しない）。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal128('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal128` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁は例外を引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64 入力で作業する際に期待しない動作をすることがあります。操作は浮動小数点命令を使用して実行されます。
例: `toDecimal128(1.15, 2)` は `1.14` に等しくなります。なぜなら、1.15 * 100 は浮動小数点で 114.99 だからです。
文字列入力を使用することができます。これは、操作が基盤となる整数型を使用するようにします: `toDecimal128('1.15', 2) = 1.15`
:::

**返される値**

- 型 `Decimal(38, S)` の値。[Decimal128(S)](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal128(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal128(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal128('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:      99
type_a: Decimal(38, 1)
b:      99.67
type_b: Decimal(38, 2)
c:      99.67
type_c: Decimal(38, 3)
```

**関連項目**

- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrNull`](#todecimal128ornull).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrZero

[`toDecimal128`](#todecimal128) と似ており、この関数は、入力値を [Decimal(38, S)](../data-types/decimal.md) 型の値に変換しますが、エラーの場合には `0` を返します。

**構文**

```sql
toDecimal128OrZero(expr, S)
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から38の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal128OrZero('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal128` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功時には型 `Decimal(38, S)` の値、失敗時には `S` 小数点を持つ `0`。[Decimal128(S)](../data-types/decimal.md).

**例**

クエリ:

```sql
SELECT
    toDecimal128OrZero(toString(0.0001), 38) AS a,
    toTypeName(a),
    toDecimal128OrZero(toString('Inf'), 38) as b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(38, 38)
b:             0
toTypeName(b): Decimal(38, 38)
```

**関連項目**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrNull`](#todecimal128ornull).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrNull

[`toDecimal128`](#todecimal128) と似ており、この関数は、入力値を [Nullable(Decimal(38, S))](../data-types/decimal.md) 型の値に変換しますが、エラーの場合には `NULL` を返します。

**構文**

```sql
toDecimal128OrNull(expr, S)
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から38の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal128OrNull('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal128` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功時には `Nullable(Decimal(38, S))` 型の値、失敗時には同じ型の `NULL`。[Decimal128(S)](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal128OrNull(toString(1/42), 38) AS a,
    toTypeName(a),
    toDecimal128OrNull(toString('Inf'), 38) as b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Nullable(Decimal(38, 38))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(38, 38))
```

**関連項目**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrDefault

[`toDecimal128`](#todecimal128) と似ており、この関数は、入力値を [Decimal(38, S)](../data-types/decimal.md) 型の値に変換しますが、エラーの場合にはデフォルト値を返します。

**構文**

```sql
toDecimal128OrDefault(expr, S[, default])
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から38の範囲。[UInt8](../data-types/int-uint.md)。
- `default` (オプション) — `Decimal128(S)` 型への変換に失敗した場合に返すデフォルト値。[Decimal128(S)](../data-types/decimal.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal128OrDefault('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal128` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64 入力で作業する際に期待しない動作をすることがあります。操作は浮動小数点命令を使用して実行されます。
例: `toDecimal128OrDefault(1.15, 2)` は `1.14` に等しくなります。なぜなら、1.15 * 100 は浮動小数点で 114.99 だからです。
文字列入力を使用することができます。これは、操作が基盤となる整数型を使用するようにします: `toDecimal128OrDefault('1.15', 2) = 1.15`
:::

**返される値**

- 成功時には `Decimal(38, S)` 型の値、失敗時にはデフォルト値が指定されていればその値、指定されていなければ `0`。[Decimal128(S)](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal128OrDefault(toString(1/42), 18) AS a,
    toTypeName(a),
    toDecimal128OrDefault('Inf', 0, CAST('-1', 'Decimal128(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Decimal(38, 18)
b:             -1
toTypeName(b): Decimal(38, 0)
```

**関連項目**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrNull`](#todecimal128ornull).

## toDecimal256

入力値を [`Decimal(76, S)`](../data-types/decimal.md) 型に変換し、スケール `S` を持つ値にします。エラーの場合には例外をスローします。

**構文**

```sql
toDecimal256(expr, S)
```

**引数**

- `expr` — 数値または数値の文字列表現を返す式。[Expression](../syntax.md/#syntax-expressions)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から76の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の値または文字列表現。
- 型 Float32/64 の値または文字列表現。

サポートされない引数:
- 値または文字列表現の Float32/64 値 `NaN` および `Inf`（大文字小文字を区別しない）。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal256('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal256` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁は例外を引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64 入力で作業する際に期待しない動作をすることがあります。操作は浮動小数点命令を使用して実行されます。
例: `toDecimal256(1.15, 2)` は `1.14` に等しくなります。なぜなら、1.15 * 100 は浮動小数点で 114.99 だからです。
文字列入力を使用することができます。これは、操作が基盤となる整数型を使用するようにします: `toDecimal256('1.15', 2) = 1.15`
:::

**返される値**

- 型 `Decimal(76, S)` の値。[Decimal256(S)](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal256(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal256(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal256('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:      99
type_a: Decimal(76, 1)
b:      99.67
type_b: Decimal(76, 2)
c:      99.67
type_c: Decimal(76, 3)
```

**関連項目**

- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrNull`](#todecimal256ornull).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrZero

[`toDecimal256`](#todecimal256) と似ており、この関数は、入力値を [Decimal(76, S)](../data-types/decimal.md) 型の値に変換しますが、エラーの場合には `0` を返します。

**構文**

```sql
toDecimal256OrZero(expr, S)
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から76の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal256OrZero('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal256` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功時には型 `Decimal(76, S)` の値、失敗時には `S` 小数点を持つ `0`。[Decimal256(S)](../data-types/decimal.md).

**例**

クエリ:

```sql
SELECT
    toDecimal256OrZero(toString(0.0001), 76) AS a,
    toTypeName(a),
    toDecimal256OrZero(toString('Inf'), 76) as b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(76, 76)
b:             0
toTypeName(b): Decimal(76, 76)
```

**関連項目**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrNull`](#todecimal256ornull).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrNull

[`toDecimal256`](#todecimal256) と似ており、この関数は、入力値を [Nullable(Decimal(76, S))](../data-types/decimal.md) 型の値に変換しますが、エラーの場合には `NULL` を返します。

**構文**

```sql
toDecimal256OrNull(expr, S)
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から76の範囲。[UInt8](../data-types/int-uint.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal256OrNull('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal256` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

**返される値**

- 成功時には `Nullable(Decimal(76, S))` 型の値、失敗時には同じ型の `NULL`。[Decimal256(S)](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal256OrNull(toString(1/42), 76) AS a,
    toTypeName(a),
    toDecimal256OrNull(toString('Inf'), 76) as b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Nullable(Decimal(76, 76))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(76, 76))
```

**関連項目**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrDefault

[`toDecimal256`](#todecimal256) と似ており、この関数は、入力値を [Decimal(76, S)](../data-types/decimal.md) 型の値に変換しますが、エラーの場合にはデフォルト値を返します。

**構文**

```sql
toDecimal256OrDefault(expr, S[, default])
```

**引数**

- `expr` — 数字の文字列表現。[String](../data-types/string.md)。
- `S` — 小数部分の最大桁数を指定するスケールパラメータ。0から76の範囲。[UInt8](../data-types/int-uint.md)。
- `default` (オプション) — `Decimal256(S)` 型への変換に失敗した場合に返すデフォルト値。[Decimal256(S)](../data-types/decimal.md)。

サポートされる引数:
- 型 (U)Int8/16/32/64/128/256 の文字列表現。
- 型 Float32/64 の文字列表現。

サポートされない引数:
- Float32/64 値 `NaN` および `Inf` の文字列表現。
- バイナリおよび16進数値の文字列表現、例: `SELECT toDecimal256OrDefault('0xc0fe', 1);`。

:::note
`expr` の値が `Decimal256` の範囲を超えるとオーバーフローが発生する可能性があります: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`。
分数部分の過剰な桁は切り捨てられます（丸められません）。
整数部分の過剰な桁はエラーを引き起こします。
:::

:::warning
変換は余分な桁を切り捨て、Float32/Float64 入力で作業する際に期待しない動作をすることがあります。操作は浮動小数点命令を使用して実行されます。
例: `toDecimal256OrDefault(1.15, 2)` は `1.14` に等しくなります。なぜなら、1.15 * 100 は浮動小数点で 114.99 だからです。
文字列入力を使用することができます。これは、操作が基盤となる整数型を使用するようにします: `toDecimal256OrDefault('1.15', 2) = 1.15`
:::

**返される値**

- 成功時には `Decimal(76, S)` 型の値、失敗時にはデフォルト値が指定されていればその値、指定されていなければ `0`。[Decimal256(S)](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT
    toDecimal256OrDefault(toString(1/42), 76) AS a,
    toTypeName(a),
    toDecimal256OrDefault('Inf', 0, CAST('-1', 'Decimal256(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

結果:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Decimal(76, 76)
b:             -1
toTypeName(b): Decimal(76, 0)
```

**関連項目**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrNull`](#todecimal256ornull).

## toString

数値、文字列（固定文字列ではない）、日付、および日時間の間で変換するための関数。
これらのすべての関数は1つの引数を受け入れます。

文字列への変換、または文字列から変換する場合、値は TabSeparated 形式（およびほぼすべての他のテキスト形式）の規則を使用してフォーマットまたはパースされます。文字列がパースできない場合、例外がスローされリクエストがキャンセルされます。

日付を数値に変換する際、またはその逆の場合、日付はUnixエポックの開始以降の日数に対応します。
日時間を数値に変換する際、またはその逆の場合、日時間はUnixエポックの開始以降の秒数に対応します。

toDate/toDateTime 関数のための日付と日時間のフォーマットは次のように定義されています:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

例外として、Date に UInt32、Int32、UInt64、または Int64 型から変換し、数が65536以上の場合、数値はUNIXタイムスタンプとして解釈されます（そして日数としては解釈されません）そして、日付に丸められます。これは、 `toDate(unix_timestamp)` を書くという一般的な事例をサポートすることを可能にします。さもなければエラーとなり、より面倒な `toDate(toDateTime(unix_timestamp))` を書かなければなりません。

日付と日時間の間の変換は自然な方法で行われます: null 時間を追加するか時間をドロップします。

数値型間の変換は C++ における異なる数値型間の代入の規則と同じものを使用します。

さらに、DateTime引数の toString 関数は、タイムゾーンの名前を含んだ第二の文字列引数を取ることができます。例: `Asia/Yekaterinburg` この場合、時間は指定されたタイムゾーンに従ってフォーマットされます。

**例**

クエリ:

``` sql
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

結果:

```response
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

また、`toUnixTimestamp` 関数も参照してください。

## toFixedString

[String](../data-types/string.md) 型の引数を [FixedString(N)](../data-types/fixedstring.md) 型（固定長 N の文字列）に変換します。
文字列が N バイト未満の場合、右側にnullバイトが追加されます。文字列が N バイトを超える場合、例外がスローされます。

**構文**

```sql
toFixedString(s, N)
```

**引数**

- `s` — 固定文字列に変換する文字列。[String](../data-types/string.md)。
- `N` — 長さ N。[UInt8](../data-types/int-uint.md)

**返される値**

- 長さ N の `s` の固定文字列。[FixedString](../data-types/fixedstring.md).

**例**

クエリ:

``` sql
SELECT toFixedString('foo', 8) AS s;
```

結果:

```response
┌─s─────────────┐
│ foo\0\0\0\0\0 │
└───────────────┘
```

## toStringCutToZero

String または FixedString 引数を受け付けます。最初に見つかったゼロバイトで内容を切り取った文字列を返します。

**構文**

```sql
toStringCutToZero(s)
```

**例**

クエリ:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut;
```

結果:

```response
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

クエリ:

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut;
```

結果:

```response
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## toDecimalString

数値を指定された小数桁数で String に変換します。

**構文**

``` sql
toDecimalString(number, scale)
```

**引数**

- `number` — String として表現される値。[Int, UInt](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Decimal](../data-types/decimal.md)。
- `scale` — 小数桁数。[UInt8](../data-types/int-uint.md)。
    * [Decimal](../data-types/decimal.md) および [Int, UInt](../data-types/int-uint.md) 型の最大スケールは 77 です（Decimal の有効桁数の最大可能数です）。
    * [Float](../data-types/float.md) の最大スケールは 60 です。

**返される値**

- 指定した桁数（スケール）で入力値を [String](../data-types/string.md) として表現したもの。
    要求されたスケールが元の数値のスケールより小さい場合、通常の算術に従って四捨五入されます。

**例**

クエリ:

``` sql
SELECT toDecimalString(CAST('64.32', 'Float64'), 5);
```

結果:

```response
┌toDecimalString(CAST('64.32', 'Float64'), 5)─┐
│ 64.32000                                    │
└─────────────────────────────────────────────┘
```

## reinterpretAsUInt8

入力値をUInt8型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt8(x)
```

**パラメータ**

- `x`: UInt8 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt8 として再解釈された値 `x`。[UInt8](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toInt8(257) AS x,
    toTypeName(x),
    reinterpretAsUInt8(x) AS res,
    toTypeName(res);
```

結果:

```response
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ Int8          │   1 │ UInt8           │
└───┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsUInt16

入力値をUInt16型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt16(x)
```

**パラメータ**

- `x`: UInt16 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt16 として再解釈された値 `x`。[UInt16](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toUInt8(257) AS x,
    toTypeName(x),
    reinterpretAsUInt16(x) AS res,
    toTypeName(res);
```

結果:

```response
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ UInt8         │   1 │ UInt16          │
└───┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsUInt32

入力値をUInt32型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt32(x)
```

**パラメータ**

- `x`: UInt32 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt32 として再解釈された値 `x`。[UInt32](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toUInt16(257) AS x,
    toTypeName(x),
    reinterpretAsUInt32(x) AS res,
    toTypeName(res)
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt16        │ 257 │ UInt32          │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsUInt64

入力値をUInt64型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt64(x)
```

**パラメータ**

- `x`: UInt64 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt64 として再解釈された値 `x`。[UInt64](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toUInt32(257) AS x,
    toTypeName(x),
    reinterpretAsUInt64(x) AS res,
    toTypeName(res)
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt32        │ 257 │ UInt64          │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsUInt128

入力値をUInt128型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt128(x)
```

**パラメータ**

- `x`: UInt128 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt128 として再解釈された値 `x`。[UInt128](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toUInt64(257) AS x,
    toTypeName(x),
    reinterpretAsUInt128(x) AS res,
    toTypeName(res)
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt64        │ 257 │ UInt128         │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsUInt256

入力値をUInt256型の値として扱うことによるバイト再解釈を実行します。[`CAST`](#cast)とは異なり、この関数は元の値を保持しようとせず、ターゲット型が入力型を表現できない場合、出力は無意味です。

**構文**

```sql
reinterpretAsUInt256(x)
```

**パラメータ**

- `x`: UInt256 としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md)、[Float](../data-types/float.md)、[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md)、[UUID](../data-types/uuid.md)、[String](../data-types/string.md)、または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt256 として再解釈された値 `x`。[UInt256](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256).

**例**

クエリ:

```sql
SELECT
    toUInt128(257) AS x,
    toTypeName(x),
    reinterpretAsUInt256(x) AS res,
    toTypeName(res)
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt128        │ 257 │ UInt256         │
└─────┴───────────────┴─────┴─────────────────┘
```

入力値をUInt256型の値として扱うことでバイトの再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsUInt256(x)
```

**パラメータ**

- `x`: UInt256としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- UInt256として再解釈された値 `x`。 [UInt256](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256)。

**例**

クエリ:

```sql
SELECT
    toUInt128(257) AS x,
    toTypeName(x),
    reinterpretAsUInt256(x) AS res,
    toTypeName(res)
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ UInt128       │ 257 │ UInt256         │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt8

入力値をInt8型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt8(x)
```

**パラメータ**

- `x`: Int8としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int8として再解釈された値 `x`。[Int8](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toUInt8(257) AS x,
    toTypeName(x),
    reinterpretAsInt8(x) AS res,
    toTypeName(res);
```

結果:

```response
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ UInt8         │   1 │ Int8            │
└───┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt16

入力値をInt16型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt16(x)
```

**パラメータ**

- `x`: Int16としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int16として再解釈された値 `x`。[Int16](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toInt8(257) AS x,
    toTypeName(x),
    reinterpretAsInt16(x) AS res,
    toTypeName(res);
```

結果:

```response
┌─x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 1 │ Int8          │   1 │ Int16           │
└───┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt32

入力値をInt32型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt32(x)
```

**パラメータ**

- `x`: Int32としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int32として再解釈された値 `x`。[Int32](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toInt16(257) AS x,
    toTypeName(x),
    reinterpretAsInt32(x) AS res,
    toTypeName(res);
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int16         │ 257 │ Int32           │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt64

入力値をInt64型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt64(x)
```

**パラメータ**

- `x`: Int64としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int64として再解釈された値 `x`。[Int64](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toInt32(257) AS x,
    toTypeName(x),
    reinterpretAsInt64(x) AS res,
    toTypeName(res);
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int32         │ 257 │ Int64           │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt128

入力値をInt128型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt128(x)
```

**パラメータ**

- `x`: Int128としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int128として再解釈された値 `x`。[Int128](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toInt64(257) AS x,
    toTypeName(x),
    reinterpretAsInt128(x) AS res,
    toTypeName(res);
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int64         │ 257 │ Int128          │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsInt256

入力値をInt256型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsInt256(x)
```

**パラメータ**

- `x`: Int256としてバイト再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Int256として再解釈された値 `x`。[Int256](../data-types/int-uint.md/#int-ranges)。

**例**

クエリ:

```sql
SELECT
    toInt128(257) AS x,
    toTypeName(x),
    reinterpretAsInt256(x) AS res,
    toTypeName(res);
```

結果:

```response
┌───x─┬─toTypeName(x)─┬─res─┬─toTypeName(res)─┐
│ 257 │ Int128        │ 257 │ Int256          │
└─────┴───────────────┴─────┴─────────────────┘
```

## reinterpretAsFloat32

入力値をFloat32型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsFloat32(x)
```

**パラメータ**

- `x`: Float32として再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Float32として再解釈された値 `x`。[Float32](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT reinterpretAsUInt32(toFloat32(0.2)) as x, reinterpretAsFloat32(x);
```

結果:

```response
┌──────────x─┬─reinterpretAsFloat32(x)─┐
│ 1045220557 │                     0.2 │
└────────────┴─────────────────────────┘
```

## reinterpretAsFloat64

入力値をFloat64型の値として扱うことでバイト再解釈を行います。[`CAST`](#cast)とは異なり、元の値を保持しようとはしません。ターゲット型が入力型を表現できない場合、出力は無意味になります。

**構文**

```sql
reinterpretAsFloat64(x)
```

**パラメータ**

- `x`: Float64として再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- Float64として再解釈された値 `x`。[Float64](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT reinterpretAsUInt64(toFloat64(0.2)) as x, reinterpretAsFloat64(x);
```

結果:

```response
┌───────────────────x─┬─reinterpretAsFloat64(x)─┐
│ 4596373779694328218 │                     0.2 │
└─────────────────────┴─────────────────────────┘
```

## reinterpretAsDate

文字列、固定文字列または数値を受け取り、バイトをホスト順（リトルエンディアン）での数値として解釈します。解釈された数値からUnixエポックの始まりからの日数として日付を返します。

**構文**

```sql
reinterpretAsDate(x)
```

**パラメータ**

- `x`: Unixエポックの始まりからの日数。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- 日付。[Date](../data-types/date.md)。

**実装の詳細**

:::note
指定された文字列が十分な長さでない場合、必要な数のヌルバイトでパディングされたかのように動作します。文字列が必要な長さを超えている場合、余分なバイトは無視されます。
:::

**例**

クエリ:

```sql
SELECT reinterpretAsDate(65), reinterpretAsDate('A');
```

結果:

```response
┌─reinterpretAsDate(65)─┬─reinterpretAsDate('A')─┐
│            1970-03-07 │             1970-03-07 │
└───────────────────────┴────────────────────────┘
```

## reinterpretAsDateTime

これらの関数は文字列を受け取り、文字列の先頭に配置されたバイトをホスト順（リトルエンディアン）での数値として解釈します。Unixエポックの始まりからの秒数として解釈された日時を返します。

**構文**

```sql
reinterpretAsDateTime(x)
```

**パラメータ**

- `x`: Unixエポックの始まりからの秒数。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md), [UUID](../data-types/uuid.md), [String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- 日付と時刻。[DateTime](../data-types/datetime.md)。

**実装の詳細**

:::note
指定された文字列が十分な長さでない場合、必要な数のヌルバイトでパディングされたかのように動作します。文字列が必要な長さを超えている場合、余分なバイトは無視されます。
:::

**例**

クエリ:

```sql
SELECT reinterpretAsDateTime(65), reinterpretAsDateTime('A');
```

結果:

```response
┌─reinterpretAsDateTime(65)─┬─reinterpretAsDateTime('A')─┐
│       1970-01-01 01:01:05 │        1970-01-01 01:01:05 │
└───────────────────────────┴────────────────────────────┘
```

## reinterpretAsString

この関数は数値、日付または時刻のある日付を受け取り、ホスト順（リトルエンディアン）で対応する値を表すバイトを含む文字列を返します。末尾のヌルバイトは削除されます。たとえば、UInt32型の値255は1バイト長の文字列です。

**構文**

```sql
reinterpretAsString(x)
```

**パラメータ**

- `x`: 文字列として再解釈する値。[(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md)。

**返される値**

- `x`を表すバイトを含む文字列。[String](../data-types/fixedstring.md)。

**例**

クエリ:

```sql
SELECT
    reinterpretAsString(toDateTime('1970-01-01 01:01:05')),
    reinterpretAsString(toDate('1970-03-07'));
```

結果:

```response
┌─reinterpretAsString(toDateTime('1970-01-01 01:01:05'))─┬─reinterpretAsString(toDate('1970-03-07'))─┐
│ A                                                      │ A                                         │
└────────────────────────────────────────────────────────┴───────────────────────────────────────────┘
```

## reinterpretAsFixedString

この関数は数値、日付または時刻のある日付を受け取り、ホスト順（リトルエンディアン）で対応する値を表すバイトを含むFixedStringを返します。末尾のヌルバイトは削除されます。たとえば、UInt32型の値255は1バイト長のFixedStringです。

**構文**

```sql
reinterpretAsFixedString(x)
```

**パラメータ**

- `x`: 文字列として再解釈する値。 [(U)Int*](../data-types/int-uint.md), [Float](../data-types/float.md), [Date](../data-types/date.md), [DateTime](../data-types/datetime.md)。

**返される値**

- `x`を表すバイトを含む固定長文字列。[FixedString](../data-types/fixedstring.md)。

**例**

クエリ:

```sql
SELECT
    reinterpretAsFixedString(toDateTime('1970-01-01 01:01:05')),
    reinterpretAsFixedString(toDate('1970-03-07'));
```

結果:

```response
┌─reinterpretAsFixedString(toDateTime('1970-01-01 01:01:05'))─┬─reinterpretAsFixedString(toDate('1970-03-07'))─┐
│ A                                                           │ A                                              │
└─────────────────────────────────────────────────────────────┴────────────────────────────────────────────────┘
```

## reinterpretAsUUID

:::note
ここに記載されているUUID関数に加えて、専用の[UUID関数ドキュメント](../functions/uuid-functions.md)があります。
:::

16バイトの文字列を受け取り、対象の値をネットワークバイトオーダー（ビッグエンディアン）で表すバイトを含むUUIDを返します。文字列が十分な長さでない場合、必要な数のヌルバイトでパディングしたかのように機能します。文字列が16バイトを超える場合、余分なバイトは無視されます。

**構文**

``` sql
reinterpretAsUUID(fixed_string)
```

**引数**

- `fixed_string` — ビッグエンディアンのバイト列。[FixedString](../data-types/fixedstring.md/#fixedstring)。

**返される値**

- UUID型の値。[UUID](../data-types/uuid.md/#uuid-data-type)。

**例**

文字列をUUIDに変換。

クエリ:

``` sql
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')));
```

結果:

```response
┌─reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))─┐
│                                  08090a0b-0c0d-0e0f-0001-020304050607 │
└───────────────────────────────────────────────────────────────────────┘
```

文字列とUUID間で変換。

クエリ:

``` sql
WITH
    generateUUIDv4() AS uuid,
    identity(lower(hex(reverse(reinterpretAsString(uuid))))) AS str,
    reinterpretAsUUID(reverse(unhex(str))) AS uuid2
SELECT uuid = uuid2;
```

結果:

```response
┌─equals(uuid, uuid2)─┐
│                   1 │
└─────────────────────┘
```

## reinterpret

`x`の値に対して同じソースのインメモリバイト列を使用し、それを目的の型に再解釈します。

**構文**

``` sql
reinterpret(x, type)
```

**引数**

- `x` — 任意の型。
- `type` — 目的の型。[String](../data-types/string.md)。

**返される値**

- 目的の型の値。

**例**

クエリ:
```sql
SELECT reinterpret(toInt8(-1), 'UInt8') as int_to_uint,
    reinterpret(toInt8(1), 'Float32') as int_to_float,
    reinterpret('1', 'UInt32') as string_to_int;
```

結果:

```
┌─int_to_uint─┬─int_to_float─┬─string_to_int─┐
│         255 │        1e-45 │            49 │
└─────────────┴──────────────┴───────────────┘
```

## CAST

入力値を指定されたデータ型に変換します。[reinterpret](#reinterpret)関数とは異なり、`CAST`は新しいデータ型を使用して同じ値を表現しようとします。変換ができない場合、例外が発生します。
複数の構文バリエーションがサポートされています。

**構文**

``` sql
CAST(x, T)
CAST(x AS t)
x::t
```

**引数**

- `x` — 変換する値。任意の型の可能性があります。
- `T` — ターゲットデータ型の名前。[String](../data-types/string.md)。
- `t` — ターゲットデータ型。

**返される値**

- 変換された値。

:::note
入力値がターゲット型の範囲に収まらない場合、結果はオーバーフローします。例えば、`CAST(-1, 'UInt8')`は`255`を返します。
:::

**例**

クエリ:

```sql
SELECT
    CAST(toInt8(-1), 'UInt8') AS cast_int_to_uint,
    CAST(1.5 AS Decimal(3,2)) AS cast_float_to_decimal,
    '1'::Int32 AS cast_string_to_int;
```

結果:

```
┌─cast_int_to_uint─┬─cast_float_to_decimal─┬─cast_string_to_int─┐
│              255 │                  1.50 │                  1 │
└──────────────────┴───────────────────────┴────────────────────┘
```

クエリ:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string;
```

結果:

```response
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

[FixedString (N)](../data-types/fixedstring.md)への変換は、[String](../data-types/string.md)または[FixedString](../data-types/fixedstring.md)型の引数に対してのみ動作します。

[Nullable](../data-types/nullable.md)型への変換やその逆もサポートされています。

**例**

クエリ:

``` sql
SELECT toTypeName(x) FROM t_null;
```

結果:

```response
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

クエリ:

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null;
```

結果:

```response
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

**関連項目**

- [cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable) 設定

## accurateCast(x, T)

`x`を`T`データ型に変換します。

[cast](#cast)との違いは、`accurateCast`は型`T`の範囲内に収まらない数値型のキャストを許可しないことです。たとえば、`accurateCast(-1, 'UInt8')`は例外をスローします。

**例**

クエリ:

``` sql
SELECT cast(-1, 'UInt8') as uint8;
```

結果:

```response
┌─uint8─┐
│   255 │
└───────┘
```

クエリ:

```sql
SELECT accurateCast(-1, 'UInt8') as uint8;
```

結果:

```response
Code: 70. DB::Exception: Received from localhost:9000. DB::Exception: Value in column Int8 cannot be safely converted into type UInt8: While processing accurateCast(-1, 'UInt8') AS uint8.
```

## accurateCastOrNull(x, T)

入力値 `x` を指定されたデータ型 `T` に変換します。常に[Nullable](../data-types/nullable.md)型を返し、キャストされた値がターゲット型で表現できない場合は[NULL](../syntax.md/#null-literal)を返します。

**構文**

```sql
accurateCastOrNull(x, T)
```

**引数**

- `x` — 入力値。
- `T` — 返されるデータ型の名前

**返される値**

- 指定されたデータ型 `T` に変換された値。

**例**

クエリ:

``` sql
SELECT toTypeName(accurateCastOrNull(5, 'UInt8'));
```

結果:

```response
┌─toTypeName(accurateCastOrNull(5, 'UInt8'))─┐
│ Nullable(UInt8)                            │
└────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT
    accurateCastOrNull(-1, 'UInt8') as uint8,
    accurateCastOrNull(128, 'Int8') as int8,
    accurateCastOrNull('Test', 'FixedString(2)') as fixed_string;
```

結果:

```response
┌─uint8─┬─int8─┬─fixed_string─┐
│  ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │
└───────┴──────┴──────────────┘
```

## accurateCastOrDefault(x, T[, default_value])

入力値 `x` を指定されたデータ型 `T` に変換します。キャストされた値がターゲット型で表現できない場合、デフォルト型の値または指定された`default_value`を返します。

**構文**

```sql
accurateCastOrDefault(x, T)
```

**引数**

- `x` — 入力値。
- `T` — 返されるデータ型の名前。
- `default_value` — 返されるデータ型のデフォルト値。

**返される値**

- 指定されたデータ型 `T` に変換された値。

**例**

クエリ:

``` sql
SELECT toTypeName(accurateCastOrDefault(5, 'UInt8'));
```

結果:

```response
┌─toTypeName(accurateCastOrDefault(5, 'UInt8'))─┐
│ UInt8                                         │
└───────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT
    accurateCastOrDefault(-1, 'UInt8') as uint8,
    accurateCastOrDefault(-1, 'UInt8', 5) as uint8_default,
    accurateCastOrDefault(128, 'Int8') as int8,
    accurateCastOrDefault(128, 'Int8', 5) as int8_default,
    accurateCastOrDefault('Test', 'FixedString(2)') as fixed_string,
    accurateCastOrDefault('Test', 'FixedString(2)', 'Te') as fixed_string_default;
```

結果:

```response
┌─uint8─┬─uint8_default─┬─int8─┬─int8_default─┬─fixed_string─┬─fixed_string_default─┐
│     0 │             5 │    0 │            5 │              │ Te                   │
└───────┴───────────────┴──────┴──────────────┴──────────────┴──────────────────────┘
```

## toIntervalYear

`n`年のデータ型[IntervalYear](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalYear(n)
```

**引数**

- `n` — 年の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`年のインターバル。[IntervalYear](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalYear(1) AS interval_to_year
SELECT date + interval_to_year AS result
```

結果:

```response
┌─────result─┐
│ 2025-06-15 │
└────────────┘
```

## toIntervalQuarter

`n`四半期のデータ型[IntervalQuarter](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalQuarter(n)
```

**引数**

- `n` — 四半期の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`四半期のインターバル。[IntervalQuarter](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalQuarter(1) AS interval_to_quarter
SELECT date + interval_to_quarter AS result
```

結果:

```response
┌─────result─┐
│ 2024-09-15 │
└────────────┘
```

## toIntervalMonth

`n`ヶ月のデータ型[IntervalMonth](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalMonth(n)
```

**引数**

- `n` — 月数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`ヶ月のインターバル。[IntervalMonth](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalMonth(1) AS interval_to_month
SELECT date + interval_to_month AS result
```

結果:

```response
┌─────result─┐
│ 2024-07-15 │
└────────────┘
```

## toIntervalWeek

`n`週のデータ型[IntervalWeek](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalWeek(n)
```

**引数**

- `n` — 週の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`週のインターバル。[IntervalWeek](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalWeek(1) AS interval_to_week
SELECT date + interval_to_week AS result
```

結果:

```response
┌─────result─┐
│ 2024-06-22 │
└────────────┘
```

## toIntervalDay

`n`日のデータ型[IntervalDay](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalDay(n)
```

**引数**

- `n` — 日数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`日のインターバル。[IntervalDay](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalDay(5) AS interval_to_days
SELECT date + interval_to_days AS result
```

結果:

```response
┌─────result─┐
│ 2024-06-20 │
└────────────┘
```

## toIntervalHour

`n`時間のデータ型[IntervalHour](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalHour(n)
```

**引数**

- `n` — 時間の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`時間のインターバル。[IntervalHour](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalHour(12) AS interval_to_hours
SELECT date + interval_to_hours AS result
```

結果:

```response
┌──────────────result─┐
│ 2024-06-15 12:00:00 │
└─────────────────────┘
```

## toIntervalMinute

`n`分のデータ型[IntervalMinute](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalMinute(n)
```

**引数**

- `n` — 分の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`分のインターバル。[IntervalMinute](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalMinute(12) AS interval_to_minutes
SELECT date + interval_to_minutes AS result
```

結果:

```response
┌──────────────result─┐
│ 2024-06-15 00:12:00 │
└─────────────────────┘
```

## toIntervalSecond

`n`秒のデータ型[IntervalSecond](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalSecond(n)
```

**引数**

- `n` — 秒の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`秒のインターバル。[IntervalSecond](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDate('2024-06-15') AS date,
    toIntervalSecond(30) AS interval_to_seconds
SELECT date + interval_to_seconds AS result
```

結果:

```response
┌──────────────result─┐
│ 2024-06-15 00:00:30 │
└─────────────────────┘
```

## toIntervalMillisecond

`n`ミリ秒のデータ型[IntervalMillisecond](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalMillisecond(n)
```

**引数**

- `n` — ミリ秒の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`ミリ秒のインターバル。[IntervalMilliseconds](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDateTime('2024-06-15') AS date,
    toIntervalMillisecond(30) AS interval_to_milliseconds
SELECT date + interval_to_milliseconds AS result
```

結果:

```response
┌──────────────────result─┐
│ 2024-06-15 00:00:00.030 │
└─────────────────────────┘
```

## toIntervalMicrosecond

`n`マイクロ秒のデータ型[IntervalMicrosecond](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalMicrosecond(n)
```

**引数**

- `n` — マイクロ秒の数。整数またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**返される値**

- `n`マイクロ秒のインターバル。[IntervalMicrosecond](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDateTime('2024-06-15') AS date,
    toIntervalMicrosecond(30) AS interval_to_microseconds
SELECT date + interval_to_microseconds AS result
```

結果:

```response
┌─────────────────────result─┐
│ 2024-06-15 00:00:00.000030 │
└────────────────────────────┘
```

## toIntervalNanosecond

`n`ナノ秒のデータ型[IntervalNanosecond](../data-types/special-data-types/interval.md)を返します。

**構文**

``` sql
toIntervalNanosecond(n)
```

**引数**
- `n` — ナノ秒の数。整数値またはその文字列表現、および浮動小数点数。[(U)Int*](../data-types/int-uint.md)/[Float*](../data-types/float.md)/[String](../data-types/string.md)。

**戻り値**

- `n` ナノ秒の間隔。[IntervalNanosecond](../data-types/special-data-types/interval.md)。

**例**

クエリ:

``` sql
WITH
    toDateTime('2024-06-15') AS date,
    toIntervalNanosecond(30) AS interval_to_nanoseconds
SELECT date + interval_to_nanoseconds AS result
```

結果:

```response
┌────────────────────────result─┐
│ 2024-06-15 00:00:00.000000030 │
└───────────────────────────────┘
```

## parseDateTime

[MySQL 形式の文字列](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format)に従って[String](../data-types/string.md)を[DateTime](../data-types/datetime.md)に変換します。

この関数は[formatDateTime](../functions/date-time-functions.md#date_time_functions-formatDateTime)関数の反対の操作を行います。

**構文**

``` sql
parseDateTime(str[, format[, timezone]])
```

**引数**

- `str` — 解析される文字列
- `format` — フォーマット文字列。省略可能。指定しない場合 `%Y-%m-%d %H:%i:%s`。
- `timezone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。省略可能。

**戻り値**

MySQL 形式のフォーマット文字列に従って入力文字列から解析された DateTime 値を返します。

**対応フォーマット指定子**

[formatDateTime](../functions/date-time-functions.md#date_time_functions-formatDateTime)に記載されたすべてのフォーマット指定子に対応。ただし以下を除く:
- %Q: 四半期 (1-4)

**例**

``` sql
SELECT parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s')

┌─parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s')─┐
│                                       2021-01-04 23:00:00 │
└───────────────────────────────────────────────────────────┘
```

別名: `TO_TIMESTAMP`.

## parseDateTimeOrZero

[parseDateTime](#parsedatetime)と同様ですが、処理できない日付フォーマットに遭遇した場合、ゼロ日付を返します。

## parseDateTimeOrNull

[parseDateTime](#parsedatetime)と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

別名: `str_to_date`.

## parseDateTimeInJodaSyntax

[parseDateTime](#parsedatetime)に類似していますが、フォーマット文字列がMySQL構文ではなく[Joda](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)である点が異なります。

この関数は[formatDateTimeInJodaSyntax](../functions/date-time-functions.md#date_time_functions-formatDateTimeInJodaSyntax)関数の反対の操作を行います。

**構文**

``` sql
parseDateTimeInJodaSyntax(str[, format[, timezone]])
```

**引数**

- `str` — 解析される文字列
- `format` — フォーマット文字列。省略可能。指定しない場合、`yyyy-MM-dd HH:mm:ss`。
- `timezone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。省略可能。

**戻り値**

Joda スタイルのフォーマットに従って入力文字列から解析された DateTime 値を返します。

**対応フォーマット指定子**

[formatDateTimeInJoda](../functions/date-time-functions.md#date_time_functions-formatDateTime)に記載されているすべてのフォーマット指定子に対応。ただし以下を除く:
- S: 秒の小数部分
- z: タイムゾーン
- Z: タイムゾーンオフセット/ID

**例**

``` sql
SELECT parseDateTimeInJodaSyntax('2023-02-24 14:53:31', 'yyyy-MM-dd HH:mm:ss', 'Europe/Minsk')

┌─parseDateTimeInJodaSyntax('2023-02-24 14:53:31', 'yyyy-MM-dd HH:mm:ss', 'Europe/Minsk')─┐
│                                                                     2023-02-24 14:53:31 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## parseDateTimeInJodaSyntaxOrZero

[parseDateTimeInJodaSyntax](#parsedatetimeinjodasyntax)と同様ですが、処理できない日付フォーマットに遭遇した場合、ゼロ日付を返します。

## parseDateTimeInJodaSyntaxOrNull

[parseDateTimeInJodaSyntax](#parsedatetimeinjodasyntax)と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTime64InJodaSyntax

[parseDateTimeInJodaSyntax](#parsedatetimeinjodasyntax)に類似していますが、型[DateTime64](../data-types/datetime64.md)の値を返す点が異なります。

## parseDateTime64InJodaSyntaxOrZero

[parseDateTime64InJodaSyntax](#parsedatetime64injodasyntax)と同様ですが、処理できない日付フォーマットに遭遇した場合、ゼロ日付を返します。

## parseDateTime64InJodaSyntaxOrNull

[parseDateTime64InJodaSyntax](#parsedatetime64injodasyntax)と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTimeBestEffort
## parseDateTime32BestEffort

[String](../data-types/string.md)の表現にある日付と時刻を[DateTime](../data-types/datetime.md/#data_type-datetime)データ型に変換します。

この関数は、[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)、[RFC 1123 - 5.2.14 RFC-822 日付と時刻の仕様](https://tools.ietf.org/html/rfc1123#page-55)、ClickHouse、およびその他のいくつかの日付と時刻のフォーマットを解析します。

**構文**

``` sql
parseDateTimeBestEffort(time_string [, time_zone])
```

**引数**

- `time_string` — 変換する日時を含む文字列。[String](../data-types/string.md)。
- `time_zone` — タイムゾーン。関数は `time_string` をタイムゾーンに従って解析します。[String](../data-types/string.md)。

**対応する非標準フォーマット**

- 9..10 桁の[UNIXタイムスタンプ](https://en.wikipedia.org/wiki/Unix_time)を含む文字列。
- 日時コンポーネントを含む文字列: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, など。
- 日付のみで時間コンポーネントを含まない文字列: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` など。
- 日と時間を含む文字列: `DD`, `DD hh`, `DD hh:mm`。この場合、`MM` は `01` に置き換えられます。
- 日付と時刻に加えてタイムゾーンオフセット情報を含む文字列: `YYYY-MM-DD hh:mm:ss ±h:mm`, など。例: `2020-12-12 17:36:00 -5:00`.
- [syslog タイムスタンプ](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2): `Mmm dd hh:mm:ss`。例: `Jun  9 14:20:32`.

セパレータのあるすべてのフォーマットについて、関数は月の名前を月のフルネームまたは月の名前の最初の三文字で表現したものを解析します。例: `24/DEC/18`, `24-Dec-18`, `01-September-2018`。
年が指定されていない場合は、現在の年と見なされます。結果の DateTime が未来の時間であった場合（たとえ現在時刻より1秒後であっても）、前年が置き換えられます。

**戻り値**

- `time_string` を変換した [DateTime](../data-types/datetime.md) データ型。

**例**

クエリ:

``` sql
SELECT parseDateTimeBestEffort('23/10/2020 12:12:57')
AS parseDateTimeBestEffort;
```

結果:

```response
┌─parseDateTimeBestEffort─┐
│     2020-10-23 12:12:57 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Asia/Istanbul')
AS parseDateTimeBestEffort;
```

結果:

```response
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort;
```

結果:

```response
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('2018-10-23 10:12:12')
AS parseDateTimeBestEffort;
```

結果:

```response
┌─parseDateTimeBestEffort─┐
│     2018-10-23 10:12:12 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT toYear(now()) as year, parseDateTimeBestEffort('10 20:19');
```

結果:

```response
┌─year─┬─parseDateTimeBestEffort('10 20:19')─┐
│ 2023 │                 2023-01-10 20:19:00 │
└──────┴─────────────────────────────────────┘
```

クエリ:

``` sql
WITH
    now() AS ts_now,
    formatDateTime(ts_around, '%b %e %T') AS syslog_arg
SELECT
    ts_now,
    syslog_arg,
    parseDateTimeBestEffort(syslog_arg)
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around);
```

結果:

```response
┌──────────────ts_now─┬─syslog_arg──────┬─parseDateTimeBestEffort(syslog_arg)─┐
│ 2023-06-30 23:59:30 │ Jun 30 23:59:00 │                 2023-06-30 23:59:00 │
│ 2023-06-30 23:59:30 │ Jul  1 00:00:00 │                 2022-07-01 00:00:00 │
└─────────────────────┴─────────────────┴─────────────────────────────────────┘
```

**関連事項**

- [RFC 1123](https://datatracker.ietf.org/doc/html/rfc1123)
- [toDate](#todate)
- [toDateTime](#todatetime)
- [ISO 8601 announcement by @xkcd](https://xkcd.com/1179/)
- [RFC 3164](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2)

## parseDateTimeBestEffortUS

この関数は、[parseDateTimeBestEffort](#parsedatetimebesteffort)と同様に ISO 日付形式、たとえば`YYYY-MM-DD hh:mm:ss`、および年月日が明確に抽出可能な他の日付形式に対して動作します。しかし、年月日があいまいで抽出できない形式（例: `MM/DD/YYYY`, `MM-DD-YYYY`, `MM-DD-YY`）では、`DD/MM/YYYY`, `DD-MM-YYYY`, `DD-MM-YY` の代わりにUS日付形式を優先します。例外として、月が12より大きく、31以下である場合、この関数は[parseDateTimeBestEffort](#parsedatetimebesteffort)の動作にフォールバックします。例えば、`15/08/2020` は `2020-08-15` として解釈されます。

## parseDateTimeBestEffortOrNull
## parseDateTime32BestEffortOrNull

[parseDateTimeBestEffort](#parsedatetimebesteffort)と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTimeBestEffortOrZero
## parseDateTime32BestEffortOrZero

[parseDateTimeBestEffort](#parsedatetimebesteffort)と同様ですが、処理できない日付フォーマットに遭遇した場合にゼロ日付またはゼロ日時を返します。

## parseDateTimeBestEffortUSOrNull

[parseDateTimeBestEffortUS](#parsedatetimebesteffortus) と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTimeBestEffortUSOrZero

[parseDateTimeBestEffortUS](#parsedatetimebesteffortus) と同様ですが、処理できない日付フォーマットに遭遇した場合、ゼロ日付 (`1970-01-01`) またはゼロ日付と時間 (`1970-01-01 00:00:00`) を返します。

## parseDateTime64BestEffort

[parseDateTimeBestEffort](#parsedatetimebesteffort) 関数と同様ですが、ミリ秒およびマイクロ秒も解析し、[DateTime](../functions/type-conversion-functions.md/#data_type-datetime) データ型を返します。

**構文**

``` sql
parseDateTime64BestEffort(time_string [, precision [, time_zone]])
```

**引数**

- `time_string` — 変換する日付または日時を含む文字列。[String](../data-types/string.md)。
- `precision` — 必要な精度。`3` — ミリ秒用、`6` — マイクロ秒用。デフォルトは`3`。省略可能。[UInt8](../data-types/int-uint.md)。
- `time_zone` — [タイムゾーン](/docs/ja/operations/server-configuration-parameters/settings.md#timezone)。関数は `time_string` をタイムゾーンに従って解析します。省略可能。[String](../data-types/string.md)。

**戻り値**

- `time_string` を変換した [DateTime](../data-types/datetime.md) データ型。

**例**

クエリ:

```sql
SELECT parseDateTime64BestEffort('2021-01-01') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',6) AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',3,'Asia/Istanbul') AS a, toTypeName(a) AS t
FORMAT PrettyCompactMonoBlock;
```

結果:

```
┌──────────────────────────a─┬─t──────────────────────────────┐
│ 2021-01-01 01:01:00.123000 │ DateTime64(3)                  │
│ 2021-01-01 00:00:00.000000 │ DateTime64(3)                  │
│ 2021-01-01 01:01:00.123460 │ DateTime64(6)                  │
│ 2020-12-31 22:01:00.123000 │ DateTime64(3, 'Asia/Istanbul') │
└────────────────────────────┴────────────────────────────────┘
```

## parseDateTime64BestEffortUS

[parseDateTime64BestEffort](#parsedatetime64besteffort)と同様ですが、この関数はあいまいな場合にUS日時形式 (`MM/DD/YYYY` など) を優先します。

## parseDateTime64BestEffortOrNull

[parseDateTime64BestEffort](#parsedatetime64besteffort)と同様ですが、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTime64BestEffortOrZero

[parseDateTime64BestEffort](#parsedatetime64besteffort)と同様ですが、処理できない日付フォーマットに遭遇した場合にゼロ日付またはゼロ日時を返します。

## parseDateTime64BestEffortUSOrNull

[parseDateTime64BestEffort](#parsedatetime64besteffort)と同様ですが、あいまいな場合にUS日時形式を優先し、処理できない日付フォーマットに遭遇した場合、`NULL`を返します。

## parseDateTime64BestEffortUSOrZero

[parseDateTime64BestEffort](#parsedatetime64besteffort)と同様ですが、あいまいな場合にUS日時形式を優先し、処理できない日付フォーマットに遭遇した場合にゼロ日付またはゼロ日時を返します。

## toLowCardinality

入力パラメータを同じデータ型の[LowCardinality](../data-types/lowcardinality.md)バージョンに変換します。

`LowCardinality` データ型からデータを変換するには [CAST](#cast) 関数を使用します。例: `CAST(x as String)`。

**構文**

```sql
toLowCardinality(expr)
```

**引数**

- `expr` — 一つの [対応データ型](../data-types/index.md/#data_types) を結果とする[式](../syntax.md/#syntax-expressions)。

**戻り値**

- `expr` の結果。[LowCardinality](../data-types/lowcardinality.md) の `expr` の型の結果。

**例**

クエリ:

```sql
SELECT toLowCardinality('1');
```

結果:

```response
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## toUnixTimestamp64Milli

`DateTime64` を固定のミリ秒精度で `Int64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
出力値は `DateTime64` のタイムゾーンではなく、UTCのタイムスタンプです。
:::

**構文**

```sql
toUnixTimestamp64Milli(value)
```

**引数**

- `value` — 任意の精度の DateTime64 値。[DateTime64](../data-types/datetime64.md)。

**戻り値**

- `Int64` データ型に変換された `value`。[Int64](../data-types/int-uint.md).

**例**

クエリ:

```sql
WITH toDateTime64('2009-02-13 23:31:31.011', 3, 'UTC') AS dt64
SELECT toUnixTimestamp64Milli(dt64);
```

結果:

```response
┌─toUnixTimestamp64Milli(dt64)─┐
│                1234567891011 │
└──────────────────────────────┘
```

## toUnixTimestamp64Micro

`DateTime64` を固定のマイクロ秒精度で `Int64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
出力値は `DateTime64` のタイムゾーンではなく、UTCのタイムスタンプです。
:::

**構文**

```sql
toUnixTimestamp64Micro(value)
```

**引数**

- `value` — 任意の精度の DateTime64 値。[DateTime64](../data-types/datetime64.md)。

**戻り値**

- `Int64` データ型に変換された `value`。[Int64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
WITH toDateTime64('1970-01-15 06:56:07.891011', 6, 'UTC') AS dt64
SELECT toUnixTimestamp64Micro(dt64);
```

結果:

```response
┌─toUnixTimestamp64Micro(dt64)─┐
│                1234567891011 │
└──────────────────────────────┘
```

## toUnixTimestamp64Nano

`DateTime64` を固定のナノ秒精度で `Int64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
出力値は `DateTime64` のタイムゾーンではなく、UTCのタイムスタンプです。
:::

**構文**

```sql
toUnixTimestamp64Nano(value)
```

**引数**

- `value` — 任意の精度の DateTime64 値。[DateTime64](../data-types/datetime64.md)。

**戻り値**

- `Int64` データ型に変換された `value`。[Int64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
WITH toDateTime64('1970-01-01 00:20:34.567891011', 9, 'UTC') AS dt64
SELECT toUnixTimestamp64Nano(dt64);
```

結果:

```response
┌─toUnixTimestamp64Nano(dt64)─┐
│               1234567891011 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli

`Int64` を固定のミリ秒精度と任意のタイムゾーンを持つ `DateTime64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
入力値は指定された（または暗黙の）タイムゾーンではなく、UTCのタイムスタンプとして扱われます。
:::

**構文**

``` sql
fromUnixTimestamp64Milli(value[, timezone])
```

**引数**

- `value` — 任意の精度の値。[Int64](../data-types/int-uint.md)。
- `timezone` — （オプション）結果のタイムゾーン名。[String](../data-types/string.md)。

**戻り値**

- `3` の精度を持つ DateTime64 に変換された `value`。[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT
    fromUnixTimestamp64Milli(i64, 'UTC') AS x,
    toTypeName(x);
```

結果:

```response
┌───────────────────────x─┬─toTypeName(x)────────┐
│ 2009-02-13 23:31:31.011 │ DateTime64(3, 'UTC') │
└─────────────────────────┴──────────────────────┘
```

## fromUnixTimestamp64Micro

`Int64` を固定のマイクロ秒精度と任意のタイムゾーンを持つ `DateTime64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
入力値は指定された（または暗黙の）タイムゾーンではなく、UTCのタイムスタンプとして扱われます。
:::

**構文**

``` sql
fromUnixTimestamp64Micro(value[, timezone])
```

**引数**

- `value` — 任意の精度の値。[Int64](../data-types/int-uint.md)。
- `timezone` — （オプション）結果のタイムゾーン名。[String](../data-types/string.md)。

**戻り値**

- `6` の精度を持つ DateTime64 に変換された `value`。[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT
    fromUnixTimestamp64Micro(i64, 'UTC') AS x,
    toTypeName(x);
```

結果:

```response
┌──────────────────────────x─┬─toTypeName(x)────────┐
│ 1970-01-15 06:56:07.891011 │ DateTime64(6, 'UTC') │
└────────────────────────────┴──────────────────────┘
```

## fromUnixTimestamp64Nano

`Int64` を固定のナノ秒精度と任意のタイムゾーンを持つ `DateTime64` 値に変換します。入力値はその精度に応じて適切にスケーリングされます。

:::note
入力値は指定された（または暗黙の）タイムゾーンではなく、UTCのタイムスタンプとして扱われます。
:::

**構文**

``` sql
fromUnixTimestamp64Nano(value[, timezone])
```

**引数**

- `value` — 任意の精度の値。[Int64](../data-types/int-uint.md)。
- `timezone` — （オプション）結果のタイムゾーン名。[String](../data-types/string.md)。

**戻り値**

- `9` の精度を持つ DateTime64 に変換された `value`。[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT
    fromUnixTimestamp64Nano(i64, 'UTC') AS x,
    toTypeName(x);
```

結果:

```response
┌─────────────────────────────x─┬─toTypeName(x)────────┐
│ 1970-01-01 00:20:34.567891011 │ DateTime64(9, 'UTC') │
└───────────────────────────────┴──────────────────────┘
```

## formatRow

任意の式を指定されたフォーマットを使って文字列に変換します。

**構文**

``` sql
formatRow(format, x, y, ...)
```

**引数**

- `format` — テキストフォーマット。例: [CSV](/docs/ja/interfaces/formats.md/#csv), [TSV](/docs/ja/interfaces/formats.md/#tabseparated)。
- `x`,`y`, ... — 式。

**戻り値**

- フォーマットされた文字列。（テキストフォーマットの場合、通常は改行文字で終わります）。

**例**

クエリ:

``` sql
SELECT formatRow('CSV', number, 'good')
FROM numbers(3);
```

結果:

```response
┌─formatRow('CSV', number, 'good')─┐
│ 0,"good"
                         │
│ 1,"good"
                         │
│ 2,"good"
                         │
└──────────────────────────────────┘
```

**注意**: フォーマットに接尾辞/接頭辞が含まれる場合、それは各行に書き込まれます。

**例**

クエリ:

``` sql
SELECT formatRow('CustomSeparated', number, 'good')
FROM numbers(3)
SETTINGS format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>'
```

結果:

```response
┌─formatRow('CustomSeparated', number, 'good')─┐
│ <prefix>
0	good
<suffix>                   │
│ <prefix>
1	good
<suffix>                   │
│ <prefix>
2	good
<suffix>                   │
└──────────────────────────────────────────────┘
```

注: この関数でサポートされているのは、行ベースのフォーマットのみです。

## formatRowNoNewline

任意の式を指定されたフォーマットを使って文字列に変換します。formatRow との違いは、この関数が最後の `\n` を取り除くことです。

**構文**

``` sql
formatRowNoNewline(format, x, y, ...)
```

**引数**

- `format` — テキストフォーマット。例: [CSV](/docs/ja/interfaces/formats.md/#csv), [TSV](/docs/ja/interfaces/formats.md/#tabseparated)。
- `x`,`y`, ... — 式。

**戻り値**

- フォーマットされた文字列。

**例**

クエリ:

``` sql
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3);
```

結果:

```response
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
```
