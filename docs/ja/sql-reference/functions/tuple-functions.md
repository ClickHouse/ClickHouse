---
slug: /ja/sql-reference/functions/tuple-functions
sidebar_position: 180
sidebar_label: タプル
---

## tuple

複数のカラムをグループ化するための関数です。
タイプ T1, T2, ... を持つカラム C1, C2, ... に対して、それらの名前がユニークでアンコーテッド識別子として扱える場合、`Tuple(C1 T1, C2 T2, ...)` 型の名前付きタプルを返し、そうでない場合は `Tuple(T1, T2, ...)` を返します。この関数を実行する際のコストはありません。
通常、タプルはIN演算子の引数としての中間値、またはラムダ関数の形式パラメータリストを作成するために使用されます。タプルはテーブルに書き込むことはできません。

この関数は演算子 `(x, y, ...)` を実装しています。

**構文**

``` sql
tuple(x, y, ...)
```

## tupleElement

タプルからカラムを取得するための関数です。

第2引数が数値 `index` の場合、それは1から始まるカラムインデックスです。第2引数が文字列 `name` の場合、それは要素の名前を表します。また、第2引数が範囲外の場合や名前に対応する要素が存在しない場合にデフォルト値を返す第3のオプション引数を指定することができます。提供された場合、第2および第3の引数は定数でなければなりません。この関数を実行するコストはありません。

この関数はオペレータ `x.index` および `x.name` を実装しています。

**構文**

``` sql
tupleElement(tuple, index, [, default_value])
tupleElement(tuple, name, [, default_value])
```

## untuple

[tuple](../data-types/tuple.md#tuplet1-t2)要素を呼び出し位置での文法置換を行います。

結果のカラムの名前は実装固有であり、変更される可能性があります。`untuple`の後に特定のカラム名を仮定しないでください。

**構文**

``` sql
untuple(x)
```

`EXCEPT` 式を使用して、クエリの結果としてカラムをスキップすることができます。

**引数**

- `x` — `tuple` 関数、カラム、または要素のタプル。[Tuple](../data-types/tuple.md)。

**返される値**

- なし。

**例**

入力テーブル:

``` text
┌─key─┬─v1─┬─v2─┬─v3─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 20 │ 40 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 65 │ 70 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 30 │ 20 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 12 │  7 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 50 │ 70 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴────┴────┴───────────┘
```

`Tuple` 型カラムを `untuple` 関数のパラメータとして使用する例:

クエリ:

``` sql
SELECT untuple(v6) FROM kv;
```

結果:

``` text
┌─_ut_1─┬─_ut_2─┐
│    33 │ ab    │
│    44 │ cd    │
│    55 │ ef    │
│    66 │ gh    │
│    77 │ kl    │
└───────┴───────┘
```

`EXCEPT` 式の使用例:

クエリ:

``` sql
SELECT untuple((* EXCEPT (v2, v3),)) FROM kv;
```

結果:

``` text
┌─key─┬─v1─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴───────────┘
```

**参照**

- [Tuple](../data-types/tuple.md)

## tupleHammingDistance

2つの同サイズのタプル間の[ハミング距離](https://en.wikipedia.org/wiki/Hamming_distance)を返します。

**構文**

``` sql
tupleHammingDistance(tuple1, tuple2)
```

**引数**

- `tuple1` — 最初のタプル。[Tuple](../data-types/tuple.md)。
- `tuple2` — 2つ目のタプル。[Tuple](../data-types/tuple.md)。

タプルは同様の型の要素を持たなければなりません。

**返される値**

- ハミング距離。

:::note
結果タイプは[算術関数](../../sql-reference/functions/arithmetic-functions.md)の場合と同様に、入力タプルの要素数に基づいて計算されます。
:::

``` sql
SELECT
    toTypeName(tupleHammingDistance(tuple(0), tuple(0))) AS t1,
    toTypeName(tupleHammingDistance((0, 0), (0, 0))) AS t2,
    toTypeName(tupleHammingDistance((0, 0, 0), (0, 0, 0))) AS t3,
    toTypeName(tupleHammingDistance((0, 0, 0, 0), (0, 0, 0, 0))) AS t4,
    toTypeName(tupleHammingDistance((0, 0, 0, 0, 0), (0, 0, 0, 0, 0))) AS t5
```

``` text
┌─t1────┬─t2─────┬─t3─────┬─t4─────┬─t5─────┐
│ UInt8 │ UInt16 │ UInt32 │ UInt64 │ UInt64 │
└───────┴────────┴────────┴────────┴────────┘
```

**例**

クエリ:

``` sql
SELECT tupleHammingDistance((1, 2, 3), (3, 2, 1)) AS HammingDistance;
```

結果:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

[MinHash](../../sql-reference/functions/hash-functions.md#ngramminhash) 関数と共に半重複文字列の検出に使用できます:

``` sql
SELECT tupleHammingDistance(wordShingleMinHash(string), wordShingleMinHashCaseInsensitive(string)) AS HammingDistance
FROM (SELECT 'ClickHouse is a column-oriented database management system for online analytical processing of queries.' AS string);
```

結果:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

## tupleToNameValuePairs

名前付きタプルを(名前, 値)ペアの配列に変換します。`Tuple(a T, b T, ..., c T)` に対しては、`Array(Tuple(String, T), ...)` を返します。ここで `Strings` はタプルの名前付きフィールドを表し、`T` はそれらの名前に関連付けられた値です。タプル内のすべての値は同じタイプでなければなりません。

**構文**

``` sql
tupleToNameValuePairs(tuple)
```

**引数**

- `tuple` — 名前付きタプル。[Tuple](../data-types/tuple.md) 型で、任意のタイプの値を持つ。

**返される値**

- (名前, 値)ペアの配列。[Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), ...))。

**例**

クエリ:

``` sql
CREATE TABLE tupletest (col Tuple(user_ID UInt64, session_ID UInt64)) ENGINE = Memory;

INSERT INTO tupletest VALUES (tuple( 100, 2502)), (tuple(1,100));

SELECT tupleToNameValuePairs(col) FROM tupletest;
```

結果:

``` text
┌─tupleToNameValuePairs(col)────────────┐
│ [('user_ID',100),('session_ID',2502)] │
│ [('user_ID',1),('session_ID',100)]    │
└───────────────────────────────────────┘
```

この関数を使用してカラムを行に変換することが可能です:

``` sql
CREATE TABLE tupletest (col Tuple(CPU Float64, Memory Float64, Disk Float64)) ENGINE = Memory;

INSERT INTO tupletest VALUES(tuple(3.3, 5.5, 6.6));

SELECT arrayJoin(tupleToNameValuePairs(col)) FROM tupletest;
```

結果:

``` text
┌─arrayJoin(tupleToNameValuePairs(col))─┐
│ ('CPU',3.3)                           │
│ ('Memory',5.5)                        │
│ ('Disk',6.6)                          │
└───────────────────────────────────────┘
```

単純なタプルを関数に渡すと、ClickHouseは値のインデックスをその名前として使用します:

``` sql
SELECT tupleToNameValuePairs(tuple(3, 2, 1));
```

結果:

``` text
┌─tupleToNameValuePairs(tuple(3, 2, 1))─┐
│ [('1',3),('2',2),('3',1)]             │
└───────────────────────────────────────┘
```

## tupleNames

タプルをカラム名の配列に変換します。`Tuple(a T, b T, ...)` 形式のタプルに対しては、タプルの名前付きカラムを表す文字列の配列を返します。タプルの要素に明示的な名前がない場合、そのインデックスがカラム名として使用されます。

**構文**

``` sql
tupleNames(tuple)
```

**引数**

- `tuple` — 名前付きタプル。[Tuple](../../sql-reference/data-types/tuple.md) 型で、任意のタイプの値を持つ。

**返される値**

- 文字列の配列。

型: [Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md), ...))。

**例**

クエリ:

``` sql
CREATE TABLE tupletest (col Tuple(user_ID UInt64, session_ID UInt64)) ENGINE = Memory;

INSERT INTO tupletest VALUES (tuple(1, 2));

SELECT tupleNames(col) FROM tupletest;
```

結果:

``` text
┌─tupleNames(col)──────────┐
│ ['user_ID','session_ID'] │
└──────────────────────────┘
```

単純なタプルを関数に渡すと、ClickHouseはカラムのインデックスをその名前として使用します:

``` sql
SELECT tupleNames(tuple(3, 2, 1));
```

結果:

``` text
┌─tupleNames((3, 2, 1))─┐
│ ['1','2','3']         │
└───────────────────────┘
```

## tuplePlus

同サイズの2つのタプルの対応する値を足した値を計算します。

**構文**

```sql
tuplePlus(tuple1, tuple2)
```

別名: `vectorSum`.

**引数**

- `tuple1` — 最初のタプル。[Tuple](../data-types/tuple.md)。
- `tuple2` — 2つ目のタプル。[Tuple](../data-types/tuple.md)。

**返される値**

- 合計のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tuplePlus((1, 2), (2, 3));
```

結果:

```text
┌─tuplePlus((1, 2), (2, 3))─┐
│ (3,5)                     │
└───────────────────────────┘
```

## tupleMinus

同サイズの2つのタプルの対応する値から引き算した値を計算します。

**構文**

```sql
tupleMinus(tuple1, tuple2)
```

別名: `vectorDifference`.

**引数**

- `tuple1` — 最初のタプル。[Tuple](../data-types/tuple.md)。
- `tuple2` — 2つ目のタプル。[Tuple](../data-types/tuple.md)。

**返される値**

- 引き算の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleMinus((1, 2), (2, 3));
```

結果:

```text
┌─tupleMinus((1, 2), (2, 3))─┐
│ (-1,-1)                    │
└────────────────────────────┘
```

## tupleMultiply

同サイズの2つのタプルの対応する値の掛け算を計算します。

**構文**

```sql
tupleMultiply(tuple1, tuple2)
```

**引数**

- `tuple1` — 最初のタプル。[Tuple](../data-types/tuple.md)。
- `tuple2` — 2つ目のタプル。[Tuple](../data-types/tuple.md)。

**返される値**

- 掛け算の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleMultiply((1, 2), (2, 3));
```

結果:

```text
┌─tupleMultiply((1, 2), (2, 3))─┐
│ (2,6)                         │
└───────────────────────────────┘
```

## tupleDivide

同サイズの2つのタプルの対応する値の割り算を計算します。ゼロでの割り算は`inf`を返すことに注意してください。

**構文**

```sql
tupleDivide(tuple1, tuple2)
```

**引数**

- `tuple1` — 最初のタプル。[Tuple](../data-types/tuple.md)。
- `tuple2` — 2つ目のタプル。[Tuple](../data-types/tuple.md)。

**返される値**

- 割り算の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleDivide((1, 2), (2, 3));
```

結果:

```text
┌─tupleDivide((1, 2), (2, 3))─┐
│ (0.5,0.6666666666666666)    │
└─────────────────────────────┘
```

## tupleNegate

タプルの値の否定を計算します。

**構文**

```sql
tupleNegate(tuple)
```

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。

**返される値**

- 否定の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleNegate((1, 2));
```

結果:

```text
┌─tupleNegate((1, 2))─┐
│ (-1,-2)             │
└─────────────────────┘
```

## tupleMultiplyByNumber

全ての値を数値で掛けた結果のタプルを返します。

**構文**

```sql
tupleMultiplyByNumber(tuple, number)
```

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。
- `number` — 乗数。[Int/UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。

**返される値**

- 掛け算の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleMultiplyByNumber((1, 2), -2.1);
```

結果:

```text
┌─tupleMultiplyByNumber((1, 2), -2.1)─┐
│ (-2.1,-4.2)                         │
└─────────────────────────────────────┘
```

## tupleDivideByNumber

全ての値を数値で割った結果のタプルを返します。ゼロでの割り算は`inf`を返すことに注意してください。

**構文**

```sql
tupleDivideByNumber(tuple, number)
```

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。
- `number` — 除数。[Int/UInt](../data-types/int-uint.md), [Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。

**返される値**

- 割り算の結果のタプル。[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT tupleDivideByNumber((1, 2), 0.5);
```

結果:

```text
┌─tupleDivideByNumber((1, 2), 0.5)─┐
│ (2,4)                            │
└──────────────────────────────────┘
```

## tupleConcat

渡されたタプルを結合します。

``` sql
tupleConcat(tuples)
```

**引数**

- `tuples` – 任意の数の[Tuple](../data-types/tuple.md) 型の引数。

**例**

``` sql
SELECT tupleConcat((1, 2), (3, 4), (true, false)) AS res
```

``` text
┌─res──────────────────┐
│ (1,2,3,4,true,false) │
└──────────────────────┘
```

## tupleIntDiv

タプルの分子とタプルの除数の整数部の割り算を行い、商のタプルを返します。

**構文**

```sql
tupleIntDiv(tuple_num, tuple_div)
```

**パラメータ**

- `tuple_num`: 分子の値のタプル。[Tuple](../data-types/tuple) of numeric type。
- `tuple_div`: 除数の値のタプル。[Tuple](../data-types/tuple) of numeric type。

**返される値**

- `tuple_num` と `tuple_div` の商のタプル。[Tuple](../data-types/tuple) of integer values。

**実装の詳細**

- `tuple_num` または `tuple_div` のいずれかが非整数値を含む場合、結果は各非整数の分子または除数を最も近い整数に丸めて計算されます。
- ゼロ除算にはエラーがスローされます。

**例**

クエリ:

``` sql
SELECT tupleIntDiv((15, 10, 5), (5, 5, 5));
```

結果:

``` text
┌─tupleIntDiv((15, 10, 5), (5, 5, 5))─┐
│ (3,2,1)                             │
└─────────────────────────────────────┘
```

クエリ:

``` sql
SELECT tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5));
```

結果:

``` text
┌─tupleIntDiv((15, 10, 5), (5.5, 5.5, 5.5))─┐
│ (2,1,0)                                   │
└───────────────────────────────────────────┘
```

## tupleIntDivOrZero

[tupleIntDiv](#tupleintdiv)と同様に、タプルの分子とタプルの除数の整数部の割り算を行い、商のタプルを返します。0の除数に対してエラーをスローするのではなく、商として0を返します。

**構文**

```sql
tupleIntDivOrZero(tuple_num, tuple_div)
```

- `tuple_num`: 分子の値のタプル。 [Tuple](../data-types/tuple) of numeric type。
- `tuple_div`: 除数の値のタプル。 [Tuple](../data-types/tuple) of numeric type。

**返される値**

- `tuple_num` と `tuple_div` の商のタプル。 [Tuple](../data-types/tuple) of integer values。
- 除数が0の場合、商として0を返します。

**実装の詳細**

- `tuple_num`または`tuple_div`のいずれかが非整数値を含む場合、[tupleIntDiv](#tupleintdiv)のように、各非整数の分子または除数を最も近い整数に丸めて結果を計算します。

**例**

クエリ:

``` sql
SELECT tupleIntDivOrZero((5, 10, 15), (0, 0, 0));
```

結果:

``` text
┌─tupleIntDivOrZero((5, 10, 15), (0, 0, 0))─┐
│ (0,0,0)                                   │
└───────────────────────────────────────────┘
```

## tupleIntDivByNumber

分子タプルを指定された除数で整数除算し、商のタプルを返します。

**構文**

```sql
tupleIntDivByNumber(tuple_num, div)
```

**パラメータ**

- `tuple_num`: 分子の値のタプル。 [Tuple](../data-types/tuple) of numeric type。
- `div`: 除数の値。 [Numeric](../data-types/int-uint.md) type。

**返される値**

- `tuple_num` と `div`の商のタプル。 [Tuple](../data-types/tuple) of integer values。

**実装の詳細**

- `tuple_num`または`div`のいずれかが非整数値を含む場合、結果は各非整数の分子または除数を最も近い整数に丸めて計算されます。
- 0での除算にはエラーがスローされます。

**例**

クエリ:

``` sql
SELECT tupleIntDivByNumber((15, 10, 5), 5);
```

結果:

``` text
┌─tupleIntDivByNumber((15, 10, 5), 5)─┐
│ (3,2,1)                             │
└─────────────────────────────────────┘
```

クエリ:

``` sql
SELECT tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8);
```

結果:

``` text
┌─tupleIntDivByNumber((15.2, 10.7, 5.5), 5.8)─┐
│ (2,1,0)                                     │
└─────────────────────────────────────────────┘
```

## tupleIntDivOrZeroByNumber

[tupleIntDivByNumber](#tupleintdivbynumber)と同様に、分子タプルを指定された除数で整数除算し、商のタプルを返します。0の除数に対してエラーをスローするのではなく、商として0を返します。

**構文**

```sql
tupleIntDivOrZeroByNumber(tuple_num, div)
```

**パラメータ**

- `tuple_num`: 分子の値のタプル。 [Tuple](../data-types/tuple) of numeric type。
- `div`: 除数の値。 [Numeric](../data-types/int-uint.md) type。

**返される値**

- `tuple_num` と `div`の商のタプル。 [Tuple](../data-types/tuple) of integer values。
- 除数が0の場合、商として0を返します。

**実装の詳細**

- `tuple_num`または`div`のいずれかが非整数値を含む場合、[tupleIntDivByNumber](#tupleintdivbynumber)のように、各非整数の分子または除数を最も近い整数に丸めて結果を計算します。

**例**

クエリ:

``` sql
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 5);
```

結果:

``` text
┌─tupleIntDivOrZeroByNumber((15, 10, 5), 5)─┐
│ (3,2,1)                                   │
└───────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT tupleIntDivOrZeroByNumber((15, 10, 5), 0)
```

結果:

``` text
┌─tupleIntDivOrZeroByNumber((15, 10, 5), 0)─┐
│ (0,0,0)                                   │
└───────────────────────────────────────────┘
```

## tupleModulo

2つのタプルの除算操作の剰余（余り）のタプルを返します。

**構文**

```sql
tupleModulo(tuple_num, tuple_mod)
```

**パラメータ**

- `tuple_num`: 分子の値のタプル。[Tuple](../data-types/tuple) of numeric type。
- `tuple_div`: 剰余の値のタプル。[Tuple](../data-types/tuple) of numeric type。

**返される値**

- `tuple_num` と `tuple_div`の余りのタプル。[Tuple](../data-types/tuple) of non-zero integer values。
- ゼロでの除算にはエラーがスローされます。

**例**

クエリ:

``` sql
SELECT tupleModulo((15, 10, 5), (5, 3, 2));
```

結果:

``` text
┌─tupleModulo((15, 10, 5), (5, 3, 2))─┐
│ (0,1,1)                             │
└─────────────────────────────────────┘
```

## tupleModuloByNumber

指定された除数でタプルの除算操作の剰余（余り）のタプルを返します。

**構文**

```sql
tupleModuloByNumber(tuple_num, div)
```

**パラメータ**

- `tuple_num`: 分子の値のタプル。[Tuple](../data-types/tuple) of numeric type。
- `div`: 除数の値。 [Numeric](../data-types/int-uint.md) type。

**返される値**

- `tuple_num` と `div`の余りのタプル。[Tuple](../data-types/tuple) of non-zero integer values。
- ゼロでの除算にはエラーがスローされます。

**例**

クエリ:

``` sql
SELECT tupleModuloByNumber((15, 10, 5), 2);
```

結果:

``` text
┌─tupleModuloByNumber((15, 10, 5), 2)─┐
│ (1,0,1)                             │
└─────────────────────────────────────┘
```

## flattenTuple

ネストされた名前付き `input` タプルから平坦化された `output` タプルを返します。`output` タプルの要素は元の `input` タプルからのパスです。例: `Tuple(a Int, Tuple(b Int, c Int)) -> Tuple(a Int, b Int, c Int)`。`flattenTuple` を使用して、`Object` タイプからのすべてのパスを別々のカラムとして選択することができます。

**構文**

```sql
flattenTuple(input)
```

**パラメータ**

- `input`: 平坦化するネストされた名前付きタプル。[Tuple](../data-types/tuple)。

**返される値**

- 元の `input` からのパスが要素となる `output` タプル。[Tuple](../data-types/tuple)。

**例**

クエリ:

``` sql
CREATE TABLE t_flatten_tuple(t Tuple(t1 Nested(a UInt32, s String), b UInt32, t2 Tuple(k String, v UInt32))) ENGINE = Memory;
INSERT INTO t_flatten_tuple VALUES (([(1, 'a'), (2, 'b')], 3, ('c', 4)));
SELECT flattenTuple(t) FROM t_flatten_tuple;
```

結果:

``` text
┌─flattenTuple(t)───────────┐
│ ([1,2],['a','b'],3,'c',4) │
└───────────────────────────┘
```

## 距離関数

すべてのサポートされている関数は[距離関数のドキュメント](../../sql-reference/functions/distance-functions.md)で説明されています。
