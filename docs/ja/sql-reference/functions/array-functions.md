---
slug: /ja/sql-reference/functions/array-functions
sidebar_position: 10
sidebar_label: Arrays
---

# Array Functions

## empty {#empty}

入力配列が空であるかをチェックします。

**構文**

``` sql
empty([x])
```

配列は、要素が含まれていない場合に空とみなされます。

:::note
[`optimize_functions_to_subcolumns` 設定](../../operations/settings/settings.md#optimize-functions-to-subcolumns) を有効にすることで最適化できます。`optimize_functions_to_subcolumns = 1` の場合、この関数は配列全体のカラムを読み込んで処理するのではなく、[size0](../data-types/array.md#array-size) サブカラムのみを読み込みます。クエリ `SELECT empty(arr) FROM TABLE;` は `SELECT arr.size0 = 0 FROM TABLE;` に変換されます。
:::

この関数は、[文字列](string-functions.md#empty)や[UUID](uuid-functions.md#empty)に対しても動作します。

**引数**

- `[x]` — 入力配列。 [Array](../data-types/array.md)。

**戻り値**

- 空の配列には `1`、非空の配列には `0` を返します。 [UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT empty([]);
```

結果:

```text
┌─empty(array())─┐
│              1 │
└────────────────┘
```

## notEmpty {#notempty}

入力配列が空でないかをチェックします。

**構文**

``` sql
notEmpty([x])
```

配列は、少なくとも1つの要素が含まれている場合に空でないとみなされます。

:::note
[optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) 設定を有効にすることで最適化できます。`optimize_functions_to_subcolumns = 1` の場合、この関数は配列全体のカラムを読み込んで処理するのではなく、[size0](../data-types/array.md#array-size) サブカラムのみを読み込みます。クエリ `SELECT notEmpty(arr) FROM table` は `SELECT arr.size0 != 0 FROM TABLE;` に変換されます。
:::

この関数は、[文字列](string-functions.md#notempty)や[UUID](uuid-functions.md#notempty)に対しても動作します。

**引数**

- `[x]` — 入力配列。 [Array](../data-types/array.md)。

**戻り値**

- 非空の配列には `1`、空の配列には `0` を返します。 [UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT notEmpty([1,2]);
```

結果:

```text
┌─notEmpty([1, 2])─┐
│                1 │
└──────────────────┘
```

## length

配列の項目数を返します。
結果の型は UInt64 です。
文字列でも動作します。

`optimize_functions_to_subcolumns` 設定を有効にすると、配列全体のカラムを読み込んで処理する代わりに、[size0](../data-types/array.md#array-size) サブカラムのみを読み込みます。クエリ `SELECT length(arr) FROM table` は `SELECT arr.size0 FROM TABLE` に変換されます。

エイリアス: `OCTET_LENGTH`

## emptyArrayUInt8

空の UInt8 配列を返します。

**構文**

```sql
emptyArrayUInt8()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayUInt8();
```

結果:

```response
[]
```

## emptyArrayUInt16

空の UInt16 配列を返します。

**構文**

```sql
emptyArrayUInt16()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayUInt16();

```

結果:

```response
[]
```

## emptyArrayUInt32

空の UInt32 配列を返します。

**構文**

```sql
emptyArrayUInt32()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayUInt32();
```

結果:

```response
[]
```

## emptyArrayUInt64

空の UInt64 配列を返します。

**構文**

```sql
emptyArrayUInt64()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayUInt64();
```

結果:

```response
[]
```

## emptyArrayInt8

空の Int8 配列を返します。

**構文**

```sql
emptyArrayInt8()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayInt8();
```

結果:

```response
[]
```

## emptyArrayInt16

空の Int16 配列を返します。

**構文**

```sql
emptyArrayInt16()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayInt16();
```

結果:

```response
[]
```

## emptyArrayInt32

空の Int32 配列を返します。

**構文**

```sql
emptyArrayInt32()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayInt32();
```

結果:

```response
[]
```

## emptyArrayInt64

空の Int64 配列を返します。

**構文**

```sql
emptyArrayInt64()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayInt64();
```

結果:

```response
[]
```

## emptyArrayFloat32 

空の Float32 配列を返します。

**構文**

```sql
emptyArrayFloat32()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayFloat32();
```

結果:

```response
[]
```

## emptyArrayFloat64

空の Float64 配列を返します。

**構文**

```sql
emptyArrayFloat64()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayFloat64();
```

結果:

```response
[]
```

## emptyArrayDate

空の Date 配列を返します。

**構文**

```sql
emptyArrayDate()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayDate();
```

## emptyArrayDateTime

空の DateTime 配列を返します。

**構文**

```sql
[]
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayDateTime();
```

結果:

```response
[]
```

## emptyArrayString

空の String 配列を返します。

**構文**

```sql
emptyArrayString()
```

**引数**

なし。

**戻り値**

空の配列。

**例**

クエリ:

```sql
SELECT emptyArrayString();
```

結果:

```response
[]
```

## emptyArrayToSingle

空の配列を受け取り、デフォルト値と等しい1要素の配列を返します。

## range(end), range(\[start, \] end \[, step\])

`start`から`end - 1`までの数値を`step`で増加する配列を返します。サポートされる型は[UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../data-types/int-uint.md)です。

**構文**

``` sql
range([start, ] end [, step])
```

**引数**

- `start` — 配列の最初の要素。`step`を使用する場合は必須です。デフォルト値: 0。
- `end` — 配列が構築される前の数。必須。
- `step` — 配列内の各要素間の増加ステップを決定する。オプション。デフォルト値: 1。

**戻り値**

- `start`から`end - 1`までの数値を`step`で増加する配列。

**実装の詳細**

- すべての引数`start`、`end`、`step`は以下のデータ型でなければなりません: `UInt8`, `UInt16`, `UInt32`, `UInt64`,`Int8`, `Int16`, `Int32`, `Int64`。返される配列の要素の型はすべての引数のスーパー型です。
- クエリが[function_range_max_elements_in_block](../../operations/settings/settings.md#function_range_max_elements_in_block)設定で指定された以上の要素数の配列を返す場合、例外がスローされます。
- 任意の引数がNullable(Nothing)型の場合、`NULL`を返します。任意の引数がNull値を持つ場合は例外をスローします(Nullable(T)型)。

**例**

クエリ:

``` sql
SELECT range(5), range(1, 5), range(1, 5, 2), range(-1, 5, 2);
```

結果:

```txt
┌─range(5)────┬─range(1, 5)─┬─range(1, 5, 2)─┬─range(-1, 5, 2)─┐
│ [0,1,2,3,4] │ [1,2,3,4]   │ [1,3]          │ [-1,1,3]        │
└─────────────┴─────────────┴────────────────┴─────────────────┘
```

## array(x1, ...), operator \[x1, ...\]

関数の引数から配列を作成します。
引数は定数であり、すべての共通の型を持つタイプでなければなりません。どのタイプの配列を作成すべきか不明なため、少なくとも1つの引数を渡す必要があります。つまり、この関数を使用して空の配列を作成することはできません（上記で説明された 'emptyArray\*' 関数を使用してください）。
‘Array(T)’ 型の結果を返します。ここで ‘T’ は渡された引数の最小共通型です。

## arrayWithConstant(length, elem)

長さ `length` の配列を生成し、定数 `elem` で満たします。

## arrayConcat

引数として渡された配列を結合します。

``` sql
arrayConcat(arrays)
```

**引数**

- `arrays` – 任意の数の [Array](../data-types/array.md) 型の引数。

**例**

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr, n), operator arr\[n\]

配列 `arr` のインデックス `n` の要素を取得します。`n` は任意の整数型である必要があります。
配列のインデックスは1から始まります。

負のインデックスもサポートされています。この場合、配列の末尾から対応する要素を選択します。たとえば、`arr[-1]` は配列の最後の項目です。

インデックスが配列の範囲外になる場合、非定数配列と定数インデックス0の場合を除いて、デフォルト値（数値の場合は0、文字列の場合は空文字列など）が返されます（この場合、「Array indices are 1-based」というエラーが発生します）。

## has(arr, elem)

`arr` 配列が `elem` 要素を持っているかをチェックします。
配列に要素がない場合は0、ある場合は1を返します。

`NULL`は値として処理されます。

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## arrayElementOrNull(arr, n)

配列 `arr` のインデックス `n` の要素を取得します。`n` は任意の整数型である必要があります。
配列のインデックスは1から始まります。

負のインデックスもサポートされており、この場合、配列の末尾から対応する要素を選択します。たとえば、`arr[-1]` は配列の最後の項目です。

インデックスが配列の範囲外になる場合、デフォルト値の代わりに `NULL` を返します。

### 例

``` sql
SELECT arrayElementOrNull([1, 2, 3], 2), arrayElementOrNull([1, 2, 3], 4)
```

``` text
 ┌─arrayElementOrNull([1, 2, 3], 2)─┬─arrayElementOrNull([1, 2, 3], 4)─┐
 │                                2 │                             ᴺᵁᴸᴸ │
 └──────────────────────────────────┴──────────────────────────────────┘
```

## hasAll {#hasall}

ある配列が別の配列の部分集合であるかをチェックします。

``` sql
hasAll(set, subset)
```

**引数**

- `set` – 要素のセットを持つ任意のタイプの配列。
- `subset` – `set`との共通スーパータイプを共有する要素を持つ任意のタイプの配列で、`set` の部分集合であることがテストされるべき要素を含んでいます。

**戻り値**

- `subset` のすべての要素が `set` に含まれている場合は`1`を返します。
- それ以外の場合は`0`を返します。

集合と部分集合の要素が共通スーパータイプを持たない場合、例外 `NO_COMMON_TYPE` が発生します。

**特殊特性**

- 空の配列は任意の配列の部分集合です。
- `Null`は値として処理されます。
- 両方の配列における値の順序は関係ありません。

**例**

`SELECT hasAll([], [])` は1を返します。

`SELECT hasAll([1, Null], [Null])` は1を返します。

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` は1を返します。

`SELECT hasAll(['a', 'b'], ['a'])` は1を返します。

`SELECT hasAll([1], ['a'])` は`NO_COMMON_TYPE`例外を発生させます。

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` は0を返します。

## hasAny {#hasany}

2つの配列がいくつかの要素で交差しているかをチェックします。

``` sql
hasAny(array1, array2)
```

**引数**

- `array1` – 要素のセットを持つ任意のタイプの配列。
- `array2` – `array1`との共通スーパータイプを共有する任意のタイプの配列。

**戻り値**

- `array1` と `array2` の間に少なくとも一つの共通要素がある場合、`1`を返します。
- それ以外の場合は`0`を返します。

`array1` と `array2` の要素が共通スーパータイプを共有しない場合、例外 `NO_COMMON_TYPE` が発生します。

**特殊特性**

- `Null`は値として処理されます。
- 両方の配列における値の順序は関係ありません。

**例**

`SELECT hasAny([1], [])` は `0` を返します。

`SELECT hasAny([Null], [Null, 1])` は `1` を返します。

`SELECT hasAny([-128, 1., 512], [1])` は `1` を返します。

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` は `NO_COMMON_TYPE` 例外を発生させます。

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` は `1` を返します。

## hasSubstr

array2 の全要素が array1 に同じ順序で現れるかを確認します。したがって、関数は、かつ `array1 = prefix + array2 + suffix` である場合にのみ 1 を返します。

``` sql
hasSubstr(array1, array2)
```

言い換えれば、関数は`array2`のすべての要素が`array1`に含まれているかをチェックします。この場合、`hasAll` 関数のように、`array1` と `array2` の両方における要素は同じ順序にある必要があります。

例:

- `hasSubstr([1,2,3,4], [2,3])` は1を返しますが、`hasSubstr([1,2,3,4], [3,2])` は`0`を返します。
- `hasSubstr([1,2,3,4], [1,2,3])` は1を返しますが、`hasSubstr([1,2,3,4], [1,2,4])` は`0`を返します。

**引数**

- `array1` – 要素のセットを持つ任意のタイプの配列。
- `array2` – 任意のタイプの配列。

**戻り値**

- `array1` が `array2` を含む場合、`1`を返します。
- それ以外の場合は`0`を返します。

`array1` と `array2` の要素が共通スーパータイプを共有しない場合、例外 `NO_COMMON_TYPE` が発生します。

**特殊特性**

- 関数は `array2` が空の場合、`1` を返します。
- `Null`は値として処理されます。たとえば、`hasSubstr([1, 2, NULL, 3, 4], [2,3])` は `0` を返しますが、`hasSubstr([1, 2, NULL, 3, 4], [2,NULL,3])` は `1` を返します。
- 両配列の値の順序は重要です。

**例**

`SELECT hasSubstr([], [])` returns 1.

`SELECT hasSubstr([1, Null], [Null])` returns 1.

`SELECT hasSubstr([1.0, 2, 3, 4], [1, 3])` returns 0.

`SELECT hasSubstr(['a', 'b'], ['a'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'b'])` returns 1.

`SELECT hasSubstr(['a', 'b' , 'c'], ['a', 'c'])` returns 0.

`SELECT hasSubstr([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]])` returns 1.

`SELECT hasSubstr([1, 2, NULL, 3, 4], ['a'])` raises a `NO_COMMON_TYPE` exception.


## indexOf(arr, x)

配列に'x'要素が含まれる場合、その最初のインデックスを返し、そうでない場合は0を返します。

例:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

`NULL` に設定された要素は通常の値として扱われます。

## arrayCount(\[func,\] arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外の値を返す要素の数を返します。`func` が指定されていない場合、配列内の0以外の要素の数を返します。

`arrayCount`は[高次関数](../../sql-reference/functions/index.md#higher-order-functions)です。最初の引数としてラムダ関数を渡すことができます。

## arrayDotProduct

2つの配列のドット積を返します。

**構文**

```sql
arrayDotProduct(vector1, vector2)
```

エイリアス: `scalarProduct`, `dotProduct`

**パラメータ**

- `vector1`: 最初のベクトル。 [Array](../data-types/array.md) または [Tuple](../data-types/tuple.md) の数値値。
- `vector2`: 二番目のベクトル。[Array](../data-types/array.md) または [Tuple](../data-types/tuple.md) の数値値。

:::note
2つのベクトルのサイズは等しくなければなりません。配列とタプルは混在した要素タイプを含んでいることもあります。
:::

**戻り値**

- 2つのベクトルのドット積。 [Numeric](https://clickhouse.com/docs/ja/native-protocol/columns#numeric-types)。

:::note
戻りの型は引数の型によって決まります。配列またはタプルが混在した要素タイプを含む場合、結果の型はスーパータイプです。
:::

**例**

クエリ:

```sql
SELECT arrayDotProduct([1, 2, 3], [4, 5, 6]) AS res, toTypeName(res);
```

結果:

```response
32	UInt16
```

クエリ:

```sql
SELECT dotProduct((1::UInt16, 2::UInt8, 3::Float32),(4::Int16, 5::Float32, 6::UInt8)) AS res, toTypeName(res);
```

結果:

```response
32	Float64
```

## countEqual(arr, x)

配列内のxと等しい要素の数を返します。`arrayCount (elem -> elem = x, arr)`と等価です。

`NULL`要素は別の値として扱われます。

例:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## arrayEnumerate(arr)

配列\[1, 2, 3, ..., length (arr)\]を返します。

この関数は通常、ARRAY JOIN で使用されます。それは ARRAY JOIN を適用した後に各配列に対してちょうど一度何かをカウントすることを可能にします。例:

``` sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

この例では、Reaches はコンバージョンの数（ARRAY JOIN を適用した後に受け取った文字列）、Hits はページビューの数（ARRAY JOIN の前の文字列）です。この特定のケースでは、次の簡単な方法で同じ結果を得ることができます:

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

この関数は高次関数でも使用できます。例えば、条件に一致する要素の配列インデックスを取得するために使用できます。

## arrayEnumerateUniq(arr, ...)

ソース配列と同じサイズの配列を返し、各要素がその値と同じ別の要素の中で初めて出現する位置を示します。
例: arrayEnumerateUniq([10, 20, 10, 30]) = [1, 1, 2, 1]。

この関数は ARRAY JOIN でネストされたデータ構造と配列要素のさらなる集約を行う際に役立ちます。
例:

``` sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

``` text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

この例では、各ゴールIDについて、コンバージョンの数（ネストされたGoalsデータ構造中の各要素が到達したゴールであることを示す）、およびセッションの数が計算されています。ARRAY JOIN を使用しない場合、sum(Sign) としてセッションの数を数えます。しかし、この特定のケースでは、ネストされた Goals 構造によって行が増幅され、これをカウント後に一度行うために、関数 arrayEnumerateUniq(Goals.ID) の値に条件を適用します。

The arrayEnumerateUniq function can take multiple arrays of the same size as arguments. In this case, uniqueness is considered for tuples of elements in the same positions in all the arrays.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

This is necessary when using ARRAY JOIN with a nested data structure and further aggregation across multiple elements in this structure.

## arrayEnumerateUniqRanked

ソース配列と同じサイズの配列を返し、各要素の初めての出現を要素の値と同じ位置の要素において示します。多次元配列を列挙し、配列のどの深さまで見ていくかを指定することができます。

**構文**

```sql
arrayEnumerateUniqRanked(clear_depth, arr, max_array_depth)
```

**パラメータ**

- `clear_depth`: 指定されたレベルで要素を個別に列挙します。`max_arr_depth` 以下の正の整数。
- `arr`: 列挙される N 次元配列。[Array](../data-types/array.md)。
- `max_array_depth`: 有効な最大深度。`arr` の深さ以下の正の整数。

**例**

`clear_depth=1`および`max_array_depth=1`では、`arrayEnumerateUniq`で同じ配列を指定した場合と同じ結果を返します。

クエリ:

``` sql
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1);
```

結果:

``` text
[1,1,2]
```

この例では、`arrayEnumerateUniqRanked`を使用して、多次元配列の各要素が同じ値を持つ要素の中で最初に出現する位置を示す配列を取得します。提供された配列の1行目`[1,2,3]`に対して、対応する結果は`[1,1,1]`で、これは`1`,`2`, `3`が初めて発見されたことを示します。提供された配列の2行目`[2,2,1]`に対して、結果の対応する結果は`[2,3,3]`で、`2`が二度目と三度目に出現したこと、`1` が二度目に出現したことを示します。同様に、提供された配列の三行目 `[3]` に対する結果は `[2]` という結果が続きます。それは `3` が二度目に出現したことを示します。

クエリ:

``` sql
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2);
```

結果:

``` text
[[1,1,1],[2,3,2],[2]]
```

`clear_depth=2`に変更すると、各行が別々に列挙されます。

クエリ:

``` sql
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2);
```

結果:

``` text
[[1,1,1],[1,2,1],[1]]
```

## arrayPopBack

配列から最後の項目を削除します。

``` sql
arrayPopBack(array)
```

**引数**

- `array` – 配列。

**例**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res;
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront

配列から最初の項目を削除します。

``` sql
arrayPopFront(array)
```

**引数**

- `array` – 配列。

**例**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res;
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack

配列の末尾に1つの項目を追加します。

``` sql
arrayPushBack(array, single_value)
```

**引数**

- `array` – 配列。
- `single_value` – 一つの値。配列に数値がある場合は数値のみ、文字列がある場合は文字列のみを追加できます。数値を追加する際、ClickHouseは自動的に`single_value`の型を配列のデータ型に設定します。ClickHouseにおけるデータ型についての詳細は、"[データ型](../data-types/index.md#data_types)"を参照してください。`NULL` を使用することができます。この関数は配列に`NULL`要素を追加し、配列要素の型を`Nullable`に変換します。

**例**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res;
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront

配列の先頭に1つの要素を追加します。

``` sql
arrayPushFront(array, single_value)
```

**引数**

- `array` – 配列。
- `single_value` – 一つの値。配列に数値がある場合は数値のみ、文字列がある場合は文字列のみを追加できます。数値を追加する際、ClickHouseは自動的に`single_value`の型を配列のデータ型に設定します。ClickHouseにおけるデータ型についての詳細は、"[データ型](../data-types/index.md#data_types)"を参照してください。`NULL` を使用することができます。この関数は配列に`NULL`要素を追加し、配列要素の型を`Nullable`に変換します。

**例**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res;
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize

配列の長さを変更します。

``` sql
arrayResize(array, size[, extender])
```

**引数:**

- `array` — 配列。
- `size` — 配列の必要な長さ。
  - `size` が元の配列のサイズより小さい場合、配列は右から切り捨てられます。
- `size` が配列の初期サイズより大きい場合、配列は右に`extender`の値または配列要素のデータ型のデフォルト値で拡張されます。
- `extender` — 配列を拡張するための値。`NULL` を指定できます。

**戻り値:**

長さが `size` の配列。

**例**

``` sql
SELECT arrayResize([1], 3);
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL);
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice

配列のスライスを返します。

``` sql
arraySlice(array, offset[, length])
```

**引数**

- `array` – データの配列。
- `offset` – 配列の端からのインデント。正の値は左側のオフセット、負の値は右側へのインデントを示します。配列項目の番号付けは1から始まります。
- `length` – 必要なスライスの長さ。負の値を指定すると、関数は開いているスライス `[offset, array_length - length]` を返します。値を省略すると、関数はスライス `[offset, the_end_of_array]` を返します。

**例**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res;
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

`NULL` に設定された配列要素は通常の値として扱われます。

## arrayShingles

入力配列の指定された長さの連続するサブ配列からなる "shingles" を生成します。

**構文**

``` sql
arrayShingles(array, length)
```

**引数**

- `array` — 入力配列 [Array](../data-types/array.md)。
- `length` — 各シングルの長さ。

**戻り値**

- 生成されたシングルの配列。[Array](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayShingles([1,2,3,4], 3) as res;
```

結果:

``` text
┌─res───────────────┐
│ [[1,2,3],[2,3,4]] │
└───────────────────┘
```

## arraySort(\[func,\] arr, ...) {#sort}

`arr` 配列の要素を昇順にソートします。`func` 関数が指定されている場合、配列の要素に適用された `func` 関数の結果によってソート順が決まります。`func` が複数の引数を受け取る場合、`arraySort` 関数には `func` の引数が対応する複数の配列が渡されます。`arraySort` の説明の終わりに詳細な例が示されています。

整数値をソートする例:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

文字列値をソートする例:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

`NULL`, `NaN` および `Inf` 値のソート順は次のとおりです:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

- `-Inf` 値は配列の最初に位置します。
- `NULL` 値は配列の最後に位置します。
- `NaN` 値は `NULL` の直前に位置します。
- `Inf` 値は `NaN` の直前に位置します。

`arraySort` は[高次の関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。この場合、ラムダ関数の要素に施された結果によってソート順が決まります。

以下の例を考えてみましょう:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

ソース配列の各要素に対して、ラムダ関数がソート用のキーを返します。これは、\[1 -> -1, 2 -> -2, 3 -> -3\] です。`arraySort` 関数はキーを昇順でソートするため、結果は \[3, 2, 1\] となります。したがって、`(x) -> -x` ラムダ関数が[降順](#arrayreversesort)のソートを設定します。

ラムダ関数は複数の引数を受け取ることができます。この場合、`arraySort` 関数に対して、ラムダ関数の引数が対応する同じ長さの複数の配列を渡す必要があります。結果の配列は最初の入力配列の要素で構成され、次の入力配列の要素がソートキーを指定します。例:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

ここで、2番目の配列に渡された要素（\[2, 1\]）が、ソース配列の対応する要素（\[‘hello’, ‘world’\]）に対するソートキーを定義します。つまり、\[‘hello’ -> 2, ‘world’ -> 1\] です。ラムダ関数は `x` を使用しないため、ソース配列の実際の値は結果の順序に影響を与えません。そのため、‘hello’ は結果の2番目の要素になり、‘world’ は最初の要素になります。

その他の例を以下に示します。

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

:::note
ソート効率を向上させるため、[Schwartzian transform](https://en.wikipedia.org/wiki/Schwartzian_transform)が使用されています。
:::

## arrayPartialSort(\[func,\] limit, arr, ...)

`arraySort` と同様ですが、部分ソートを許可するために追加の `limit` 引数があります。戻り値は元の配列と同じサイズの配列で、範囲 `[1..limit]` の要素は昇順にソートされます。残りの要素 `(limit..N]` には未指定の順序の要素が含まれます。

## arrayReverseSort

`arr` 配列の要素を降順にソートします。`func` 関数が指定されている場合、配列の要素に適用された `func` 関数の結果によって `arr` がソートされ、次にソートされた配列が逆順になります。`func` が複数の引数を受け取る場合、`arrayReverseSort` 関数には `func` の引数が対応する複数の配列が渡されます。`arrayReverseSort` の説明の終わりに詳細な例が示されています。

**構文**

```sql
arrayReverseSort([func,] arr, ...)
```
整数値をソートする例:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

文字列値をソートする例:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

`NULL`, `NaN` および `Inf` 値のソート順は次のとおりです:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

- `Inf` 値は配列の最初に位置します。
- `NULL` 値は配列の最後に位置します。
- `NaN` 値は `NULL` の直前に位置します。
- `-Inf` 値は `NaN` の直前に位置します。

`arrayReverseSort` は[高次の関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。例を以下に示します。

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

この配列は以下のようにソートされます:

1. 最初に、ソース配列（\[1, 2, 3\]）が配列の要素に適用されたラムダ関数の結果によってソートされます。結果は配列 \[3, 2, 1\] です。
2. 前のステップで得られた配列を逆順にします。したがって、最終的な結果は \[1, 2, 3\] です。

ラムダ関数は複数の引数を受け取ることができます。この場合、`arrayReverseSort` 関数に対して、ラムダ関数の引数が対応する同じ長さの複数の配列を渡す必要があります。結果の配列は最初の入力配列の要素で構成され、次の入力配列の要素がソートキーを指定します。例:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

この例では、配列は以下のようにソートされます:

1. 最初に、ソース配列（\[‘hello’, ‘world’\]）が配列の要素に適用されたラムダ関数の結果によってソートされます。2 番目の配列に渡された要素（\[2, 1\]）がソートキーを対応するソース配列の要素に定義します。結果は配列 \[‘world’, ‘hello’\] です。
2. 前のステップでソートされた配列を逆順にします。したがって、最終的な結果は \[‘hello’, ‘world’\] です。

以下に他の例を示します。

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

``` text
┌─res─────┐
│ [5,3,4] │
└─────────┘
```

``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

``` text
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayPartialReverseSort(\[func,\] limit, arr, ...)

`arrayReverseSort`と同様ですが、部分ソートを許可するために追加の `limit` 引数があります。戻りは元の配列と同じサイズの配列であり、範囲 `[1..limit]` の要素は降順にソートされ、残りの要素は未指定の順序で含まれます。

## arrayShuffle

元の配列と同じサイズの配列を返し、要素がシャッフルされた順序で格納されます。
各可能な要素の並び替えが出現する確率が等しくなるように要素が並べ替えられます。

**構文**

```sql
arrayShuffle(arr[, seed])
```

**パラメータ**

- `arr`: 部分的にシャッフルされる配列。[Array](../data-types/array.md)。
- `seed`（オプション）: 乱数生成に使用されるシード。指定されていない場合はランダムなものが使用されます。[UInt または Int](../data-types/int-uint.md)。

**戻り値**

- シャッフルされた要素を持つ配列。

**実装の詳細**

:::note 
この関数は定数を具体化しません。
:::

**例**

この例では、`arrayShuffle`を`seed`を指定せずに使用しています。そのため、ランダムに生成されます。

クエリ:

```sql
SELECT arrayShuffle([1, 2, 3, 4]);
```

注意: [ClickHouse Fiddle](https://fiddle.clickhouse.com/)を使用する場合、関数のランダムな性質により正確な応答が異なる可能性があります。

結果: 

```response
[1,4,2,3]
```

この例では、`arrayShuffle`に`seed`が指定されており、安定した結果が生成されます。

クエリ:

```sql
SELECT arrayShuffle([1, 2, 3, 4], 41);
```

結果: 

```response
[3,2,1,4]
```

## arrayPartialShuffle

カードの大きさ `N` の入力配列を与えられた場合、範囲 `[1...limit]` の要素がシャッフルされ、残りの要素（範囲 `(limit...n]`) はシャッフルされない配列を返します。

**構文**

```sql
arrayPartialShuffle(arr[, limit[, seed]])
```

**パラメータ**

- `arr`: 部分的にシャッフルされる配列。[Array](../data-types/array.md)。
- `limit`（オプション）: 要素の交換を制限するための数。[UInt または Int](../data-types/int-uint.md)。
- `seed`（オプション）: 乱数生成に使用されるシード値。指定されていない場合はランダムなものが使用されます。[UInt または Int](../data-types/int-uint.md)。

**戻り値**

- 部分的にシャッフルされた要素を持つ配列。

**実装の詳細**

:::note 
この関数は定数を具体化しません。

`limit` の値は `[1..N]` 範囲内にある必要があります。この範囲外の値は、完全な [arrayShuffle](#arrayshuffle) を実行するのと同等です。
:::

**例**

注意: [ClickHouse Fiddle](https://fiddle.clickhouse.com/)を使用する場合、関数のランダムな性質により正確な応答が異なる可能性があります。 

クエリ:

```sql
SELECT arrayPartialShuffle([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1)
```

結果:

要素の順序は (`[2,3,4,5], [7,8,9,10]`) のままで、シャッフルされた2つの要素 `[1, 6]` を除きます。`seed` が指定されていないため、関数はランダムに選びます。

```response
[6,2,3,4,5,1,7,8,9,10]
```

この例では、`limit` が `2` に増加し、`seed` 値が指定されています。

クエリ:

```sql
SELECT arrayPartialShuffle([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2);
```

要素の順序は (`[4, 5, 6, 7, 8], [10]`) のままで、シャッフルされた4つの要素 `[1, 2, 3, 9]` を除きます。

結果: 
```response
[3,9,1,4,5,6,7,8,2,10]
```

## arrayUniq(arr, ...)

1つの引数が渡された場合、配列内の異なる要素の数をカウントします。
複数の引数が渡された場合、複数の配列内の対応する位置の要素の異なる組み合わせの数をカウントします。

配列内の一意のアイテムのリストを取得する場合、`arrayReduce('groupUniqArray', arr)`を使用できます。

## arrayJoin(arr)

特別な関数。[「arrayJoin関数」](../../sql-reference/functions/array-join.md#functions_arrayjoin) セクションを参照してください。

## arrayDifference

隣接する配列要素間の差分の配列を計算します。結果配列の最初の要素は0、2番目は`a[1] - a[0]`、3番目は`a[2] - a[1]` などです。結果配列の要素の型は減算の型推論ルールによって決まります（例: `UInt8` - `UInt8` = `Int16`）。

**構文**

``` sql
arrayDifference(array)
```

**引数**

- `array` – [Array](https://clickhouse.com/docs/ja/data_types/array/)。

**戻り値**

隣接する配列要素間の差分の配列を返します。[UInt\*](https://clickhouse.com/docs/ja/data_types/int_uint/#uint-ranges)、[Int\*](https://clickhouse.com/docs/ja/data_types/int_uint/#int-ranges)、[Float\*](https://clickhouse.com/docs/ja/data_types/float/)。

**例**

クエリ:

``` sql
SELECT arrayDifference([1, 2, 3, 4]);
```

結果:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

Int64 によるオーバーフローの例:

クエリ:

``` sql
SELECT arrayDifference([0, 10000000000000000000]);
```

結果:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct

配列を取り、一意の要素のみを含む配列を返します。

**構文**

``` sql
arrayDistinct(array)
```

**引数**

- `array` – [Array](https://clickhouse.com/docs/ja/data_types/array/)。

**戻り値**

一意の要素を含む配列を返します。

**例**

クエリ:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1]);
```

結果:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## arrayEnumerateDense

配列と同じサイズの配列を返し、各要素のソース配列における最初の出現位置を示します。

**構文**

```sql
arrayEnumerateDense(arr)
```

**例**

クエリ:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

結果:

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```
## arrayEnumerateDenseRanked

配列と同じサイズの配列を返し、各要素の最初の出現場所をソース配列において示します。多次元配列を列挙し、配列のどの深さまで見ていくかを指定することができます。

**構文**

```sql
arrayEnumerateDenseRanked(clear_depth, arr, max_array_depth)
```

**パラメータ**

- `clear_depth`: 指定されたレベルで要素を個別に列挙します。`max_arr_depth`以下の正の整数。
- `arr`: 列挙されるN次元配列。[Array](../data-types/array.md)。
- `max_array_depth`: 有効な最大深度。`arr` の深さ以下の正の整数。

**例**

`clear_depth=1`および`max_array_depth=1`では、[arrayEnumerateDense](#arrayenumeratedense) と同じ結果を返します。

クエリ:

``` sql
SELECT arrayEnumerateDenseRanked(1,[10, 20, 10, 30],1);
```

結果:

``` text
[1,2,1,3]
```

この例では、`arrayEnumerateDenseRanked` を使用して、多次元配列の各要素が同じ値を持つ別の要素の中で最初に出現する位置を示す配列を取得します。最初の行がソース配列の`[10,10,30,20]`である場合、この行に対応する結果は `[1,1,2,3]` であり、`10`が初めて発見された位置は位置1と2であること、`30`は別の発見された要素である位置 3、`20`はさらに別の発見された要素である位置4。第2の行が `[40, 50, 10, 30]` であるとき、結果の第2行は `[4,5,1,2]` であり、`40`と`50`が4番目と5番目であることを示しますこの行の位置1と2の要素として、別の`10`（最初に見つかった数）が位置3に、`30`（2番目の数）が最後の位置にあります。

クエリ:

``` sql
SELECT arrayEnumerateDenseRanked(1,[[10,10,30,20],[40,50,10,30]],2);
```

結果:

``` text
[[1,1,2,3],[4,5,1,2]]
```

`clear_depth=2`に変更すると、各行が別々に列挙されます。

クエリ:

``` sql
SELECT arrayEnumerateDenseRanked(2,[[10,10,30,20],[40,50,10,30]],2);
```

結果:

``` text
[[1,1,2,3],[1,2,3,4]]
```

## arrayUnion(arr)

複数の配列を取り、ソース配列のいずれかに存在するすべての要素を含む配列を返します。

例:
```sql
SELECT
    arrayUnion([-2, 1], [10, 1], [-2], []) as num_example,
    arrayUnion(['hi'], [], ['hello', 'hi']) as str_example,
    arrayUnion([1, 3, NULL], [2, 3, NULL]) as null_example
```

```text
┌─num_example─┬─str_example────┬─null_example─┐
│ [10,-2,1]   │ ['hello','hi'] │ [3,2,1,NULL] │
└─────────────┴────────────────┴──────────────┘
```

## arrayIntersect(arr)

複数の配列を取り、すべてのソース配列に存在する要素を持つ配列を返します。

例:

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

``` text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayJaccardIndex

2つの配列の[ジャッカード係数](https://en.wikipedia.org/wiki/Jaccard_index)を返します。

**例**

クエリ:
``` sql
SELECT arrayJaccardIndex([1, 2], [2, 3]) AS res
```

結果:
``` text
┌─res────────────────┐
│ 0.3333333333333333 │
└────────────────────┘
```

## arrayReduce

集約関数を配列要素に適用し、その結果を返します。集約関数の名前はシングルクォートで囲まれた文字列 `'max'`, `'sum'` として渡されます。パラメトリック集計関数を使用する場合、関数名の後括弧で囲んでパラメータを指定します `'uniqUpTo(6)'`。

**構文**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**引数**

- `agg_func` — 集約関数の名前で、定数[文字列](../data-types/string.md)でなければなりません。
- `arr` — アイテムとして集約関数のパラメータとして使用される任意の数の[Array](../data-types/array.md)型のカラム。

**戻り値**

集約関数で指定された範囲内の結果を含む配列。[Array](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayReduce('max', [1, 2, 3]);
```

結果:

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

集約関数が複数の引数をとる場合、この関数は同じサイズの複数の配列に対して適用される必要があります。

クエリ:

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0]);
```

結果:

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

パラメトリック集計関数の例:

クエリ:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
```

結果:

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

**関連項目**

- [arrayFold](#arrayfold)

## arrayReduceInRanges

集約関数を指定された範囲内の配列要素に適用し、各範囲に対応する結果を含む配列を返します。この関数は複数の `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)` と同様の結果を返します。

**構文**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**引数**

- `agg_func` — 集約関数の名前で、定数[文字列](../data-types/string.md)でなければなりません。
- `ranges` — 集約される範囲で、各範囲のインデックスと長さを含む[タプル](../data-types/tuple.md)の配列[Array](../data-types/array.md)です。
- `arr` — 集約関数のパラメータとして使用される任意の数の[Array](../data-types/array.md)型のカラム。

**戻り値**

- 指定された範囲内の集約関数の結果を含む配列。[Array](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

結果:

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## arrayFold

ラムダ関数を同サイズの1つまたは複数の配列に適用し、結果をアキュムレータに集めます。

**構文**

``` sql
arrayFold(lambda_function, arr1, arr2, ..., accumulator)
```

**例**

クエリ:

``` sql
SELECT arrayFold( acc,x -> acc + x*2,  [1, 2, 3, 4], toInt64(3)) AS res;
```

結果:

``` text
┌─res─┐
│  23 │
└─────┘
```

**フィボナッチ数列の例**

```sql
SELECT arrayFold( acc,x -> (acc.2, acc.2 + acc.1), range(number), (1::Int64, 0::Int64)).1 AS fibonacci
FROM numbers(1,10);

┌─fibonacci─┐
│         0 │
│         1 │
│         1 │
│         2 │
│         3 │
│         5 │
│         8 │
│        13 │
│        21 │
│        34 │
└───────────┘
```

**関連項目**

- [arrayReduce](#arrayreduce)

## arrayReverse

元の配列と同じサイズの配列を返し、順番が逆になった要素を含みます。

**構文**

```sql
arrayReverse(arr)
```

例:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## reverse(arr)

「[arrayReverse](#arrayreverse)」の同義語

## arrayFlatten

配列の配列をフラットな配列に変換します。

関数：

- ネストされた配列の任意の深さに適用されます。
- すでにフラットである配列は変更しません。

フラット化した配列には、すべてのソース配列のすべての要素が含まれています。

**構文**

``` sql
flatten(array_of_arrays)
```

エイリアス: `flatten`.

**パラメータ**

- `array_of_arrays` — [Array](../data-types/array.md) の配列。例えば、`[[1,2,3], [4,5]]`.

**例**

``` sql
SELECT flatten([[[1]], [[2], [3]]]);
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact

配列から連続する重複要素を削除します。元の配列中のオーダーによって、結果の値の順序が決まります。

**構文**

``` sql
arrayCompact(arr)
```

**引数**

`arr` — チェックされる[Array](../data-types/array.md)です。

**戻り値**

重複を取り除いた配列。[Array](../data-types/array.md)です。

**例**

クエリ:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3]);
```

結果:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip

複数の配列を1つの配列に結合します。結果の配列には、ソース配列対応する要素が引数の順序でタプルにまとめられています。

**構文**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**引数**

- `arrN` — [Array](../data-types/array.md).

この機能は異なる型の任意の数の配列を受け取ることができます。すべての入力配列は同じサイズでなければなりません。

**戻り値**

- ソース配列の要素を[タプル](../data-types/tuple.md)にまとめた配列です。タプル内のデータ型は、入力配列の型と同じで、渡された配列の順序に従います。[Array](../data-types/array.md)です。

**例**

クエリ:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1]);
```

結果:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayZipUnaligned

ズレのある配列を考慮して複数の配列を1つに結合します。結果の配列には、ソース配列の対応する要素が引数のリストされた順序でタプルにまとめられています。

**構文**

``` sql
arrayZipUnaligned(arr1, arr2, ..., arrN)
```

**引数**

- `arrN` — [Array](../data-types/array.md).

この機能は異なる型の任意の数の配列を受け取ることができます。

**戻り値**

- ソース配列からの要素を[タ
`func(arr1[i], ..., arrN[i])`を各要素に適用することによって、元の配列から新たな配列を取得します。配列 `arr1` ... `arrN` は同じ数の要素を持っている必要があります。

例:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

次の例は、異なる配列の要素からタプルを作成する方法を示しています:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

`arrayMap`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayFilter(func, arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す`arr1`の要素のみを含む配列を返します。

例:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

``` text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

``` text
┌─res─┐
│ [2] │
└─────┘
```

`arrayFilter`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayFill(func, arr1, ...)

`arr1`を最初の要素から最後の要素までスキャンし、`func(arr1[i], ..., arrN[i])`が0を返す場合は`arr1[i - 1]`で`arr1[i]`を置き換えます。`arr1`の最初の要素は置き換えられません。

例:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

`arrayFill`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayReverseFill(func, arr1, ...)

`arr1`を最後の要素から最初の要素までスキャンし、`func(arr1[i], ..., arrN[i])`が0を返す場合は`arr1[i + 1]`で`arr1[i]`を置き換えます。`arr1`の最後の要素は置き換えられません。

例:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

`arrayReverseFill`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arraySplit(func, arr1, ...)

`arr1`を複数の配列に分割します。`func(arr1[i], ..., arrN[i])`が0以外を返す場合、配列はその要素の左側で分割されます。配列は最初の要素の前で分割されません。

例:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

`arraySplit`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayReverseSplit(func, arr1, ...)

`arr1`を複数の配列に分割します。`func(arr1[i], ..., arrN[i])`が0以外を返す場合、配列はその要素の右側で分割されます。配列は最後の要素の後では分割されません。

例:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

`arrayReverseSplit`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayExists(\[func,\] arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す要素が少なくとも1つ存在する場合は1を返します。それ以外の場合は0を返します。

`arrayExists`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

## arrayAll(\[func,\] arr1, ...)

`func(arr1[i], ..., arrN[i])`がすべての要素に対して0以外を返す場合は1を返します。それ以外は0を返します。

`arrayAll`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

## arrayFirst(func, arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す最初の要素を`arr1`配列から返します。

## arrayFirstOrNull

`func(arr1[i], ..., arrN[i])`が0以外を返す最初の要素を`arr1`配列から返します。そうでない場合は`NULL`を返します。

**構文**

```sql
arrayFirstOrNull(func, arr1, ...)
```

**パラメータ**

- `func`: ラムダ関数。[ラムダ関数](../functions/#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `arr1`: 操作対象の配列。[配列](../data-types/array.md)。

**返される値**

- 渡された配列の最初の要素。
- そうでない場合は`NULL`を返します

**実装の詳細**

`arrayFirstOrNull`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

**例**

クエリ:

```sql
SELECT arrayFirstOrNull(x -> x >= 2, [1, 2, 3]);
```

結果:

```response
2
```

クエリ:

```sql
SELECT arrayFirstOrNull(x -> x >= 2, emptyArrayUInt8());
```

結果:

```response
\N
```

クエリ:

```sql
SELECT arrayLastOrNull((x,f) -> f, [1,2,3,NULL], [0,1,0,1]);
```

結果:

```response
\N
```

## arrayLast(func, arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す最後の要素を`arr1`配列から返します。

`arrayLast`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayLastOrNull

`func(arr1[i], ..., arrN[i])`が0以外を返す最後の要素を`arr1`配列から返します。そうでない場合は`NULL`を返します。

**構文**

```sql
arrayLastOrNull(func, arr1, ...)
```

**パラメータ**

- `func`: ラムダ関数。[ラムダ関数](../functions/#higher-order-functions---operator-and-lambdaparams-expr-function)。
- `arr1`: 操作対象の配列。[配列](../data-types/array.md)。

**返される値**

- 渡された配列の最後の要素。
- そうでない場合は`NULL`を返します

**実装の詳細**

`arrayLastOrNull`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

**例**

クエリ:

```sql
SELECT arrayLastOrNull(x -> x >= 2, [1, 2, 3]);
```

結果:

```response
3
```

クエリ:

```sql
SELECT arrayLastOrNull(x -> x >= 2, emptyArrayUInt8());
```

結果:

```response
\N
```

## arrayFirstIndex(func, arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す最初の要素のインデックスを`arr1`配列から返します。

`arrayFirstIndex`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayLastIndex(func, arr1, ...)

`func(arr1[i], ..., arrN[i])`が0以外を返す最後の要素のインデックスを`arr1`配列から返します。

`arrayLastIndex`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡す必要があり、省略することはできません。

## arrayMin

元の配列内の要素の最小値を返します。

`func`関数が指定されている場合は、この関数によって変換された要素の最小値を返します。

`arrayMin`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

**構文**

```sql
arrayMin([func,] arr)
```

**引数**

- `func` — 関数。[Expression](../data-types/special-data-types/expression.md)。
- `arr` — 配列。[配列](../data-types/array.md)。

**返される値**

- 関数値の最小値（または配列の最小値）。

:::note
`func`が指定されている場合、戻り値の型は`func`の戻り値の型と一致します。それ以外の場合は配列の要素の型と一致します。
:::

**例**

クエリ:

```sql
SELECT arrayMin([1, 2, 4]) AS res;
```

結果:

```text
┌─res─┐
│   1 │
└─────┘
```

クエリ:

```sql
SELECT arrayMin(x -> (-x), [1, 2, 4]) AS res;
```

結果:

```text
┌─res─┐
│  -4 │
└─────┘
```

## arrayMax

元の配列内の要素の最大値を返します。

`func`関数が指定されている場合は、この関数によって変換された要素の最大値を返します。

`arrayMax`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

**構文**

```sql
arrayMax([func,] arr)
```

**引数**

- `func` — 関数。[Expression](../data-types/special-data-types/expression.md)。
- `arr` — 配列。[配列](../data-types/array.md)。

**返される値**

- 関数値の最大値（または配列の最大値）。

:::note
`func`が指定されている場合、戻り値の型は`func`の戻り値の型と一致します。それ以外の場合は配列の要素の型と一致します。
:::

**例**

クエリ:

```sql
SELECT arrayMax([1, 2, 4]) AS res;
```

結果:

```text
┌─res─┐
│   4 │
└─────┘
```

クエリ:

```sql
SELECT arrayMax(x -> (-x), [1, 2, 4]) AS res;
```

結果:

```text
┌─res─┐
│  -1 │
└─────┘
```

## arraySum

元の配列内の要素の合計値を返します。

`func`関数が指定されている場合は、この関数によって変換された要素の合計値を返します。

`arraySum`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

**構文**

```sql
arraySum([func,] arr)
```

**引数**

- `func` — 関数。[Expression](../data-types/special-data-types/expression.md)。
- `arr` — 配列。[配列](../data-types/array.md)。

**返される値**

- 関数値の合計値（または配列の合計値）。

:::note
戻り値の型:

- 元の配列（または`func`が指定されている場合は変換された値）の小数の場合 — [Decimal128](../data-types/decimal.md)。
- 浮動小数点数の場合 — [Float64](../data-types/float.md)。
- 符号なし整数の場合 — [UInt64](../data-types/int-uint.md)。 
- 符号付き整数の場合 — [Int64](../data-types/int-uint.md)。
:::

**例**

クエリ:

```sql
SELECT arraySum([2, 3]) AS res;
```

結果:

```text
┌─res─┐
│   5 │
└─────┘
```

クエリ:

```sql
SELECT arraySum(x -> x*x, [2, 3]) AS res;
```

結果:

```text
┌─res─┐
│  13 │
└─────┘
```

## arrayAvg

元の配列内の要素の平均を返します。

`func`関数が指定されている場合は、この関数によって変換された要素の平均を返します。

`arrayAvg`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

**構文**

```sql
arrayAvg([func,] arr)
```

**引数**

- `func` — 関数。[Expression](../data-types/special-data-types/expression.md)。
- `arr` — 配列。[配列](../data-types/array.md)。

**返される値**

- 関数値の平均値（または配列の平均値）。[Float64](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT arrayAvg([1, 2, 4]) AS res;
```

結果:

```text
┌────────────────res─┐
│ 2.3333333333333335 │
└────────────────────┘
```

クエリ:

```sql
SELECT arrayAvg(x -> (x * x), [2, 4]) AS res;
```

結果:

```text
┌─res─┐
│  10 │
└─────┘
```

## arrayCumSum(\[func,\] arr1, ...)

元の配列`arr1`の要素の部分（ランニング）合計を返します。`func`が指定されている場合、合計は`func`を`arr1`、`arr2`、...、`arrN`に適用することにより計算されます。すなわち、`func(arr1[i], ..., arrN[i])`です。

**構文**

``` sql
arrayCumSum(arr)
```

**引数**

- `arr` — 数値の[配列](../data-types/array.md)。

**返される値**

- 元の配列の要素の部分合計の配列を返します。[UInt\*](https://clickhouse.com/docs/ja/data_types/int_uint/#uint-ranges)、[Int\*](https://clickhouse.com/docs/ja/data_types/int_uint/#int-ranges)、[Float\*](https://clickhouse.com/docs/ja/data_types/float/)。

**例**

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

`arrayCumSum`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

## arrayCumSumNonNegative(\[func,\] arr1, ...)

`arrayCumSum`と同様に、元の配列の要素の部分（ランニング）合計を返します。`func`が指定されている場合、合計は`func`を`arr1`、`arr2`、...、`arrN`に適用することにより計算されます。すなわち、`func(arr1[i], ..., arrN[i])`です。`arrayCumSum`とは異なり、現在のランニング合計が0未満の場合、それは0に置き換えられます。

**構文**

``` sql
arrayCumSumNonNegative(arr)
```

**引数**

- `arr` — 数値の[配列](../data-types/array.md)。

**返される値**

- 元の配列の要素の非負部分合計の配列を返します。[UInt\*](https://clickhouse.com/docs/ja/data_types/int_uint/#uint-ranges)、[Int\*](https://clickhouse.com/docs/ja/data_types/int_uint/#int-ranges)、[Float\*](https://clickhouse.com/docs/ja/data_types/float/)。

**例**

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

`arraySumNonNegative`は[高階関数](../../sql-reference/functions/index.md#higher-order-functions)であることに注意してください。最初の引数としてラムダ関数を渡すことができます。

## arrayProduct

[配列](../data-types/array.md)の要素を乗算します。

**構文**

``` sql
arrayProduct(arr)
```

**引数**

- `arr` — 数値の[配列](../data-types/array.md)。

**返される値**

- 配列の要素の積。[Float64](../data-types/float.md)。

**例**

クエリ:

``` sql
SELECT arrayProduct([1,2,3,4,5,6]) as res;
```

結果:

``` text
┌─res───┐
│ 720   │
└───────┘
```

クエリ:

``` sql
SELECT arrayProduct([toDecimal64(1,8), toDecimal64(2,8), toDecimal64(3,8)]) as res, toTypeName(res);
```

戻り値の型は常に[Float64](../data-types/float.md)です。結果:

``` text
┌─res─┬─toTypeName(arrayProduct(array(toDecimal64(1, 8), toDecimal64(2, 8), toDecimal64(3, 8))))─┐
│ 6   │ Float64                                                                                  │
└─────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

## arrayRotateLeft

[配列](../data-types/array.md)を指定した要素数だけ左に回転させます。
要素数が負の場合は、配列は右に回転します。

**構文**

``` sql
arrayRotateLeft(arr, n)
```

**引数**

- `arr` — [配列](../data-types/array.md)。
- `n` — 回転させる要素の数。

**返される値**

- 指定された要素数だけ左に回転させた配列。[配列](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayRotateLeft([1,2,3,4,5,6], 2) as res;
```

結果:

``` text
┌─res───────────┐
│ [3,4,5,6,1,2] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayRotateLeft([1,2,3,4,5,6], -2) as res;
```

結果:

``` text
┌─res───────────┐
│ [5,6,1,2,3,4] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayRotateLeft(['a','b','c','d','e'], 3) as res;
```

結果:

``` text
┌─res───────────────────┐
│ ['d','e','a','b','c'] │
└───────────────────────┘
```

## arrayRotateRight

[配列](../data-types/array.md)を指定した要素数だけ右に回転させます。
要素数が負の場合は、配列は左に回転します。

**構文**

``` sql
arrayRotateRight(arr, n)
```

**引数**

- `arr` — [配列](../data-types/array.md)。
- `n` — 回転させる要素の数。

**返される値**

- 指定された要素数だけ右に回転させた配列。[配列](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayRotateRight([1,2,3,4,5,6], 2) as res;
```

結果:

``` text
┌─res───────────┐
│ [5,6,1,2,3,4] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayRotateRight([1,2,3,4,5,6], -2) as res;
```

結果:

``` text
┌─res───────────┐
│ [3,4,5,6,1,2] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayRotateRight(['a','b','c','d','e'], 3) as res;
```

結果:

``` text
┌─res───────────────────┐
│ ['c','d','e','a','b'] │
└───────────────────────┘
```

## arrayShiftLeft

[配列](../data-types/array.md)を指定した要素数だけ左にシフトします。
新しい要素は提供された引数または配列要素型のデフォルト値で埋められます。
要素数が負の場合は、配列は右にシフトされます。

**構文**

``` sql
arrayShiftLeft(arr, n[, default])
```

**引数**

- `arr` — [配列](../data-types/array.md)。
- `n` — シフトさせる要素の数。
- `default` — オプション。新しい要素のデフォルト値。

**返される値**

- 指定された要素数だけ左にシフトさせた配列。[配列](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], 2) as res;
```

結果:

``` text
┌─res───────────┐
│ [3,4,5,6,0,0] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], -2) as res;
```

結果:

``` text
┌─res───────────┐
│ [0,0,1,2,3,4] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6], 2, 42) as res;
```

結果:

``` text
┌─res─────────────┐
│ [3,4,5,6,42,42] │
└─────────────────┘
```

クエリ:

``` sql
SELECT arrayShiftLeft(['a','b','c','d','e','f'], 3, 'foo') as res;
```

結果:

``` text
┌─res─────────────────────────────┐
│ ['d','e','f','foo','foo','foo'] │
└─────────────────────────────────┘
```

クエリ:

``` sql
SELECT arrayShiftLeft([1,2,3,4,5,6] :: Array(UInt16), 2, 4242) as res;
```

結果:

``` text
┌─res─────────────────┐
│ [3,4,5,6,4242,4242] │
└─────────────────────┘
```

## arrayShiftRight

[配列](../data-types/array.md)を指定した要素数だけ右にシフトします。
新しい要素は提供された引数または配列要素型のデフォルト値で埋められます。
要素数が負の場合は、配列は左にシフトされます。

**構文**

``` sql
arrayShiftRight(arr, n[, default])
```

**引数**

- `arr` — [配列](../data-types/array.md)。
- `n` — シフトさせる要素の数。
- `default` — オプション。新しい要素のデフォルト値。

**返される値**

- 指定された要素数だけ右にシフトさせた配列。[配列](../data-types/array.md)。

**例**

クエリ:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], 2) as res;
```

結果:

``` text
┌─res───────────┐
│ [0,0,1,2,3,4] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], -2) as res;
```

結果:

``` text
┌─res───────────┐
│ [3,4,5,6,0,0] │
└───────────────┘
```

クエリ:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6], 2, 42) as res;
```

結果:

``` text
┌─res─────────────┐
│ [42,42,1,2,3,4] │
└─────────────────┘
```

クエリ:

``` sql
SELECT arrayShiftRight(['a','b','c','d','e','f'], 3, 'foo') as res;
```

結果:

``` text
┌─res─────────────────────────────┐
│ ['foo','foo','foo','a','b','c'] │
└─────────────────────────────────┘
```

クエリ:

``` sql
SELECT arrayShiftRight([1,2,3,4,5,6] :: Array(UInt16), 2, 4242) as res;
```

結果:

``` text
┌─res─────────────────┐
│ [4242,4242,1,2,3,4] │
└─────────────────────┘
```

## arrayRandomSample

関数`arrayRandomSample`は、入力配列からランダムに`samples`個の要素のサブセットを返します。`samples`が入力配列のサイズを超える場合、サンプルサイズは配列のサイズに制限されます。つまり、すべての配列要素が返されますが、順序は保証されません。この関数はフラットな配列とネストされた配列の両方を処理できます。

**構文**

```sql
arrayRandomSample(arr, samples)
```

**引数**

- `arr` — サンプル要素を抽出する入力配列。([Array(T)](../data-types/array.md))
- `samples` — ランダムサンプルに含める要素の数 ([UInt*](../data-types/int-uint.md))

**返される値**

- 入力配列からランダムにサンプルした要素の配列。[配列](../data-types/array.md)。

**例**

クエリ:

```sql
SELECT arrayRandomSample(['apple', 'banana', 'cherry', 'date'], 2) as res;
```

結果:

```
┌─res────────────────┐
│ ['cherry','apple'] │
└────────────────────┘
```

クエリ:

```sql
SELECT arrayRandomSample([[1, 2], [3, 4], [5, 6]], 2) as res;
```

結果:

```
┌─res───────────┐
│ [[3,4],[5,6]] │
└───────────────┘
```

クエリ:

```sql
SELECT arrayRandomSample([1, 2, 3], 5) as res;
```

結果:

```
┌─res─────┐
│ [3,1,2] │
└─────────┘
```

## 距離関数

サポートされているすべての関数は、[距離関数のドキュメント](../../sql-reference/functions/distance-functions.md)に記載されています。
