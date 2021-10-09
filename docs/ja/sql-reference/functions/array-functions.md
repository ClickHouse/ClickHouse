---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u914D\u5217\u306E\u64CD\u4F5C"
---

# 配列を操作するための関数 {#functions-for-working-with-arrays}

## 空 {#function-empty}

空の配列の場合は1、空でない配列の場合は0を返します。
結果の型はUInt8です。
この関数は文字列に対しても機能します。

## ノーテンプティ {#function-notempty}

空の配列の場合は0、空でない配列の場合は1を返します。
結果の型はUInt8です。
この関数は文字列に対しても機能します。

## 長さ {#array_functions-length}

配列内の項目の数を返します。
結果の型はUInt64です。
この関数は文字列に対しても機能します。

## emptyArrayUInt8,emptyArrayUInt16,emptyArrayUInt32,emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8,emptyArrayInt16,emptyArrayInt32,emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32,emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate,emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

ゼロの引数を受け取り、適切な型の空の配列を返します。

## エンプティアライトシングル {#emptyarraytosingle}

空の配列を受け取り、既定値と等しい一要素配列を返します。

## 範囲(終了)、範囲(開始、終了\[、ステップ\]) {#rangeend-rangestart-end-step}

開始から終了-1までの数値の配列をステップごとに返します。
引数が `start` 既定値は0です。
引数が `step` 既定値は1です。
それはpythonicのように動作します `range`. しかし、違いは、すべての引数の型は `UInt` 数字だ
場合によっては、合計長が100,000,000を超える要素の配列がデータブロックに作成されると、例外がスローされます。

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

関数の引数から配列を作成します。
引数は定数であり、最小の共通型を持つ型を持つ必要があります。 それ以外の場合は、作成する配列の型が明確ではないためです。 つまり、この関数を使用して空の配列を作成することはできません（それを行うには、 ‘emptyArray\*’ 以上の機能）。
を返す ‘Array(T)’ "result"と入力します。 ‘T’ 渡された引数のうち最小の共通型です。

## arrayConcat {#arrayconcat}

引数として渡された配列を結合します。

``` sql
arrayConcat(arrays)
```

**パラメータ**

-   `arrays` – Arbitrary number of arguments of [配列](../../sql-reference/data-types/array.md) タイプ。
    **例**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## arrayElement(arr,n),演算子arr\[n\] {#arrayelementarr-n-operator-arrn}

インデックスを持つ要素を取得します `n` 配列から `arr`. `n` 任意の整数型である必要があります。
配列のインデックスは、配列から始まります。
負の索引がサポートされます。 この場合、末尾から番号付きの対応する要素を選択する。 例えば, `arr[-1]` 配列内の最後の項目です。

インデックスが配列の境界の外にある場合、デフォルト値（数値の場合は0、文字列の場合は空の文字列など）を返します。非定数配列と定数インデックス0の場合を除きます（この場合はエラーが発生します `Array indices are 1-based`).

## has(arr,elem) {#hasarr-elem}

チェックかどうか ‘arr’ 配列は ‘elem’ 要素。
要素が配列内にない場合は0を返し、要素が配列内にない場合は1を返します。

`NULL` 値として処理されます。

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## hasAll {#hasall}

ある配列が別の配列のサブセットかどうかを確認します。

``` sql
hasAll(set, subset)
```

**パラメータ**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**戻り値**

-   `1`,if `set` すべての要素が含まれています `subset`.
-   `0` そうでなければ

**独特の特性**

-   空の配列は、任意の配列のサブセットです。
-   `Null` 値として処理されます。
-   両方の配列の値の順序は重要ではありません。

**例**

`SELECT hasAll([], [])` 1を返します。

`SELECT hasAll([1, Null], [Null])` 1を返します。

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` 1を返します。

`SELECT hasAll(['a', 'b'], ['a'])` 1を返します。

`SELECT hasAll([1], ['a'])` 0を返します。

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` 0を返します。

## ハサニー {#hasany}

るかどうかを判二つの配列が互いの交差点にある。

``` sql
hasAny(array1, array2)
```

**パラメータ**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**戻り値**

-   `1`,if `array1` と `array2` 少なくとも一つの同様の要素があります。
-   `0` そうでなければ

**独特の特性**

-   `Null` 値として処理されます。
-   両方の配列の値の順序は重要ではありません。

**例**

`SELECT hasAny([1], [])` ﾂづｩﾂ。 `0`.

`SELECT hasAny([Null], [Null, 1])` ﾂづｩﾂ。 `1`.

`SELECT hasAny([-128, 1., 512], [1])` ﾂづｩﾂ。 `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` ﾂづｩﾂ。 `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` ﾂづｩﾂ。 `1`.

## インデックス(arr,x) {#indexofarr-x}

最初のインデックスを返します ‘x’ 配列内にある場合は要素（1から始まる）、そうでない場合は0。

例:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

に設定された要素 `NULL` 通常の値として処理されます。

## カウントイカル(arr,x) {#countequalarr-x}

Xと等しい配列内の要素の数を返します。arrayCount(elem-\>elem=x,arr)と同等です。

`NULL` 要素は個別の値として処理されます。

例:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## アレイン(arr) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

この関数は通常、配列結合で使用されます。 配列結合を適用した後、配列ごとに一度だけ何かを数えることができます。 例:

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

この例では、Reachはコンバージョン数(配列結合を適用した後に受信した文字列)、Hitsはページビュー数(配列結合の前の文字列)です。 この特定のケースでは、同じ結果を簡単に得ることができます:

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

この関数は、高次関数でも使用できます。 たとえば、条件に一致する要素の配列インデックスを取得するために使用できます。

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

ソース配列と同じサイズの配列を返します。
たとえば、arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

この関数は、配列結合と配列要素の集計を使用する場合に便利です。
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

この例では、各ゴールIDには、コンバージョン数(ゴールの入れ子になったデータ構造の各要素は、到達したゴールであり、これをコンバージョンと呼びます)とセッシ 配列結合がなければ、セッション数をsum(Sign)として数えました。 しかし、この特定のケースでは、行にネストされたGoals構造体が乗算されているため、この後に各セッションを一度カウントするために、arrayEnumerateUniq(Goals.ID）機能。

ArrayEnumerateUniq関数は、引数と同じサイズの複数の配列を取ることができます。 この場合、すべての配列の同じ位置にある要素のタプルに対して一意性が考慮されます。

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

これは、入れ子になったデータ構造で配列結合を使用し、この構造体内の複数の要素間でさらに集計する場合に必要です。

## arrayPopBack {#arraypopback}

配列から最後の項目を削除します。

``` sql
arrayPopBack(array)
```

**パラメータ**

-   `array` – Array.

**例**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront {#arraypopfront}

配列から最初の項目を削除します。

``` sql
arrayPopFront(array)
```

**パラメータ**

-   `array` – Array.

**例**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

配列の最後に項目を追加します。

``` sql
arrayPushBack(array, single_value)
```

**パラメータ**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` 配列のデータ型の型。 ClickHouseのデータの種類の詳細については、次を参照してください “[データ型](../../sql-reference/data-types/index.md#data_types)”. ことができます `NULL`. この関数は `NULL` 要素を配列に変換し、配列要素の型を次のように変換します `Nullable`.

**例**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## アレイプッシュフロント {#arraypushfront}

配列の先頭に要素を追加します。

``` sql
arrayPushFront(array, single_value)
```

**パラメータ**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` 配列のデータ型の型。 ClickHouseのデータの種類の詳細については、次を参照してください “[データ型](../../sql-reference/data-types/index.md#data_types)”. ことができます `NULL`. この関数は `NULL` 要素を配列に変換し、配列要素の型を次のように変換します `Nullable`.

**例**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

配列の長さを変更します。

``` sql
arrayResize(array, size[, extender])
```

**パラメータ:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   もし `size` 配列の元のサイズよりも小さい場合、配列は右から切り捨てられます。
-   もし `size` 配列の初期サイズよりも大きい場合、配列は次のように右に拡張されます `extender` 配列項目のデータ型の値または既定値。
-   `extender` — Value for extending an array. Can be `NULL`.

**戻り値:**

長さの配列 `size`.

**コールの例**

``` sql
SELECT arrayResize([1], 3)
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL)
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## アレイスライス {#arrayslice}

配列のスライスを返します。

``` sql
arraySlice(array, offset[, length])
```

**パラメータ**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` -必要なスライスの長さ。 負の値を指定すると、関数は開いているスライスを返します `[offset, array_length - length)`. 値を省略すると、関数はスライスを返します `[offset, the_end_of_array]`.

**例**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

配列要素に設定 `NULL` 通常の値として処理されます。

## arraySort(\[func,\] arr, …) {#array_functions-sort}

の要素を並べ替えます `arr` 昇順の配列。 もし `func` 関数が指定されると、並べ替え順序は `func` 配列の要素に適用される関数。 もし `func` 複数の引数を受け入れる。 `arraySort` 関数には、引数の配列が複数渡されます。 `func` に対応する。 詳しい例はの終わりに示されています `arraySort` 説明。

整数値ソートの例:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

文字列値のソートの例:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

次の並べ替え順序を考えてみましょう `NULL`, `NaN` と `Inf` 値:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` 値は配列の最初にあります。
-   `NULL` 値は配列の最後にあります。
-   `NaN` 値は直前です `NULL`.
-   `Inf` 値は直前です `NaN`.

なお `arraySort` は [高次関数](higher-order-functions.md). Lambda関数を最初の引数として渡すことができます。 この場合、並べ替え順序は、配列の要素に適用されたlambda関数の結果によって決定されます。

次の例を考えてみましょう:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` 関数は昇順にキーをソートし、結果は\[3、2、1\]です。 したがって、 `(x) –> -x` ラムダ関数は [降順](#array_functions-reverse-sort) ソートで。

Lambda関数は複数の引数を受け取ることができます。 この場合、 `arraySort` 関数lambda関数の引数が対応する同じ長さのいくつかの配列。 結果の配列は最初の入力配列の要素で構成され、次の入力配列の要素は並べ替えキーを指定します。 例えば:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

ここでは、二番目の配列(\[2,1\])に渡される要素は、ソース配列の対応する要素の並べ替えキーを定義します (\[‘hello’, ‘world’\]）、それは, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn't use `x`、ソース配列の実際の値は結果の順序に影響しません。 だから, ‘hello’ 結果の第二の要素になります。 ‘world’ 最初になります。

その他の例を以下に示す。

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

!!! note "注"
    効率を分類することを改善するため、 [シュワルツ変換](https://en.wikipedia.org/wiki/Schwartzian_transform) が使用される。

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

の要素を並べ替えます `arr` 降順の配列。 もし `func` 関数を指定します, `arr` の結果に従ってソートされます。 `func` 関数は、配列の要素に適用され、その後、ソートされた配列が逆になります。 もし `func` 複数の引数を受け入れる。 `arrayReverseSort` 関数には、引数の配列が複数渡されます。 `func` に対応する。 詳しい例はの終わりに示されています `arrayReverseSort` 説明。

整数値ソートの例:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

文字列値のソートの例:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

次の並べ替え順序を考えてみましょう `NULL`, `NaN` と `Inf` 値:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` 値は配列の最初にあります。
-   `NULL` 値は配列の最後にあります。
-   `NaN` 値は直前です `NULL`.
-   `-Inf` 値は直前です `NaN`.

なお、 `arrayReverseSort` は [高次関数](higher-order-functions.md). Lambda関数を最初の引数として渡すことができます。 以下に例を示す。

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

配列は次のように並べ替えられます:

1.  最初に、ソース配列(\[1,2,3\])は、配列の要素に適用されたlambda関数の結果に従ってソートされます。 結果は配列\[3,2,1\]です。
2.  前のステップで取得された配列は、逆になります。 したがって、最終的な結果は\[1,2,3\]です。

Lambda関数は複数の引数を受け取ることができます。 この場合、 `arrayReverseSort` 関数lambda関数の引数が対応する同じ長さのいくつかの配列。 結果の配列は最初の入力配列の要素で構成され、次の入力配列の要素は並べ替えキーを指定します。 例えば:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

この例では、配列は次のように並べ替えられます:

1.  最初は、ソース配列 (\[‘hello’, ‘world’\])は、配列の要素に適用されたlambda関数の結果に従ってソートされます。 二番目の配列（\[2,1\]）に渡される要素は、ソース配列の対応する要素の並べ替えキーを定義します。 結果は配列です \[‘world’, ‘hello’\].
2.  前のステップでソートされた配列は、逆になります。 だから、最終的な結果は \[‘hello’, ‘world’\].

その他の例を以下に示す。

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

## arrayUniq(arr, …) {#arrayuniqarr}

一つの引数が渡されると、配列内の異なる要素の数がカウントされます。
複数の引数が渡された場合、複数の配列内の対応する位置にある要素の異なるタプルの数をカウントします。

配列内の一意の項目のリストを取得する場合は、arrayReduceを使用できます(‘groupUniqArray’,arr）。

## アレイジョイン(arr) {#array-functions-join}

特別な機能。 セクションを参照 [“ArrayJoin function”](array-join.md#functions_arrayjoin).

## arrayDifference {#arraydifference}

隣接する配列要素の差を計算します。 最初の要素が0になる配列を返します。 `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**構文**

``` sql
arrayDifference(array)
```

**パラメータ**

-   `array` – [配列](https://clickhouse.com/docs/en/data_types/array/).

**戻り値**

隣接する要素間の差分の配列を返します。

タイプ: [UInt\*](https://clickhouse.com/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.com/docs/en/data_types/int_uint/#int-ranges), [フロート\*](https://clickhouse.com/docs/en/data_types/float/).

**例**

クエリ:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

結果:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

結果の型Int64によるオーバーフローの例:

クエリ:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

結果:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## アレイディスト {#arraydistinct}

配列を受け取り、別個の要素のみを含む配列を返します。

**構文**

``` sql
arrayDistinct(array)
```

**パラメータ**

-   `array` – [配列](https://clickhouse.com/docs/en/data_types/array/).

**戻り値**

個別の要素を含む配列を返します。

**例**

クエリ:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

結果:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## アレイニュメラテンセ(arr) {#array_functions-arrayenumeratedense}

ソース配列と同じサイズの配列を返します。

例:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## アレイインターセクト(arr) {#array-functions-arrayintersect}

複数の配列を取り、すべてのソース配列に存在する要素を含む配列を返します。 結果の配列内の要素の順序は、最初の配列と同じです。

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

## arrayReduce {#arrayreduce}

集計関数を配列要素に適用し、その結果を返します。 集計関数の名前は、単一引quotesで文字列として渡されます `'max'`, `'sum'`. パラメトリック集計関数を使用する場合、パラメーターは関数名の後に括弧で示されます `'uniqUpTo(6)'`.

**構文**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**パラメータ**

-   `agg_func` — The name of an aggregate function which should be a constant [文字列](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [配列](../../sql-reference/data-types/array.md) 集計関数のパラメーターとして列を入力します。

**戻り値**

**例**

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

集計関数が複数の引数を取る場合、この関数は同じサイズの複数の配列に適用する必要があります。

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

パラメトリック集計関数の例:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceInRanges {#arrayreduceinranges}

指定された範囲の配列要素に集計関数を適用し、各範囲に対応する結果を含む配列を返します。 関数はmultipleと同じ結果を返します `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**構文**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**パラメータ**

-   `agg_func` — The name of an aggregate function which should be a constant [文字列](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [配列](../../sql-reference/data-types/array.md) の [タプル](../../sql-reference/data-types/tuple.md) これには、各範囲のインデックスと長さが含まれます。
-   `arr` — Any number of [配列](../../sql-reference/data-types/array.md) 集計関数のパラメーターとして列を入力します。

**戻り値**

**例**

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## アレイリバース(arr) {#arrayreverse}

逆の順序で要素を含む元の配列と同じサイズの配列を返します。

例:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## リバース(arr) {#array-functions-reverse}

の同義語 [“arrayReverse”](#arrayreverse)

## アレイフラッテン {#arrayflatten}

配列の配列をフラット配列に変換します。

関数:

-   ネストされた配列の任意の深さに適用されます。
-   既にフラットな配列は変更しません。

の平坦化された配列を含むすべての要素をすべてソース配列.

**構文**

``` sql
flatten(array_of_arrays)
```

別名: `flatten`.

**パラメータ**

-   `array_of_arrays` — [配列](../../sql-reference/data-types/array.md) 配列の。 例えば, `[[1,2,3], [4,5]]`.

**例**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## アレイコンパクト {#arraycompact}

配列から連続した重複要素を削除します。 結果の値の順序は、ソース配列の順序によって決まります。

**構文**

``` sql
arrayCompact(arr)
```

**パラメータ**

`arr` — The [配列](../../sql-reference/data-types/array.md) 検査する。

**戻り値**

重複のない配列。

タイプ: `Array`.

**例**

クエリ:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

結果:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

複数の配列を単一の配列に結合します。 結果の配列には、引数の順序で組にグループ化されたソース配列の対応する要素が含まれます。

**構文**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**パラメータ**

-   `arrN` — [配列](../data-types/array.md).

関数は、異なる型の任意の数の配列を取ることができます。 すべての入力配列は同じサイズでなければなりません。

**戻り値**

-   ソース配列の要素をグループ化した配列 [タプル](../data-types/tuple.md). タプル内のデータ型は、入力配列の型と同じで、配列が渡される順序と同じです。

タイプ: [配列](../data-types/array.md).

**例**

クエリ:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

結果:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## アレイオーク {#arrayauc}

計算AUC（機械学習の概念である曲線の下の面積、詳細を参照してください：https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve）。

**構文**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**パラメータ**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**戻り値**
FLOAT64型のAUC値を返します。

**例**
クエリ:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

結果:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[元の記事](https://clickhouse.com/docs/en/query_language/functions/array_functions/) <!--hide-->
