---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u9AD8\u6B21"
---

# 高次関数 {#higher-order-functions}

## `->` 演算子,lambda(params,expr)関数 {#operator-lambdaparams-expr-function}

Allows describing a lambda function for passing to a higher-order function. The left side of the arrow has a formal parameter, which is any ID, or multiple formal parameters – any IDs in a tuple. The right side of the arrow has an expression that can use these formal parameters, as well as any table columns.

例: `x -> 2 * x, str -> str != Referer.`

高階関数は、関数の引数としてラムダ関数のみを受け入れることができます。

複数の引数を受け入れるlambda関数は、高次関数に渡すことができます。 この場合、高次関数には、これらの引数が対応する同じ長さのいくつかの配列が渡されます。

以下のような一部の機能については [arrayCount](#higher_order_functions-array-count) または [アレイサム](#higher_order_functions-array-count)、最初の引数（ラムダ関数）は省略することができます。 この場合、同一の写像が仮定される。

Lambda関数は、次の関数では省略できません:

-   [arrayMap](#higher_order_functions-array-map)
-   [arrayFilter](#higher_order_functions-array-filter)
-   [arrayFill](#higher_order_functions-array-fill)
-   [arrayReverseFill](#higher_order_functions-array-reverse-fill)
-   [arraySplit](#higher_order_functions-array-split)
-   [arrayReverseSplit](#higher_order_functions-array-reverse-split)
-   [arrayFirst](#higher_order_functions-array-first)
-   [arrayFirstIndex](#higher_order_functions-array-first-index)

### arrayMap(func, arr1, …) {#higher_order_functions-array-map}

の元のアプリケーションから取得した配列を返します。 `func` の各要素に対する関数 `arr` 配列

例:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

異なる配列から要素のタプルを作成する方法を次の例に示します:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayMap` 機能。

### arrayFilter(func, arr1, …) {#higher_order_functions-array-filter}

要素のみを含む配列を返します。 `arr1` そのために `func` 0以外のものを返します。

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

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayFilter` 機能。

### arrayFill(func, arr1, …) {#higher_order_functions-array-fill}

スキャンスルー `arr1` 最初の要素から最後の要素へと置き換えます `arr1[i]` によって `arr1[i - 1]` もし `func` 0を返します。 の最初の要素 `arr1` 交換されません。

例:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayFill` 機能。

### arrayReverseFill(func, arr1, …) {#higher_order_functions-array-reverse-fill}

スキャンスルー `arr1` 最後の要素から最初の要素へと置き換えます `arr1[i]` によって `arr1[i + 1]` もし `func` 0を返します。 の最後の要素 `arr1` 交換されません。

例:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayReverseFill` 機能。

### arraySplit(func, arr1, …) {#higher_order_functions-array-split}

分割 `arr1` 複数の配列に。 とき `func` 0以外のものを返すと、配列は要素の左側で分割されます。 配列は最初の要素の前に分割されません。

例:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arraySplit` 機能。

### arrayReverseSplit(func, arr1, …) {#higher_order_functions-array-reverse-split}

分割 `arr1` 複数の配列に。 とき `func` 0以外のものを返すと、配列は要素の右側に分割されます。 配列は最後の要素の後に分割されません。

例:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arraySplit` 機能。

### arrayCount(\[func,\] arr1, …) {#higher_order_functions-array-count}

Funcが0以外のものを返すarr配列内の要素の数を返します。 もし ‘func’ 指定されていない場合は、配列内のゼロ以外の要素の数を返します。

### arrayExists(\[func,\] arr1, …) {#arrayexistsfunc-arr1}

少なくとも一つの要素がある場合は1を返します ‘arr’ そのために ‘func’ 0以外のものを返します。 それ以外の場合は0を返します。

### arrayAll(\[func,\] arr1, …) {#arrayallfunc-arr1}

場合は1を返します ‘func’ 内のすべての要素に対して0以外のものを返します ‘arr’. それ以外の場合は0を返します。

### arraySum(\[func,\] arr1, …) {#higher-order-functions-array-sum}

の合計を返します。 ‘func’ 値。 関数を省略すると、配列要素の合計が返されます。

### arrayFirst(func, arr1, …) {#higher_order_functions-array-first}

の最初の要素を返します。 ‘arr1’ これに対する配列 ‘func’ 0以外のものを返します。

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayFirst` 機能。

### arrayFirstIndex(func, arr1, …) {#higher_order_functions-array-first-index}

の最初の要素のインデックスを返します。 ‘arr1’ これに対する配列 ‘func’ 0以外のものを返します。

最初の引数（ラムダ関数）は省略できないことに注意してください。 `arrayFirstIndex` 機能。

### arrayCumSum(\[func,\] arr1, …) {#arraycumsumfunc-arr1}

ソース配列内の要素の部分和(実行中の合計)の配列を返します。 もし `func` 関数が指定されると、配列要素の値は合計する前にこの関数によって変換されます。

例:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### arrayCumSumNonNegative(arr) {#arraycumsumnonnegativearr}

同じ `arrayCumSum`,ソース配列内の要素の部分和の配列を返します(実行中の合計)。 異なる `arrayCumSum` 返された値にゼロ未満の値が含まれている場合、値はゼロに置き換えられ、その後の計算はゼロパラメータで実行されます。 例えば:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### arraySort(\[func,\] arr1, …) {#arraysortfunc-arr1}

の要素を並べ替えた結果として配列を返します `arr1` 昇順で。 もし `func` 関数が指定され、ソート順序は関数の結果によって決定されます `func` 配列（配列）の要素に適用されます)

その [シュワルツ変換](https://en.wikipedia.org/wiki/Schwartzian_transform) 分類の効率を改善するのに使用されています。

例:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

詳細については、 `arraySort` 方法は、参照してください [配列を操作するための関数](array-functions.md#array_functions-sort) セクション

### arrayReverseSort(\[func,\] arr1, …) {#arrayreversesortfunc-arr1}

の要素を並べ替えた結果として配列を返します `arr1` 降順で。 もし `func` 関数が指定され、ソート順序は関数の結果によって決定されます `func` 配列（配列）の要素に適用されます。

例:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

詳細については、 `arrayReverseSort` 方法は、参照してください [配列を操作するための関数](array-functions.md#array_functions-reverse-sort) セクション

[元の記事](https://clickhouse.com/docs/en/query_language/functions/higher_order_functions/) <!--hide-->
