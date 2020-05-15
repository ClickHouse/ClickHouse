---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 43
toc_title: "\u6761\u4EF6\u4ED8\u304D "
---

# 条件関数 {#conditional-functions}

## もし {#if}

条件分岐を制御します。 と異なりほとんどのシステムclickhouse常に評価さの両方表現 `then` と `else`.

**構文**

``` sql
SELECT if(cond, then, else)
```

条件の場合 `cond` ゼロ以外の値として評価し、式の結果を返します `then`、および式の結果 `else`、存在する場合は、スキップされます。 この `cond` ゼロまたは `NULL` その後の結果 `then` 式はスキップされる。 `else` 式が存在する場合は、その式が返されます。

**パラメータ**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` -条件が満たされた場合に返される式。
-   `else` -条件が満たされていない場合に返される式。

**戻り値**

関数が実行されます `then` と `else` 式とその結果を返します。 `cond` ゼロかどうかに終わった。

**例えば**

クエリ:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

結果:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

クエリ:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

結果:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` と `else` 共通タイプが最も小さい。

**例えば:**

これを取る `LEFT_RIGHT` テーブル:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

次のクエリは比較します `left` と `right` 値:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is greater or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is greater or equal than left │
│    3 │     1 │ right is greater or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

メモ: `NULL` この例では値は使用されません。 [条件のnull値](#null-values-in-conditionals) セクション。

## 三項演算子 {#ternary-operator}

この作品と同じ `if` 機能。

構文: `cond ? then : else`

を返します `then` この `cond` true(ゼロより大きい)と評価され、それ以外の場合は `else`.

-   `cond` のタイプである必要があります `UInt8`、と `then` と `else` 共通タイプが最も小さい。

-   `then` と `else` できる。 `NULL`

**また見なさい**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## multif {#multiif}

あなたが書くことができます [CASE](../operators.md#operator_case) クエリでよりコンパクトに演算子。

構文: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**パラメータ:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

この関数は、 `2N+1` パラメータ。

**戻り値**

この関数は、いずれかの値を返します `then_N` または `else`、条件に応じて `cond_N`.

**例えば**

再度を使用して `LEFT_RIGHT` テーブル。

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## 条件付き結果を直接使用する {#using-conditional-results-directly}

条件は常に次のようになります `0`, `1` または `NULL`. できますので使用条件と結果が直接このような:

``` sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

## 条件のnull値 {#null-values-in-conditionals}

とき `NULL` 値は条件文に含まれ、結果は次のようになります `NULL`.

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

したがって、型が `Nullable`.

次の例は、equals条件を追加できないことを示しています `multiIf`.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
