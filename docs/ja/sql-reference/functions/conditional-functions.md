---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: "\u6761\u4EF6\u4ED8\u304D "
---

# 条件関数 {#conditional-functions}

## もし {#if}

条件分岐を制御します。 と異なりほとんどのシステムClickHouse常に評価さの両方表現 `then` と `else`.

**構文**

``` sql
SELECT if(cond, then, else)
```

条件が `cond` ゼロ以外の値に評価され、式の結果を返します `then` 式の結果 `else`、存在する場合は、スキップされます。 もし `cond` ゼロまたは `NULL` の結果は `then` 式はスキップされ、 `else` expressionが存在する場合は、expressionが返されます。

**パラメータ**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` -条件が満たされた場合に返される式。
-   `else` -条件が満たされていない場合に返される式。

**戻り値**

この関数は実行されます `then` と `else` 式とは、条件かどうかに応じて、その結果を返します `cond` ゼロかどうかになってしまいました。

**例**

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

-   `then` と `else` 共通タイプが最も低い必要があります。

**例:**

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

注: `NULL` この例では値は使用されません。 [条件付きのNULL値](#null-values-in-conditionals) セクション

## 三項演算子 {#ternary-operator}

それは同じように働く `if` 機能。

構文: `cond ? then : else`

ﾂづｩﾂ。 `then` もし `cond` true(ゼロより大きい)と評価されます。 `else`.

-   `cond` の型でなければなりません `UInt8`,and `then` と `else` 共通タイプが最も低い必要があります。

-   `then` と `else` ことができます `NULL`

**も参照。**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## multif {#multiif}

あなたが書くことができます [CASE](../operators/index.md#operator_case) よりコンパクトにクエリ内の演算子。

構文: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**パラメータ:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

この関数は `2N+1` 変数。

**戻り値**

関数は、次のいずれかの値を返します `then_N` または `else`、条件によって `cond_N`.

**例**

再び使用する `LEFT_RIGHT` テーブル。

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

## 条件付きのNULL値 {#null-values-in-conditionals}

とき `NULL` 値は条件に含まれ、結果も次のようになります `NULL`.

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

その構築お問合せくの場合はタイプ `Nullable`.

次の例では、equals条件の追加に失敗してこれを示します `multiIf`.

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
