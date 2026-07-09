---
description: '条件関数に関するドキュメント'
sidebar_label: '条件'
slug: /sql-reference/functions/conditional-functions
title: '条件関数'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

<div id="using-conditional-results-directly">
  ### 条件式の結果を直接使う
</div>

条件式の結果は常に `0`、`1`、または `NULL` です。そのため、次のように条件式の結果を直接使えます。

```sql
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

<div id="null-values-in-conditionals">
  ### 条件式での NULL 値
</div>

条件式で `NULL` 値が使われる場合、結果も `NULL` になります。

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

したがって、型が `Nullable` の場合は、クエリを注意して組み立てる必要があります。

次の例では、`multiIf` に等値条件を追加しないとどうなるかを示しています。

```sql
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

<div id="case-statement">
  ### CASE ステートメント
</div>

ClickHouse の CASE 式は、SQL の CASE 演算子と同様の条件分岐を提供します。条件を評価し、最初に一致した条件に応じて値を返します。

ClickHouse では、CASE に 2 つの形式があります。

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   この形式は柔軟性が高く、内部的には [multiIf](/ja/sql-reference/functions/conditional-functions#multiIf) 関数で実装されています。各条件は独立して評価され、式には定数以外の値も含められます。

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   このより簡潔な形式は、定数値との照合向けに最適化されており、内部的には `caseWithExpression()` が使用されます。

たとえば、次のような記述が有効です。

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

この形式では、返す式が定数である必要もありません。

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### 注意点
</div>

ClickHouse は、CASE 式 (または `multiIf` などの内部的に等価な式) の結果型を、条件を評価する前に決定します。これは、返り値の式の型が異なる場合、たとえばタイムゾーンや数値型が異なる場合に重要です。

* 結果型は、すべての分岐の中で largest compatible type に基づいて選択されます。
* いったんこの型が選択されると、実行時にそのロジックが決して実行されない場合でも、ほかのすべての分岐は暗黙的にその型へ CAST されます。
* DateTime64 のようにタイムゾーンが型シグネチャの一部になっている型では、これにより予期しない動作が生じることがあります。ほかの分岐で別のタイムゾーンを指定していても、最初に現れたタイムゾーンがすべての分岐に使われることがあります。

たとえば、以下ではすべての行が、最初に一致した分岐のタイムゾーン、つまり `Asia/Kolkata` のタイムスタンプを返します。

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

ここでは、ClickHouse は複数の `DateTime64(3, <timezone>)` の戻り値型があると判断します。最初に見つかった `DateTime64(3, 'Asia/Kolkata'` を共通型として推論し、他の分岐を暗黙的にこの型へ CAST します。

これは、意図したタイムゾーンのフォーマットを保持するために、文字列に変換することで対処できます:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  以下のタグ内の内容は、ドキュメントフレームワークのビルド時に
  system.functions から生成されたドキュメントに差し替えられます。タグは変更または削除しないでください。
  参照: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }