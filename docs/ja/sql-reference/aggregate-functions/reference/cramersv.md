---
slug: /ja/sql-reference/aggregate-functions/reference/cramersv
sidebar_position: 127
---

# cramersV

[Cramer's V](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V)（Cramer's phiとも呼ばれることがあります）は、テーブル内の2つの**カラム**間の関連性を測定する指標です。`cramersV`関数の結果は、（変数間に関連がないことに対応する）0から1までの範囲を取り、各値が他の値によって完全に決定される場合にのみ1に達します。これは、2つの変数間の関連性を、最大可能な変動の割合として見ることができます。

:::note
Cramer's V のバイアス修正バージョンについては、[cramersVBiasCorrected](./cramersvbiascorrected.md)を参照してください。
:::

**構文**

``` sql
cramersV(column1, column2)
```

**パラメータ**

- `column1`: 比較する最初のカラム。
- `column2`: 比較する2番目のカラム。

**返される値**

- 0（カラムの値間に関連がないことに対応）から1（完全な関連性）までの値。

タイプ: 常に [Float64](../../../sql-reference/data-types/float.md)。

**例**

以下の2つの**カラム**は互いに関連がないため、`cramersV`の結果は0です。

クエリ:

``` sql
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 3 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    );
```

結果:

```response
┌─cramersV(a, b)─┐
│              0 │
└────────────────┘
```

以下の2つの**カラム**はかなり密接に関連しているため、`cramersV`の結果は高い値になります。

```sql
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    );
```

結果:

```response
┌─────cramersV(a, b)─┐
│ 0.8944271909999159 │
└────────────────────┘
```
