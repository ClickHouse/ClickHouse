---
description: 'LIMIT 句のドキュメント'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'LIMIT 句'
doc_type: 'reference'
---

`LIMIT` 句は、クエリ結果として返される行数を制御します。

<div id="basic-syntax">
  ## 基本構文
</div>

**先頭の行を選択:**

```sql
LIMIT m
```

結果の先頭 `m` 行を返します。結果が `m` 行未満の場合は、すべてのレコードを返します。

**TOP の代替構文 (MS SQL Server 互換) :**

```sql
-- SELECT TOP number|percent column_name(s) FROM table_name
SELECT TOP 10 * FROM numbers(100);
SELECT TOP 0.1 * FROM numbers(100);
```

これは `LIMIT m` と同等で、Microsoft SQL Server のクエリとの互換性のために使用できます。

**OFFSET を指定した SELECT:**

```sql
LIMIT m OFFSET n
-- or equivalently:
LIMIT n, m
```

先頭の `n` 行をスキップし、続く `m` 行を返します。

どちらの形式でも、`n` と `m` には 0 以上の整数を指定する必要があります。

<div id="negative-limits">
  ## 負の LIMIT
</div>

負の値を使って、結果セットの*末尾*から行を選択します。

| 構文                   | 結果                         |
| -------------------- | -------------------------- |
| `LIMIT -m`           | 末尾の `m` 行                  |
| `LIMIT -m OFFSET -n` | 末尾の `n` 行をスキップした後の末尾 `m` 行 |
| `LIMIT m OFFSET -n`  | 末尾の `n` 行をスキップした後の先頭 `m` 行 |
| `LIMIT -m OFFSET n`  | 先頭の `n` 行をスキップした後の末尾 `m` 行 |

`LIMIT -n, -m` 構文は `LIMIT -m OFFSET -n` と同等です。

<div id="fractional-limits">
  ## 小数による制限
</div>

0 から 1 の間の小数値を使用して、行の一定割合を選択できます。

| 構文                      | 結果                                |
| ----------------------- | --------------------------------- |
| `LIMIT 0.1`             | 先頭 10% の行                         |
| `LIMIT 1 OFFSET 0.5`    | 中央の行                              |
| `LIMIT 0.25 OFFSET 0.5` | 第3四分位数 (先頭 50% をスキップした後の 25% の行)  |

:::note

* 小数は、0 より大きく 1 より小さい [Float64](../../data-types/float.md) 値である必要があります。
* 小数で指定した行数は、次の整数に切り上げられます。
  :::

<div id="combining-limit-types">
  ## LIMIT の種類を組み合わせる
</div>

通常の整数と、小数または負のオフセットを組み合わせて使用できます。

```sql
LIMIT 10 OFFSET 0.5    -- 10 rows starting from the halfway point
LIMIT 10 OFFSET -20    -- 10 rows after skipping the last 20
```

<div id="limit--with-ties-modifier">
  ## LIMIT ... WITH TIES
</div>

`WITH TIES` 修飾子を使用すると、LIMIT の最後の行と同じ `ORDER BY` 値を持つ行も追加で含まれます。

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

`WITH TIES` を使用すると、最後の値と同じ値を持つすべての行が含まれます。

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0, 5 WITH TIES
```

```response
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

6行目は、5行目と同じ値 (`2`) であるため含まれます。

`OFFSET` キーワードを使ってオフセットを指定した場合も同様です。

```sql
SELECT * FROM (
    SELECT number % 50 AS n FROM numbers(100)
) ORDER BY n LIMIT 3 OFFSET 2 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

先頭の 2 行をスキップして 3 行を取得すると、通常は `1, 1, 2` が返されますが、最後の行と同順位のため、2 つ目の `2` も含まれます。

`WITH TIES` は、負の LIMIT 値やオフセットでも機能します。最初に選択された行と同じ `ORDER BY` の値を持つ行も追加で含まれます。

```sql
SELECT number % 3 AS n FROM numbers(15)
ORDER BY n LIMIT -4 OFFSET -3 WITH TIES
```

```response
┌─n─┐
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

`WITH TIES` がない場合、結果は `1, 1, 2, 2` になります。`WITH TIES` がある場合は、最初に選択された行と同順位になるため、値 `1` の行がさらに 3 行含まれます。

この修飾子は、[`ORDER BY ... WITH FILL`](/ja/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier) 修飾子と組み合わせて使用できます。

<div id="considerations">
  ## 注意事項
</div>

**非決定論的な結果:** [`ORDER BY`](../../../sql-reference/statements/select/order-by.md) 句がない場合、返される行は任意の順序となる可能性があり、クエリの実行ごとに異なることがあります。

**サーバー側の制限:** 返される行数は、[limit](../../../operations/settings/settings.md#limit) 設定によって制限される場合もあります。

<div id="see-also">
  ## 関連項目
</div>

* [LIMIT BY](/ja/sql-reference/statements/select/limit-by) — 値のグループごとに行数を制限します。各カテゴリ内の上位 N 件を取得する場合に便利です。