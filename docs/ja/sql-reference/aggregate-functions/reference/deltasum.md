---
slug: /ja/sql-reference/aggregate-functions/reference/deltasum
sidebar_position: 129
---

# deltaSum

連続する行の間の算術差を合計します。差が負の場合は無視されます。

:::note
この関数が正しく動作するためには、基になるデータがソートされている必要があります。この関数を[Materialized View](../../../sql-reference/statements/create/view.md#materialized)で使用したい場合、代わりに[deltaSumTimestamp](../../../sql-reference/aggregate-functions/reference/deltasumtimestamp.md#agg_functions-deltasumtimestamp)メソッドを使用することをお勧めします。
:::

**構文**

``` sql
deltaSum(value)
```

**引数**

- `value` — 入力値。型は[Integer](../../data-types/int-uint.md)または[Float](../../data-types/float.md)でなければなりません。

**返される値**

- 算術差の合計で、`Integer`または`Float`型です。

**例**

クエリ:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3]));
```

結果:

``` text
┌─deltaSum(arrayJoin([1, 2, 3]))─┐
│                              2 │
└────────────────────────────────┘
```

クエリ:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
```

結果:

``` text
┌─deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]))─┐
│                                             7 │
└───────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT deltaSum(arrayJoin([2.25, 3, 4.5]));
```

結果:

``` text
┌─deltaSum(arrayJoin([2.25, 3, 4.5]))─┐
│                                2.25 │
└─────────────────────────────────────┘
```

## 関連項目

- [runningDifference](../../functions/other-functions.md#other_functions-runningdifference)
