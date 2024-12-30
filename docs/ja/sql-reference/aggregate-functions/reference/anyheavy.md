---
slug: /ja/sql-reference/aggregate-functions/reference/anyheavy
sidebar_position: 104
---

# anyHeavy

[heavy hitters](https://doi.org/10.1145/762471.762473) アルゴリズムを使用して頻繁に出現する値を選択します。クエリの各実行スレッドで半分以上のケースで発生する値がある場合、その値が返されます。通常、結果は非決定的です。

``` sql
anyHeavy(column)
```

**引数**

- `column` – カラム名。

**例**

[OnTime](../../../getting-started/example-datasets/ontime.md) データセットを使用して、`AirlineID` カラム内で頻繁に出現する値を選択します。

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```
