---
slug: /ja/sql-reference/aggregate-functions/reference/uniqcombined64
sidebar_position: 206
---

# uniqCombined64

異なる引数値の概算数を計算します。これは、すべてのデータ型に対して64ビットハッシュを使用する点を除けば、[uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)と同じです。

``` sql
uniqCombined64(HLL_precision)(x[, ...])
```

**パラメータ**

- `HLL_precision`: [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)のセル数の2進対数。オプションとして、関数を`uniqCombined64(x[, ...])`として使用できます。`HLL_precision`のデフォルト値は17で、実際には96 KiBのスペース（2^17セル、各6ビット）です。
- `X`: 可変数のパラメータ。パラメータには`Tuple`、`Array`、`Date`、`DateTime`、`String`、または数値型を使用できます。

**返される値**

- 数値型[UInt64](../../../sql-reference/data-types/int-uint.md)。

**実装の詳細**

`uniqCombined64`関数は以下のように動作します：
- 集約内のすべてのパラメータに対してハッシュ（すべてのデータ型に対して64ビットハッシュ）を計算し、それを計算に使用します。
- 3つのアルゴリズムの組み合わせを使用します：配列、ハッシュテーブル、エラー訂正テーブル付きのHyperLogLog。
    - 個別の要素が少数の場合、配列を使用します。
    - セットサイズが大きくなると、ハッシュテーブルを使用します。
    - より多くの要素がある場合、HyperLogLogが使用され、固定量のメモリを占有します。
- 結果は決定論的に提供されます（クエリ処理の順序に依存しません）。

:::note
すべての型に対して64ビットハッシュを使用するため、[uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md)が非`String`型に対して32ビットハッシュを使用している場合のように、`UINT_MAX`を大幅に超えるカーディナリティに対して非常に高い誤差が発生することはありません。
:::

[uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)関数と比較して、`uniqCombined64`関数は：

- 数倍少ないメモリを消費します。
- 数倍高い精度で計算します。

**例**

以下の例では、`uniqCombined64`が`1e10`の異なる数値に対して実行され、異なる引数値の数に非常に近い概算を返します。

クエリ:

```sql
SELECT uniqCombined64(number) FROM numbers(1e10);
```

結果:

```response
┌─uniqCombined64(number)─┐
│             9998568925 │ -- 10.00 billion
└────────────────────────┘
```

比較すると、`uniqCombined`関数はこのサイズの入力に対してかなり低い精度の概算を返します。

クエリ:

```sql
SELECT uniqCombined(number) FROM numbers(1e10);
```

結果:

```response
┌─uniqCombined(number)─┐
│           5545308725 │ -- 5.55 billion
└──────────────────────┘
```

**関連項目**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
