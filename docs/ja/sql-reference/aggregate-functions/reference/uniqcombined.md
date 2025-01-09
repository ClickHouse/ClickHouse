---
slug: /ja/sql-reference/aggregate-functions/reference/uniqcombined
sidebar_position: 205
---

# uniqCombined

異なる引数値の近似数を計算します。

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

`uniqCombined`関数は、異なる値の数を計算するのに適した選択です。

**引数**

- `HLL_precision`: [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) のセル数の2を底とする対数。省略可能で、`uniqCombined(x[, ...])`として関数を使用できます。`HLL_precision`のデフォルト値は17で、実質的には96 KiBのスペース（2^17セル、各セル6ビット）です。
- `X`: 可変個数のパラメータ。パラメータは`Tuple`、`Array`、`Date`、`DateTime`、`String`、または数値型を指定できます。

**戻り値**

- [UInt64](../../../sql-reference/data-types/int-uint.md)型の数値。

**実装の詳細**

`uniqCombined`関数は以下を行います:

- 集約内のすべてのパラメータに対して、ハッシュ（`String`は64ビットハッシュ、それ以外は32ビット）を計算し、そのハッシュを使用して計算を行います。
- 配列、ハッシュテーブル、エラー補正テーブル付きHyperLogLogの3つのアルゴリズムを組み合わせて使用します。
    - 異なる要素の数が少ない場合は、配列を使用します。
    - セットサイズが大きくなると、ハッシュテーブルを使用します。
    - より多くの要素の場合には、一定量のメモリを消費するHyperLogLogを使用します。
- 結果は決定論的に提供されます（クエリの処理順序に依存しません）。

:::note    
非`String`型に対して32ビットハッシュを使用するため、`UINT_MAX`を大きく超える基数の場合、結果の誤差が非常に高くなります（数十億の異なる値を超えたあたりから誤差が急激に上がります）。この場合は[uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)を使用すべきです。
:::

[uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)関数と比較して、`uniqCombined`関数は：

- メモリ消費が数倍少ないです。
- 精度が数倍高く計算されます。
- 通常、パフォーマンスがやや低いです。一部のシナリオでは、`uniqCombined`が`uniq`よりも優れたパフォーマンスを発揮することがあります。例えば、大量の集約状態をネットワーク経由で送信する分散クエリの場合です。

**例**

クエリ:

```sql
SELECT uniqCombined(number) FROM numbers(1e6);
```

結果:

```response
┌─uniqCombined(number)─┐
│              1001148 │ -- 1.00百万
└──────────────────────┘
```

より大きな入力の例として、`uniqCombined`と`uniqCombined64`の違いについては、[uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)の例のセクションを参照してください。

**関連項目**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
