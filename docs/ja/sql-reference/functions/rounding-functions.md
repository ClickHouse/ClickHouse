---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: "\u4E38\u3081"
---

# 丸め関数 {#rounding-functions}

## 床(x\[,N\]) {#floorx-n}

次の値以下の最大ラウンド数を返します `x`. ラウンド数は1/10Nの倍数、または1/10Nが正確でない場合は適切なデータ型の最も近い数です。
‘N’ 整数定数、オプションのパラメータです。 デフォルトではゼロで、整数に丸めることを意味します。
‘N’ 負の可能性があります。

例: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` 任意の数値型です。 結果は同じタイプの数です。
整数引数の場合、負の値で丸めるのが理にかなっています `N` 値(非負の場合 `N`、関数は何もしません）。
丸めによってオーバーフロー(たとえば、floor(-128,-1))が発生した場合は、実装固有の結果が返されます。

## ceil(x\[,N\]),ceiling(x\[,N\]) {#ceilx-n-ceilingx-n}

次の値以上の最小の丸め数を返します `x`. 他のすべての方法では、それはと同じです `floor` 関数(上記参照)。

## trunc(x\[,N\]),truncate(x\[,N\]) {#truncx-n-truncatex-n}

絶対値が以下の最大絶対値を持つラウンド数を返します `x`‘s. In every other way, it is the same as the ’floor’ 関数(上記参照)。

## ラウンド(x\[,N\]) {#rounding_functions-round}

指定した小数点以下の桁数に値を丸めます。

この関数は、指定された順序の最も近い番号を返します。 与えられた数が周囲の数と等しい距離を持つ場合、関数は浮動小数点数型に対してバンカーの丸めを使用し、他の数値型に対してゼロから丸めます。

``` sql
round(expression [, decimal_places])
```

**パラメータ:**

-   `expression` — A number to be rounded. Can be any [式](../syntax.md#syntax-expressions) 数値を返す [データ型](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   もし `decimal-places > 0` 次に、この関数は値を小数点の右側に丸めます。
    -   もし `decimal-places < 0` 次に、この関数は値を小数点の左側に丸めます。
    -   もし `decimal-places = 0` 次に、この関数は値を整数に丸めます。 この場合、引数は省略できます。

**戻り値:**

入力番号と同じタイプの丸められた数。

### 例 {#examples}

**使用例**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**丸めの例**

最も近い数値に丸めます。

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

銀行の丸め。

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**また見なさい**

-   [ラウンドバンカー](#roundbankers)

## ラウンドバンカー {#roundbankers}

数値を指定した小数点以下の桁数に丸めます。

-   丸め番号が二つの数字の中間にある場合、関数はバンカーの丸めを使用します。

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   それ以外の場合、関数は数値を最も近い整数に丸めます。

バンカーの丸めを使用すると、丸め数値がこれらの数値の加算または減算の結果に与える影響を減らすことができます。

たとえば、異なる丸めの合計1.5、2.5、3.5、4.5:

-   丸めなし: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   銀行の丸め: 2 + 2 + 4 + 4 = 12.
-   最も近い整数への丸め: 2 + 3 + 4 + 5 = 14.

**構文**

``` sql
roundBankers(expression [, decimal_places])
```

**パラメータ**

-   `expression` — A number to be rounded. Can be any [式](../syntax.md#syntax-expressions) 数値を返す [データ型](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**戻り値**

バンカーの丸めメソッドによって丸められた値。

### 例 {#examples-1}

**使用例**

クエリ:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

結果:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**銀行の丸めの例**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**また見なさい**

-   [ラウンド](#rounding_functions-round)

## roundToExp2(num) {#roundtoexp2num}

数値を受け取ります。 数値が小さい場合は0を返します。 それ以外の場合は、数値を最も近い（非負の全体）程度に丸めます。

## ラウンドデュレーション(num) {#rounddurationnum}

数値を受け取ります。 数値が小さい場合は0を返します。 それ以外の場合は、数値をセットから数値に切り下げます: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. この機能はyandexに固有です。metricaとセッションの長さに関するレポートを実装するために使用。

## roundAge(num) {#roundagenum}

数値を受け取ります。 数値が18未満の場合、0を返します。 それ以外の場合は、数値をセットから数値に切り下げます: 18, 25, 35, 45, 55. この機能はyandexに固有です。metricaとユーザーの年齢に関するレポートを実装するために使用。

## ラウンドダウン(num,arr) {#rounddownnum-arr}

数値を受け取り、指定した配列内の要素に切り捨てます。 値が下限より小さい場合は、下限が返されます。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
