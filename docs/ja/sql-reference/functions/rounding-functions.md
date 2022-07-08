---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u4E38\u3081"
---

# 丸め関数 {#rounding-functions}

## フロア(x\[,N\]) {#floorx-n}

以下の最大ラウンド数を返します `x`. ラウンド数は1/10Nの倍数、または1/10Nが正確でない場合は適切なデータ型の最も近い数値です。
‘N’ 整数定数、オプションのパラメーターです。 これは整数に丸めることを意味します。
‘N’ 負の場合があります。

例: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` 任意の数値型です。 結果は同じ型の数です。
整数引数の場合、負の値で丸めるのが理にかなっています `N` 値(負でない場合 `N`、関数は何もしません）。
丸めによってオーバーフローが発生した場合(たとえば、floor(-128,-1))、実装固有の結果が返されます。

## ceil(x\[,N\]),ceiling(x\[,N\]) {#ceilx-n-ceilingx-n}

以上の最小の丸め数を返します `x`. 他のすべての方法では、それはと同じです `floor` 関数（上記参照）。

## trunc(x\[,N\]),truncate(x\[,N\]) {#truncx-n-truncatex-n}

絶対値が以下の最大絶対値を持つ丸め数を返します `x`‘s. In every other way, it is the same as the ’floor’ 関数（上記参照）。

## round(x\[,N\]) {#rounding_functions-round}

指定した小数点以下の桁数に値を丸めます。

この関数は、指定された順序の最も近い番号を返します。 指定された数値が周囲の数値と等しい距離を持つ場合、この関数は浮動小数点数型に対してバンカーの丸めを使用し、他の数値型に対してはゼロから

``` sql
round(expression [, decimal_places])
```

**パラメータ:**

-   `expression` — A number to be rounded. Can be any [式](../syntax.md#syntax-expressions) 数値を返す [データ型](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   もし `decimal-places > 0` 次に、関数は値を小数点の右側に丸めます。
    -   もし `decimal-places < 0` 次に、この関数は値を小数点の左側に丸めます。
    -   もし `decimal-places = 0` 次に、この関数は値を整数に丸めます。 この場合、引数は省略できます。

**戻り値:**

入力番号と同じタイプの丸められた数値。

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

バンカーの丸め。

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**も参照。**

-   [ラウンドバンカー](#roundbankers)

## ラウンドバンカー {#roundbankers}

数値を指定した小数点以下の位置に丸めます。

-   丸め数が二つの数値の中間にある場合、関数は銀行家の丸めを使用します。

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   それ以外の場合、関数は数値を最も近い整数に丸めます。

銀行家の丸めを使用すると、丸めの数値がこれらの数値の合計または減算の結果に与える影響を減らすことができます。

たとえば、丸めが異なる合計値1.5、2.5、3.5、4.5などです:

-   丸めなし: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   バンカーの丸め: 2 + 2 + 4 + 4 = 12.
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

バンカーの丸め法によって丸められた値。

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

**銀行家の丸めの例**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**も参照。**

-   [丸](#rounding_functions-round)

## roundToExp2(num) {#roundtoexp2num}

番号を受け入れます。 数値が一つより小さい場合は、0を返します。 それ以外の場合は、数値を最も近い（負でない全体の）次数まで切り捨てます。

## roundDuration(num) {#rounddurationnum}

番号を受け入れます。 数値が一つより小さい場合は、0を返します。 それ以外の場合は、数値をセットの数値に切り捨てます: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. この機能はYandexに固有のものです。Metricaとセッションの長さに関するレポートの実装に使用されます。

## roundAge(num) {#roundagenum}

番号を受け入れます。 数値が18未満の場合は、0を返します。 それ以外の場合は、数値をセットの数値に切り捨てます: 18, 25, 35, 45, 55. この機能はYandexに固有のものです。ユーザー年齢に関するレポートを実装するために使用されます。

## ラウンドダウン(num,arr) {#rounddownnum-arr}

数値を受け取り、指定された配列内の要素に切り捨てます。 値が最低限界より小さい場合は、最低限界が返されます。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/rounding_functions/) <!--hide-->
