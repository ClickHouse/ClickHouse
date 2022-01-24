---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "\u7B97\u8853"
---

# 算術関数 {#arithmetic-functions}

すべての算術関数について、結果の型は、そのような型がある場合、結果が収まる最小の数値型として計算されます。 最小値は、ビット数、署名されているかどうか、および浮動小数点数に基づいて同時に取得されます。 十分なビットがない場合は、最も高いビットタイプが取られます。

例:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

算術関数は、UInt8、UInt16、UInt32、UInt64、Int8、Int16、Int32、Int64、Float32、Float64のいずれかの型のペアに対しても機能します。

オーバーフローは、C++と同じ方法で生成されます。

## プラス（a、b）、a+b演算子 {#plusa-b-a-b-operator}

数値の合計を計算します。
また、日付または日付と時刻を持つ整数を追加することもできます。 日付の場合、整数を追加することは、対応する日数を追加することを意味します。 日付と時刻の場合は、対応する秒数を加算することを意味します。

## マイナス(a,b),a-b演算子 {#minusa-b-a-b-operator}

差を計算します。 結果は常に署名されます。

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## 乗算(a,b),a\*b演算子 {#multiplya-b-a-b-operator}

数値の積を計算します。

## 除算(a,b)、a/b演算子 {#dividea-b-a-b-operator}

数値の商を計算します。 結果の型は常に浮動小数点型です。
整数除算ではありません。 整数除算の場合は、 ‘intDiv’ 機能。
ゼロで割ると ‘inf’, ‘-inf’,または ‘nan’.

## intDiv(a,b) {#intdiva-b}

数値の商を計算します。 整数に分割し、（絶対値で）丸めます。
例外は、ゼロで除算するとき、または最小の負の数をマイナスで除算するときにスローされます。

## intDivOrZero(a,b) {#intdivorzeroa-b}

とは異なる ‘intDiv’ ゼロで除算するとき、または最小の負の数をマイナスで除算するときにゼロを返します。

## modulo(a,b),a%b演算子 {#moduloa-b-a-b-operator}

除算後の剰余を計算します。
引数が浮動小数点数の場合は、小数点部分を削除することによって整数に事前変換されます。
残りはC++と同じ意味で取られます。 切り捨て除算は、負の数に使用されます。
例外は、ゼロで除算するとき、または最小の負の数をマイナスで除算するときにスローされます。

## moduloOrZero(a,b) {#moduloorzeroa-b}

とは異なる ‘modulo’ 除数がゼロのときにゼロを返すという点で。

## 否定(a),-演算子 {#negatea-a-operator}

逆符号で数値を計算します。 結果は常に署名されます。

## abs(a) {#arithm_func-abs}

数値（ａ）の絶対値を算出する。 つまり、a\<0の場合、-aを返します。 符号付き整数型の場合、符号なしの数値を返します。

## gcd(a,b) {#gcda-b}

数値の最大公約数を返します。
例外は、ゼロで除算するとき、または最小の負の数をマイナスで除算するときにスローされます。

## lcm(a,b) {#lcma-b}

数値の最小公倍数を返します。
例外は、ゼロで除算するとき、または最小の負の数をマイナスで除算するときにスローされます。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
