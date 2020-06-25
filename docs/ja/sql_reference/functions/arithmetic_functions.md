---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 35
toc_title: "\u7B97\u6570"
---

# 算術関数 {#arithmetic-functions}

すべての算術関数の場合、結果の型は、そのような型がある場合、結果が収まる最小の数値型として計算されます。 最小値は、ビット数、符号付きかどうか、および浮動小数点かどうかに基づいて同時に取得されます。 十分なビットがない場合、最高のビットタイプが取られます。

例えば:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

算術関数は、uint8、uint16、uint32、uint64、int8、int16、int32、int64、float32、またはfloat64の任意のペアの型に対して機能します。

オーバーフローはc++と同じ方法で生成されます。

## プラス（a、b）、a+b演算子 {#plusa-b-a-b-operator}

数値の合計を計算します。
また、日付または日付と時刻を持つ整数を追加することができます。 日付の場合、整数を追加すると、対応する日数を追加することを意味します。 時間のある日付の場合、対応する秒数を加算することを意味します。

## マイナス(a,b),a-b演算子 {#minusa-b-a-b-operator}

差を計算します。 結果は常に署名されます。

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## 乗算(a,b),a\*b演算子 {#multiplya-b-a-b-operator}

数字の積を計算します。

## 除算(a,b),a/b演算子 {#dividea-b-a-b-operator}

数値の商を計算します。 結果の型は常に浮動小数点型です。
整数除算ではありません。 整数除算の場合は、以下を使用します ‘intDiv’ 機能。
ゼロで割ると ‘inf’, ‘-inf’、または ‘nan’.

## intDiv(a,b) {#intdiva-b}

数値の商を計算します。 整数に分割し、（絶対値によって）切り捨てます。
ゼロで除算するとき、または最小の負の数をマイナスの数で除算するときに例外がスローされます。

## intDivOrZero(a,b) {#intdivorzeroa-b}

とは異なります ‘intDiv’ ゼロで除算するとき、または最小の負の数をマイナスの数で除算するときにゼロを返すという点で。

## モジュロ(a,b),a%b演算子 {#moduloa-b-a-b-operator}

除算の後の剰余を計算します。
引数が浮動小数点数である場合、それらは小数部分を削除することによって整数にあらかじめ変換されます。
残りはc++と同じ意味で取られます。 負の数には切り捨て除算が使用されます。
ゼロで除算するとき、または最小の負の数をマイナスの数で除算するときに例外がスローされます。

## モジュロオルゼロ(a,b) {#moduloorzeroa-b}

とは異なります ‘modulo’ 除数がゼロのときにゼロを返すという点です。

## 否定(a),-演算子 {#negatea-a-operator}

逆符号を持つ数値を計算します。 結果は常に署名されます。

## abs(a) {#arithm_func-abs}

数値(a)の絶対値を計算します。 つまり、a\<0の場合、-a返します。 符号付き整数型の場合は、符号なしの数値を返します。

## gcd(a,b) {#gcda-b}

数値の最大公約数を返します。
ゼロで除算するとき、または最小の負の数をマイナスの数で除算するときに例外がスローされます。

## lcm(a,b) {#lcma-b}

数値の最小公倍数を返します。
ゼロで除算するとき、または最小の負の数をマイナスの数で除算するときに例外がスローされます。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
