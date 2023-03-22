---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "\u30D3\u30C3\u30C8"
---

# ビット関数 {#bit-functions}

ビット関数は、UInt8、UInt16、UInt32、UInt64、Int8、Int16、Int32、Int64、Float32、Float64のいずれかの型のペアに対しても機能します。

結果の型は、引数の最大ビットに等しいビットを持つ整数です。 引数のうち少なくともいずれかが署名されている場合、結果は符号付き番号になります。 引数が浮動小数点数の場合は、Int64にキャストされます。

## ビタン(a,b) {#bitanda-b}

## ビター(a,b) {#bitora-b}

## bitXor(a,b) {#bitxora-b}

## bitNot(a) {#bitnota}

## ビットシフトレフト(a,b) {#bitshiftlefta-b}

## ビットシフトライト(a,b) {#bitshiftrighta-b}

## ビットロタテレフト(a,b) {#bitrotatelefta-b}

## ビットロータライト(a,b) {#bitrotaterighta-b}

## bitTest {#bittest}

間の任意の整数に換算しています。 [バイナリ形式](https://en.wikipedia.org/wiki/Binary_number),指定された位置にあるビットの値を返します。 カウントダウンは右から左に0から始まります。

**構文**

``` sql
SELECT bitTest(number, index)
```

**パラメータ**

-   `number` – integer number.
-   `index` – position of bit.

**戻り値**

指定された位置にあるbitの値を返します。

タイプ: `UInt8`.

**例**

たとえば、基数43-2(二進数)の数値システムでは101011です。

クエリ:

``` sql
SELECT bitTest(43, 1)
```

結果:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

別の例:

クエリ:

``` sql
SELECT bitTest(43, 2)
```

結果:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## ビッテスタール {#bittestall}

の結果を返します [論理結合](https://en.wikipedia.org/wiki/Logical_conjunction) 指定された位置にあるすべてのビットの(and演算子)。 カウントダウンは右から左に0から始まります。

ビットごとの演算のための結合:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**構文**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**パラメータ**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`)は、すべての位置が真である場合にのみtrueです (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**戻り値**

論理結合の結果を返します。

タイプ: `UInt8`.

**例**

たとえば、基数43-2(二進数)の数値システムでは101011です。

クエリ:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

結果:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

別の例:

クエリ:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

結果:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## ビッテスタニ {#bittestany}

の結果を返します [論理和](https://en.wikipedia.org/wiki/Logical_disjunction) 指定された位置にあるすべてのビットの（または演算子）。 カウントダウンは右から左に0から始まります。

ビットごとの演算の分離:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**構文**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**パラメータ**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit.

**戻り値**

論理解釈の結果を返します。

タイプ: `UInt8`.

**例**

たとえば、基数43-2(二進数)の数値システムでは101011です。

クエリ:

``` sql
SELECT bitTestAny(43, 0, 2)
```

結果:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

別の例:

クエリ:

``` sql
SELECT bitTestAny(43, 4, 2)
```

結果:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## ビット数 {#bitcount}

数値のバイナリ表現で一つに設定されたビット数を計算します。

**構文**

``` sql
bitCount(x)
```

**パラメータ**

-   `x` — [整数](../../sql-reference/data-types/int-uint.md) または [浮動小数点数](../../sql-reference/data-types/float.md) 番号 この関数は、メモリ内の値表現を使用します。 浮動小数点数をサポートできます。

**戻り値**

-   入力番号の一つに設定されたビット数。

この関数は、入力値をより大きな型に変換しません ([符号拡張](https://en.wikipedia.org/wiki/Sign_extension)). 例えば, `bitCount(toUInt8(-1)) = 8`.

タイプ: `UInt8`.

**例**

たとえば、数333を取る。 そのバイナリ表現：00000000101001101。

クエリ:

``` sql
SELECT bitCount(333)
```

結果:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

[元の記事](https://clickhouse.com/docs/en/query_language/functions/bit_functions/) <!--hide-->
