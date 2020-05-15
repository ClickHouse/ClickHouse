---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 48
toc_title: "\u30D3\u30C3\u30C8"
---

# ビット機能 {#bit-functions}

ビット関数は、uint8、uint16、uint32、uint64、int8、int16、int32、int64、float32、またはfloat64のいずれかの種類のペアで機能します。

結果の型は、その引数の最大ビットに等しいビットを持つ整数です。 引数のうち少なくとも一方が署名されている場合、結果は署名された番号になります。 引数が浮動小数点数の場合、int64にキャストされます。

## bitAnd(a,b) {#bitanda-b}

## bitOr(a,b) {#bitora-b}

## bitXor(a,b) {#bitxora-b}

## bitNot(a) {#bitnota}

## ビットシフトレフト(a,b) {#bitshiftlefta-b}

## ビットシフトライト(a,b) {#bitshiftrighta-b}

## bitRotateLeft(a,b) {#bitrotatelefta-b}

## bitRotateRight(a,b) {#bitrotaterighta-b}

## 適者生存 {#bittest}

任意の整数を受け取り、それを [バイナリ形式](https://en.wikipedia.org/wiki/Binary_number)、指定された位置にあるビットの値を返します。 カウントダウンは右から左に0から始まります。

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

**例えば**

たとえば、ベース43（バイナリ）数値システムの数値は101011です。

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

## bitTestAll {#bittestall}

の結果を返します [論理結合](https://en.wikipedia.org/wiki/Logical_conjunction) 与えられた位置にあるすべてのビットの（and演算子）。 カウントダウンは右から左に0から始まります。

ビット演算のためのconjuction:

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
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4` すべてのポジションがtrueの場合にのみtrue (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**戻り値**

論理conjuctionの結果を返します。

タイプ: `UInt8`.

**例えば**

たとえば、ベース43（バイナリ）数値システムの数値は101011です。

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

## bitTestAny {#bittestany}

の結果を返します [論理和](https://en.wikipedia.org/wiki/Logical_disjunction) 与えられた位置にあるすべてのビットの（または演算子）。 カウントダウンは右から左に0から始まります。

ビットごとの操作のための分離:

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

論理disjuctionの結果を返します。

タイプ: `UInt8`.

**例えば**

たとえば、ベース43（バイナリ）数値システムの数値は101011です。

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

数値のバイナリ表現で設定されたビット数を計算します。

**構文**

``` sql
bitCount(x)
```

**パラメータ**

-   `x` — [整数](../../sql-reference/data-types/int-uint.md) または [浮動小数点](../../sql-reference/data-types/float.md) 番号 この関数は、メモリ内の値表現を使用します。 浮動小数点数をサポートすることができます。

**戻り値**

-   入力番号内のビット数を一つに設定します。

この関数は入力値を大きな型に変換しません ([記号の拡張子](https://en.wikipedia.org/wiki/Sign_extension)). 例えば, `bitCount(toUInt8(-1)) = 8`.

タイプ: `UInt8`.

**例えば**

例えば、数333を取ります。 そのバイナリ表現：00000000101001101。

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

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
