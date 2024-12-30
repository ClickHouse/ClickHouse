---
slug: /ja/sql-reference/aggregate-functions/reference/groupbitxor
sidebar_position: 153
---

# groupBitXor

一連の数値に対してビットごとの`XOR`を適用します。

``` sql
groupBitXor(expr)
```

**引数**

`expr` – `UInt*`または`Int*`型の結果をもたらす式。

**戻り値**

`UInt*`または`Int*`型の値。

**例**

テストデータ:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

クエリ:

``` sql
SELECT groupBitXor(num) FROM t
```

ここで、`num`はテストデータを持つカラムです。

結果:

``` text
binary     decimal
01101000 = 104
```
