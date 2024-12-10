---
slug: /ja/sql-reference/aggregate-functions/reference/groupbitand
sidebar_position: 147
---

# groupBitAnd

一連の数値に対してビット単位の`AND`を適用します。

``` sql
groupBitAnd(expr)
```

**引数**

`expr` – `UInt*`または`Int*`型の結果となる式。

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
SELECT groupBitAnd(num) FROM t
```

ここで、`num`はテストデータを含むカラムです。

結果:

``` text
binary     decimal
00000100 = 4
```
