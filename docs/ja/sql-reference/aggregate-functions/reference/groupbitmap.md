---
slug: /ja/sql-reference/aggregate-functions/reference/groupbitmap
sidebar_position: 148
---

# groupBitmap

ビットマップまたは符号なし整数カラムからの集計計算を行い、`UInt64`型の基数を返します。`-State`のサフィックスを追加すると、[ビットマップオブジェクト](../../../sql-reference/functions/bitmap-functions.md)を返します。

``` sql
groupBitmap(expr)
```

**引数**

`expr` – `UInt*` 型の結果をもたらす式。

**戻り値**

`UInt64` 型の値。

**例**

テストデータ:

``` text
UserID
1
1
2
3
```

クエリ:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

結果:

``` text
num
3
```
