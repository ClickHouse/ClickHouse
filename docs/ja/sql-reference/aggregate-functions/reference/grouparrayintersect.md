---
slug: /ja/sql-reference/aggregate-functions/reference/grouparrayintersect
sidebar_position: 141
---

# groupArrayIntersect

与えられた配列の共通部分を返します（すべての配列に含まれるアイテムを返します）。

**構文**

``` sql
groupArrayIntersect(x)
```

**引数**

- `x` — 引数（カラム名や式）。

**戻り値**

- すべての配列に含まれる要素を持つ配列。

タイプ: [Array](../../data-types/array.md)。

**例**

テーブル `numbers` を考えます:

``` text
┌─a──────────────┐
│ [1,2,4]        │
│ [1,5,2,8,-1,0] │
│ [1,5,7,5,8,2]  │
└────────────────┘
```

カラム名を引数として使用したクエリ:

``` sql
SELECT groupArrayIntersect(a) as intersection FROM numbers;
```

結果:

```text
┌─intersection──────┐
│ [1, 2]            │
└───────────────────┘
```
