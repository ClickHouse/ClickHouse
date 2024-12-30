---
slug: /ja/sql-reference/aggregate-functions/reference/groupuniqarray
sidebar_position: 154
---

# groupUniqArray

構文: `groupUniqArray(x)` または `groupUniqArray(max_size)(x)`

異なる引数の値から配列を作成します。メモリ消費は [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md) 関数と同じです。

第二のバージョン（`max_size` パラメータ付き）は、結果の配列のサイズを `max_size` 要素に制限します。例えば、`groupUniqArray(1)(x)` は `[any(x)]` と同等です。
