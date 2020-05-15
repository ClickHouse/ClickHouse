---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 54
toc_title: Nullable
---

# Nullable(typename) {#data_type-nullable}

特別なマーカーを保存できます ([NULL](../../sql-reference/syntax.md))それは意味する “missing value” 通常の値と並んで `TypeName`. たとえば、 `Nullable(Int8)` タイプ列は保存できます `Int8` 値を入力すると、値を持たない行が格納されます `NULL`.

のための `TypeName` 複合データ型は使用できません [配列](array.md) と [タプル](tuple.md). 複合データタイプを含むことができ `Nullable` 以下のようなタイプの値 `Array(Nullable(Int8))`.

A `Nullable` typeフィールドできない含まれてテーブルスを作成します。

`NULL` anyのデフォルト値を指定します。 `Nullable` ClickHouseサーバー構成で別途指定されている場合を除き、入力します。

## ストレージ機能 {#storage-features}

保存する `Nullable` 型の値テーブルのカラムClickHouse用途別のファイル `NULL` 値を持つ通常のファイルに加えて、マスク。 マスクファイル内のエントリは、ClickHouseが `NULL` そして、各テーブルの行の対応するデータ型のデフォルト値。 追加のファイルのため, `Nullable` 列は、同様の通常の記憶領域と比較して追加の記憶領域を消費します。

!!! info "メモ"
    を使用して `Nullable` ほとんどの場合、パフォーマンスに悪影響を及ぼします。

## 使用例 {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
