---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Null\u53EF\u80FD"
---

# Nullable(型名) {#data_type-nullable}

できる特別マーカー ([NULL](../../sql-reference/syntax.md)）を表す。 “missing value” と共に正常値を許可する `TypeName`. たとえば、 `Nullable(Int8)` 型列が格納できます `Int8` 値を入力し、値を持たない行には格納されます `NULL`.

のために `TypeName` 複合データ型は使用できません [配列](array.md) と [タプル](tuple.md). 複合データ型には `Nullable` 次のような型の値 `Array(Nullable(Int8))`.

A `Nullable` typeフィールドできない含まれてテーブルスを作成します。

`NULL` のデフォルト値です `Nullable` ClickHouseサーバー構成で特に指定がない限り、入力します。

## ストレージ機能 {#storage-features}

保存するには `Nullable` テーブルの列に値を入力すると、ClickHouseは別のファイルを使用します `NULL` 値を持つ通常のファイルに加えて、マスク。 マスクファイル内のエントリはClickHouseが `NULL` テーブル行ごとに対応するデータ型のデフォルト値。 追加のファイルのために, `Nullable` columnは、同様の通常のものと比較して追加の記憶領域を消費します。

!!! info "注"
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

[元の記事](https://clickhouse.com/docs/en/data_types/nullable/) <!--hide-->
