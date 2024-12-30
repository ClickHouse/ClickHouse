---
slug: /ja/sql-reference/data-types/nullable
sidebar_position: 44
sidebar_label: Nullable(T)
---

# Nullable(T)

`T` が許可する通常の値に加えて、特別なマーカー ([NULL](../../sql-reference/syntax.md)) を格納できるようにします。これは「欠損値」を示します。例えば、`Nullable(Int8)` 型のカラムは `Int8` 型の値を格納することができ、値を持たない行は `NULL` を格納します。

`T` は、[Array](../../sql-reference/data-types/array.md)、[Map](../../sql-reference/data-types/map.md)、[Tuple](../../sql-reference/data-types/tuple.md) の複合データ型であってはならないが、複合データ型には `Nullable` 型の値を含めることができます。例えば `Array(Nullable(Int8))` です。

`Nullable` 型のフィールドは、テーブルのインデックスに含めることはできません。

`NULL` は、ClickHouse サーバーの設定で別途指定されない限り、どの `Nullable` 型においてもデフォルトの値です。

## ストレージの特性

ClickHouse は、テーブルのカラムに `Nullable` 型の値を格納するために、通常の値を格納するファイルに加えて `NULL` マスクのための別のファイルを使用します。マスクファイルのエントリにより、ClickHouse は各テーブル行の対応するデータ型のデフォルト値と `NULL` を区別することができます。この追加ファイルのため、`Nullable` カラムは類似の通常のカラムに比べて追加のストレージスペースを消費します。

:::note    
`Nullable` の使用はほとんど常にパフォーマンスに悪影響を与えますので、データベースを設計する際にはこれを念頭に置いてください。
:::

## NULLの検索

カラム全体を読み込むことなく、`null` サブカラムを使用して `NULL` 値を見つけることが可能です。対応する値が `NULL` の場合は `1` を返し、それ以外の場合は `0` を返します。

**例**

クエリ:

``` sql
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

結果:

``` text
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

## 使用例

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
