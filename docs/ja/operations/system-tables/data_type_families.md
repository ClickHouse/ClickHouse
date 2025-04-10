---
slug: /ja/operations/system-tables/data_type_families
---
# data_type_families

サポートされている[データ型](../../sql-reference/data-types/index.md)に関する情報が含まれています。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — データ型の名前。
- `case_insensitive` ([UInt8](../../sql-reference/data-types/int-uint.md)) — データ型の名前をクエリ内で大文字小文字を区別せずに使用できるかどうかを示すプロパティ。例えば、`Date` と `date` はどちらも有効です。
- `alias_to` ([String](../../sql-reference/data-types/string.md)) — `name`がエイリアスとなっているデータ型の名前。

**例**

``` sql
SELECT * FROM system.data_type_families WHERE alias_to = 'String'
```

``` text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```

**関連項目**

- [構文](../../sql-reference/syntax.md) — サポートされている構文に関する情報。
