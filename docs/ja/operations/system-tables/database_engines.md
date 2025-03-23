---
slug: /ja/operations/system-tables/database_engines
---
# database_engines

サーバーでサポートされているデータベースエンジンのリストを含みます。

このテーブルには以下のカラムが含まれます（カラムの型は括弧内に示されています）:

- `name` (String) — データベースエンジンの名前。

例:

``` sql
SELECT *
FROM system.database_engines
WHERE name in ('Atomic', 'Lazy', 'Ordinary')
```

``` text
┌─name─────┐
│ Ordinary │
│ Atomic   │
│ Lazy     │
└──────────┘
```
