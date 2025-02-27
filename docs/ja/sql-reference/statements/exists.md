---
slug: /ja/sql-reference/statements/exists
sidebar_position: 45
sidebar_label: EXISTS
---

# EXISTS ステートメント

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

このステートメントは `UInt8` 型の単一カラムを返します。指定されたデータベースにテーブルまたはデータベースが存在しない場合は値 `0` が、存在する場合は値 `1` が格納されます。
