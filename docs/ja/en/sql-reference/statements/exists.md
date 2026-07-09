---
description: 'EXISTS ステートメントのドキュメント'
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: 'EXISTS ステートメント'
doc_type: 'reference'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

指定したデータベース内にテーブルが存在する場合は `1`、テーブルまたはデータベースが存在しない場合は `0` を含む、`UInt8` 型の単一のカラムを返します。