---
description: 'EXISTS 语句文档'
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: 'EXISTS 语句'
doc_type: 'reference'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

返回一个 `UInt8` 类型的列：如果表或数据库不存在，该列的值为 `0`；如果表存在于指定数据库中，该列的值为 `1`。