---
description: 'ALL句のドキュメント'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'ALL句'
doc_type: 'reference'
---

テーブル内に一致する行が複数ある場合、`ALL` はそのすべてを返します。`SELECT ALL` は、`DISTINCT` を指定しない `SELECT` と同じです。`ALL` と `DISTINCT` の両方を指定すると、例外が発生します。

`ALL` は集約関数内でも指定できますが、クエリの結果に実質的な影響はありません。

例:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

次と同等です:

```sql
SELECT sum(number) FROM numbers(10);
```