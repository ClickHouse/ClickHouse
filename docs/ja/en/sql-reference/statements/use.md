---
description: 'USE ステートメントのドキュメント'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'USE ステートメント'
doc_type: 'reference'
---

```sql
USE [DATABASE] db
```

セッションの現在のデータベースを設定できます。

現在のデータベースは、クエリ内でテーブル名の前にドット付きでデータベースが明示的に指定されていない場合に、table を探す際に使用されます。

HTTP protocol を使用している場合は、session という概念がないため、このクエリは実行できません。