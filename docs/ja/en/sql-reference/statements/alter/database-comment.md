---
description: 'データベースコメントの追加、変更、削除を行う ALTER DATABASE ... MODIFY COMMENT ステートメントのドキュメントです。'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'ALTER DATABASE ... MODIFY COMMENT 文'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

事前に設定されていたかどうかにかかわらず、データベースコメントを追加、変更、または削除します。コメントの変更は、[`system.databases`](/ja/operations/system-tables/databases.md) と `SHOW CREATE DATABASE` クエリの両方に反映されます。

<div id="syntax">
  ## 構文
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## 例
</div>

コメント付きの`DATABASE`を作成するには:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

コメントを変更するには:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

変更後のコメントを表示するには:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

データベースコメントを削除するには:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

コメントが削除されていることを確認するには:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## 関連コンテンツ
</div>

* [`COMMENT`](/ja/sql-reference/statements/create/table#comment-clause) 句
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)