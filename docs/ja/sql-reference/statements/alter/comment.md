---
slug: /ja/sql-reference/statements/alter/comment
sidebar_position: 51
sidebar_label: COMMENT
---

# ALTER TABLE ... MODIFY COMMENT

コメントが設定されていたかどうかに関わらず、テーブルにコメントを追加、修正、または削除します。コメントの変更は、[system.tables](../../../operations/system-tables/tables.md)と`SHOW CREATE TABLE`クエリの両方で反映されます。

**構文**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

**例**

コメント付きのテーブルを作成する（詳細は[COMMENT](../../../sql-reference/statements/create/table.md#comment-table)句を参照）:

``` sql
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

テーブルコメントを修正する:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT 'new comment on a table';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

新しいコメントの出力:

```text
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

テーブルコメントを削除する:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT '';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

削除されたコメントの出力:

```text
┌─comment─┐
│         │
└─────────┘
```

**注意点**

Replicated テーブルの場合、コメントは異なるレプリカで異なる場合があります。コメントの修正は単一のレプリカに適用されます。

この機能はバージョン 23.9 以降で利用可能です。以前の ClickHouse バージョンでは動作しません。
