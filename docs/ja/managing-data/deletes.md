---
slug: /ja/deletes
title: データの削除
description: ClickHouseでデータを削除する方法
keywords: [削除, トランケート, ドロップ, 論理削除]
---

ClickHouseではデータを削除するためのいくつかの方法があり、それぞれに利点とパフォーマンス特性があります。データモデルと削除する予定のデータ量に基づいて適切な方法を選択する必要があります。

| 方法 | 構文 | 使用のタイミング |
| --- | --- | --- |
| [論理削除](/ja/guides/developer/lightweight-delete) | `DELETE FROM [table]` | 小規模なデータを削除する場合に使用します。`SELECT`クエリからすぐにフィルタリングされますが、最初は内部的に削除済みとしてマークされるだけで、ディスクからは削除されません。 |
| [物理削除](/ja/sql-reference/statements/alter/delete) | `ALTER TABLE [table] DELETE` | ディスクからすぐにデータを削除する必要がある場合に使用します（例：コンプライアンス目的）。`SELECT`のパフォーマンスに悪影響を及ぼします。 |
| [テーブルをトランケート](/ja/sql-reference/statements/truncate) | `TRUNCATE TABLE [db.table]` | テーブルからすべてのデータを効率的に削除します。 |
| [パーティションをドロップ](/ja/sql-reference/statements/alter/partition#drop-partitionpart) | `DROP PARTITION` | パーティションからすべてのデータを効率的に削除します。 |

ClickHouseでデータを削除するさまざまな方法の概要は以下の通りです。

## 論理削除

論理削除により、行は削除済みとしてマークされ、すべての後続の`SELECT`クエリから自動的にフィルタリングされます。削除された行のその後の削除は自然なマージサイクル中に行われるため、I/Oが少なくなります。そのため、未指定の期間、データは実際にはストレージから削除されず、削除としてマークされるのみです。データが確実に削除されることを保証する必要がある場合は、上記のミューテーションコマンドを検討してください。

```sql
-- ミューテーションで2018年のすべてのデータを削除します。推奨されません。
DELETE FROM posts WHERE toYear(CreationDate) = 2018
```

論理削除ステートメントを使用して大量のデータを削除すると、`SELECT`クエリのパフォーマンスに悪影響を及ぼす可能性もあります。このコマンドは、プロジェクションを持つテーブルには互換性がありません。

操作には削除された行を[マークするためのミューテーション](/ja/sql-reference/statements/delete#how-lightweight-deletes-work-internally-in-clickhouse)（`_row_exists`カラムを追加）に関連したI/Oが生じます。

一般に、削除されたデータがディスク上に存在することが許容される場合（非コンプライアンスケースなど）、論理削除を物理削除よりも優先するべきです。すべてのデータを削除する必要がある場合は、このアプローチは避けるべきです。

[論理削除](/ja/guides/developer/lightweight-delete)について詳しく読む。

## 物理削除

論理削除は`ALTER TABLE … DELETE`コマンドを通じて発行できます。例：

```sql
-- ミューテーションで2018年のすべてのデータを削除します。推奨されません。
ALTER TABLE posts DELETE WHERE toYear(CreationDate) = 2018
```

これらは同期的に（非レプリケートのデフォルト）、または非同期的に（[mutations_sync](/ja/operations/settings/settings#mutations_sync)設定により決定）実行できます。`WHERE`式に一致するすべての部分を再書き込みするため、非常にI/Oが多くなります。このプロセスにはアトミック性がなく、部分が準備ができ次第、ミューテーションされた部分に置き換えられます。ミューテーション中に実行される`SELECT`クエリは、すでにミューテーションされた部分のデータとまだミューテーションされていない部分のデータの両方を観察します。進行状況の状態は[systems.mutations](/ja/operations/system-tables/mutations#system_tables-mutations)テーブルを通して確認できます。これらはI/Oが多い操作であり、クラスタの`SELECT`パフォーマンスに影響を与える可能性があるため、慎重に使用するべきです。

[論理削除](/ja/sql-reference/statements/alter/delete)について詳しく読む。

## テーブルをトランケート

テーブル内のすべてのデータを削除する必要がある場合は、以下の`TRUNCATE TABLE`コマンドを使用してください。これは論理な操作です。

```sql
TRUNCATE TABLE posts
```

[TRUNCATE TABLE](/ja/sql-reference/statements/truncate)について詳しく読む。

## パーティションをドロップ

データにカスタムパーティションキーを指定している場合、パーティションを効率的にドロップできます。高カーディナリティのパーティショニングは避けてください。

```sql
ALTER TABLE posts (DROP PARTITION '2008')
```

[DROP PARTITION](/ja/sql-reference/statements/alter/partition)について詳しく読む。

## その他のリソース

- [ClickHouseでのアップデートと削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
