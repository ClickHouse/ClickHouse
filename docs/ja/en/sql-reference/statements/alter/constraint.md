---
description: '制約の操作に関するドキュメント'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: '制約の操作'
doc_type: 'reference'
---

制約は、次の構文で追加、変更、または削除できます。

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

テーブル作成時と同様に、制約は `CHECK` (`INSERT` 時に適用) として宣言することも、`ASSUME` (検証は行わず、オプティマイザが前提として扱う) として宣言することもできます。両者の違いについては、[constraints](../../../sql-reference/statements/create/table.md#constraints) を参照してください。

`MODIFY CONSTRAINT` は、テーブル定義内での位置を維持したまま、既存の制約の宣言を置き換えます。また、制約の種類を変更することもできます (たとえば `CHECK` から `ASSUME` へ) 。これは、制約を削除して新しい宣言でもう一度追加するのと同等です。制約が存在しない場合、`IF EXISTS` が指定されていない限り、クエリはエラーを返します。

[constraints](../../../sql-reference/statements/create/table.md#constraints) も参照してください。

これらのクエリは、テーブルの制約に関するメタデータの追加、変更、削除のみを行うため、即座に処理されます。

:::tip
制約チェックは、制約を追加または変更しても、既存のデータに対しては**実行されません**。
:::

レプリケートテーブルに対するすべての変更は ZooKeeper に通知され、他のレプリカにも適用されます。