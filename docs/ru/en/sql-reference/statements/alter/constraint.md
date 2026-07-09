---
description: 'Документация по изменению ограничений'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Изменение ограничений'
doc_type: 'reference'
---

Ограничения можно добавлять, изменять и удалять с помощью следующего синтаксиса:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

Как и при создании таблицы, ограничение можно объявить либо как `CHECK` (проверяется при `INSERT`), либо как `ASSUME` (считается оптимизатором истинным без проверки). О различиях между ними см. в разделе [constraints](../../../sql-reference/statements/create/table.md#constraints).

`MODIFY CONSTRAINT` заменяет объявление существующего ограничения, сохраняя его положение в определении таблицы. Этот оператор также может изменить тип ограничения (например, с `CHECK` на `ASSUME`). Это эквивалентно удалению ограничения и его повторному добавлению с новым объявлением. Если ограничение не существует, будет сгенерирована ошибка, если не указано `IF EXISTS`.

Подробнее см. в разделе [constraints](../../../sql-reference/statements/create/table.md#constraints).

Эти команды добавляют, изменяют или удаляют метаданные ограничений в таблице, поэтому обрабатываются сразу.

:::tip
Проверка ограничения **не будет выполняться** для существующих данных, если оно было добавлено или изменено.
:::

Все изменения в реплицируемых таблицах транслируются через ZooKeeper и будут применены на других репликах.