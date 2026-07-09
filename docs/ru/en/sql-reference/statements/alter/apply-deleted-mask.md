---
description: 'Документация по команде APPLY DELETED MASK'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'APPLY DELETED MASK'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

Команда применяет маску, созданную с помощью [легковесного удаления](/ru/sql-reference/statements/delete), и принудительно удаляет с диска строки, помеченные как удалённые. Эта команда представляет собой тяжеловесную мутацию и семантически эквивалентна запросу `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`.

:::note
Она работает только для таблиц семейства [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](../../../engines/table-engines/mergetree-family/replication.md) таблицы).
:::

**См. также**

* [Легковесные удаления](/ru/sql-reference/statements/delete)
* [Тяжеловесные удаления](/ru/sql-reference/statements/alter/delete.md)