---
description: 'Документация по изменению выражения в SAMPLE BY'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: 'Изменение выражений ключа сэмплирования'
doc_type: 'reference'
---

Доступны следующие операции:

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

Команда изменяет [ключ выборки](../../../engines/table-engines/mergetree-family/mergetree.md) таблицы на `new_expression` (выражение или кортеж выражений). Первичный ключ должен содержать новый ключ выборки.

<div id="remove">
  ## УДАЛЕНИЕ
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

Команда удаляет [ключ выборки](../../../engines/table-engines/mergetree-family/mergetree.md) таблицы.

Команды `MODIFY` и `REMOVE` считаются легковесными, поскольку изменяют только метаданные или удаляют файлы.

:::note
Это работает только для таблиц семейства [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](../../../engines/table-engines/mergetree-family/replication.md) таблицы).
:::