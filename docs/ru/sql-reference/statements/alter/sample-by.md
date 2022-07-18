---
toc_priority: 41
toc_title: SAMPLE BY
---

# Manipulating Sampling-Key Expressions {#manipulations-with-sampling-key-expressions}

Синтаксис:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

Команда меняет [ключ сэмплирования](../../../engines/table-engines/mergetree-family/mergetree.md) таблицы на `new_expression` (выражение или ряд выражений).

Эта команда является упрощенной в том смысле, что она изменяет только метаданные. Первичный ключ должен содержать новый ключ сэмплирования.

!!! note "Note"
    Это работает только для таблиц в семействе [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (включая
[реплицируемые](../../../engines/table-engines/mergetree-family/replication.md) таблицы).