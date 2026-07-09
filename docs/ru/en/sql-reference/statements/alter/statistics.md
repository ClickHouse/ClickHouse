---
description: 'Документация по работе со статистикой столбцов'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'Работа со статистикой столбцов'
doc_type: 'справочник'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # Работа со статистикой столбцов
</div>

<CloudNotSupportedBadge />

Доступны следующие операции:

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Добавляет описание статистики в метаданные таблицы.

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Изменяет описание статистики в метаданных таблицы.

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Удаляет статистику из метаданных указанных столбцов и все объекты статистики во всех частях для этих столбцов.

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Удаляет все объекты статистики во всех частях для указанных столбцов. Объекты статистики можно пересоздать с помощью `ALTER TABLE MATERIALIZE STATISTICS`.

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - Пересоздаёт статистику для столбцов. Реализовано как [мутация](../../../sql-reference/statements/alter/index.md#mutations).

Первые две команды являются легковесными, поскольку лишь изменяют метаданные или удаляют файлы.

Кроме того, они поддерживают репликацию: метаданные статистики синхронизируются через ZooKeeper.

<div id="example">
  ## Пример:
</div>

Добавление статистики двух типов для двух столбцов:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
Статистика поддерживается только для таблиц семейства [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](../../../engines/table-engines/mergetree-family/replication.md) варианты).
:::