---
description: 'Документация по команде APPLY PATCHES для легковесных обновлений'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'APPLY PATCHES для легковесных обновлений'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

Команда вручную запускает физическую материализацию патч-частей, созданных командами [легковесного `UPDATE`](/ru/sql-reference/statements/update). Она принудительно применяет отложенные патчи к частям данных, переписывая только затронутые столбцы.

:::note

* Она работает только для таблиц семейства [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](../../../engines/table-engines/mergetree-family/replication.md) таблицы).
* Это операция мутации, которая выполняется асинхронно в фоновом режиме.
  :::

<div id="when-to-use">
  ## Когда использовать APPLY PATCHES
</div>

:::tip
Как правило, использовать `APPLY PATCHES` не требуется
:::

Патч-части обычно применяются автоматически во время слияний, когда включена настройка [`apply_patches_on_merge`](/ru/operations/settings/merge-tree-settings#apply_patches_on_merge) (по умолчанию). Однако в следующих случаях может потребоваться вручную запустить применение патчей:

* Чтобы уменьшить накладные расходы на применение патчей при выполнении запросов `SELECT`
* Чтобы объединить несколько патч-частей до того, как они накопятся
* Чтобы подготовить данные к резервному копированию или экспорту, когда патчи уже материализованы
* Когда `apply_patches_on_merge` отключена и вы хотите сами контролировать момент применения патчей

<div id="examples">
  ## Примеры
</div>

Примените все отложенные патчи для таблицы:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

Применяйте патчи только к определённой партиции:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Совмещайте с другими операциями:

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## Отслеживание применения патчей
</div>

Вы можете следить за ходом применения патчей с помощью таблицы [`system.mutations`](/ru/operations/system-tables/mutations):

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## См. также
</div>

* [Легковесный `UPDATE`](/ru/sql-reference/statements/update) - Создание патч-частей при легковесных обновлениях
* [настройка `apply_patches_on_merge`](/ru/operations/settings/merge-tree-settings#apply_patches_on_merge) - Управление автоматическим применением патчей при слиянии