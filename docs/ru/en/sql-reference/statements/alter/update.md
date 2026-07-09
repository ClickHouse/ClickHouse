---
description: 'Документация по командам ALTER TABLE ... UPDATE'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'Команды ALTER TABLE ... UPDATE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

Изменяет данные, соответствующие указанному выражению фильтрации. Реализовано в виде [мутации](/ru/sql-reference/statements/alter/index.md#mutations).

:::note
Префикс `ALTER TABLE` отличает этот синтаксис от большинства других систем, поддерживающих SQL. Он указывает на то, что, в отличие от похожих запросов в OLTP-базах данных, это ресурсоёмкая операция, не предназначенная для частого использования.
:::

`filter_expr` должен иметь тип `UInt8`. Этот запрос обновляет значения указанных столбцов значениями соответствующих выражений в строках, для которых `filter_expr` принимает ненулевое значение. Значения приводятся к типу столбца с помощью оператора `CAST`. Обновление столбцов, используемых при вычислении основного ключа или ключа партиционирования, не поддерживается.

Один запрос может содержать несколько команд, разделённых запятыми.

Синхронность выполнения запроса определяется настройкой [mutations&#95;sync](/ru/operations/settings/settings.md/#mutations_sync). По умолчанию выполнение происходит асинхронно.

**См. также**

* [Мутации](/ru/sql-reference/statements/alter/index.md#mutations)
* [Синхронность ALTER-запросов](/ru/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* настройка [mutations&#95;sync](/ru/operations/settings/settings.md/#mutations_sync)
* [Легковесный `UPDATE`](/ru/sql-reference/statements/update) - Альтернативное легковесное обновление с использованием патч-частей
* [`APPLY PATCHES`](/ru/sql-reference/statements/alter/apply-patches) - Вручную применить патчи из легковесных обновлений

<div id="related-content">
  ## См. также
</div>

* Блог: [Обновления и удаления в ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)