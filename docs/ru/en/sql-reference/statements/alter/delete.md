---
description: 'Документация по оператору ALTER TABLE ... DELETE'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'Оператор ALTER TABLE ... DELETE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Удаляет данные, соответствующие указанному выражению фильтрации. Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

:::note
Префикс `ALTER TABLE` отличает этот синтаксис от синтаксиса большинства других SQL-систем. Он указывает на то, что, в отличие от аналогичных запросов в OLTP-базах данных, это ресурсоемкая операция, не предназначенная для частого использования. `ALTER TABLE` считается тяжеловесной операцией, требующей слияния базовых данных перед их удалением. Для таблиц семейства MergeTree рекомендуется использовать [запрос `DELETE FROM`](/ru/sql-reference/statements/delete.md), который выполняет легковесное удаление и может быть значительно быстрее.
:::

`filter_expr` должен иметь тип `UInt8`. Запрос удаляет строки таблицы, для которых это выражение принимает ненулевое значение.

Один запрос может содержать несколько команд, разделенных запятыми.

Синхронность обработки запроса определяется настройкой [mutations&#95;sync](/ru/operations/settings/settings.md/#mutations_sync). По умолчанию обработка выполняется асинхронно.

**См. также**

* [Мутации](/ru/sql-reference/statements/alter/index.md#mutations)
* [Синхронность запросов ALTER](/ru/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* настройка [mutations&#95;sync](/ru/operations/settings/settings.md/#mutations_sync)

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Как обрабатывать обновления и удаления в ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)