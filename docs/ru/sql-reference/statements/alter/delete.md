---
slug: /ru/sql-reference/statements/alter/delete
sidebar_position: 39
sidebar_label: DELETE
---

# ALTER TABLE ... DELETE {#alter-mutations}

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Удаляет данные, соответствующие указанному выражению фильтрации. Реализовано как [мутация](../../../sql-reference/statements/alter/index.md#mutations).

:::note Примечание
Префикс `ALTER TABLE` делает этот синтаксис отличным от большинства других систем, поддерживающих SQL. Он предназначен для обозначения того, что в отличие от аналогичных запросов в базах данных OLTP это тяжелая операция, не предназначенная для частого использования.
:::

Выражение `filter_expr` должно иметь тип `UInt8`. Запрос удаляет строки в таблице, для которых это выражение принимает ненулевое значение.

Один запрос может содержать несколько команд, разделенных запятыми.

Синхронность обработки запроса определяется параметром [mutations_sync](../../../operations/settings/settings.md#mutations_sync). По умолчанию он является асинхронным.

**Смотрите также**

-   [Мутации](../../../sql-reference/statements/alter/index.md#mutations)
-   [Синхронность запросов ALTER](../../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [mutations_sync](../../../operations/settings/settings.md#mutations_sync) setting
