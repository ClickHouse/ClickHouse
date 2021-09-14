---
toc_priority: 40
toc_title: UPDATE
---

# ALTER TABLE … UPDATE {#alter-table-update-statements}

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

Манипулирует данными, соответствующими заданному выражению фильтрации. Реализовано как [мутация](../../../sql-reference/statements/alter/index.md#mutations).

!!! note "Note"
    Префикс `ALTER TABLE` делает этот синтаксис отличным от большинства других систем, поддерживающих SQL. Он предназначен для обозначения того, что в отличие от аналогичных запросов в базах данных OLTP это тяжелая операция, не предназначенная для частого использования.

Выражение `filter_expr` должно иметь тип `UInt8`. Запрос изменяет значение указанных столбцов на вычисленное значение соответствующих выражений в каждой строке, для которой `filter_expr` принимает ненулевое значение. Вычисленные значения преобразуются к типу столбца с помощью оператора `CAST`. Изменение столбцов, которые используются при вычислении первичного ключа или ключа партиционирования, не поддерживается.

Один запрос может содержать несколько команд, разделенных запятыми.

Синхронность обработки запроса определяется параметром [mutations_sync](../../../operations/settings/settings.md#mutations_sync). По умолчанию он является асинхронным.

**Смотрите также**

-   [Мутации](../../../sql-reference/statements/alter/index.md#mutations)
-   [Синхронность запросов ALTER](../../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [mutations_sync](../../../operations/settings/settings.md#mutations_sync) setting

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/update/) <!--hide-->