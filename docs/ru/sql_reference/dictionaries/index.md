# Словари {#slovari}

Словарь — это отображение (`ключ -> атрибуты`), которое удобно использовать для различного вида справочников.

ClickHouse поддерживает специальные функции для работы со словарями, которые можно использовать в запросах. Проще и эффективнее использовать словари с помощью функций, чем `JOIN` с таблицами-справочниками.

В словаре нельзя хранить значения [NULL](../syntax.md#null).

ClickHouse поддерживает:

-   [Встроенные словари](internal_dicts.md#internal_dicts) со специфическим [набором функций](../../sql_reference/dictionaries/external_dictionaries/index.md).
-   [Подключаемые (внешние) словари](external_dictionaries/external_dicts.md) с [набором функций](../../sql_reference/dictionaries/external_dictionaries/index.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/) <!--hide-->
