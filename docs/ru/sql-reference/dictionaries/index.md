# Словари {#slovari}

Словарь — это отображение (`ключ -> атрибуты`), которое удобно использовать для различного вида справочников.

ClickHouse поддерживает специальные функции для работы со словарями, которые можно использовать в запросах. Проще и эффективнее использовать словари с помощью функций, чем `JOIN` с таблицами-справочниками.

В словаре нельзя хранить значения [NULL](../../sql-reference/syntax.md#null-literal).

ClickHouse поддерживает:

-   [Встроенные словари](internal-dicts.md#internal_dicts) со специфическим [набором функций](../../sql-reference/dictionaries/external-dictionaries/index.md).
-   [Подключаемые (внешние) словари](external-dictionaries/external-dicts.md#dicts-external-dicts) с [набором функций](../../sql-reference/dictionaries/external-dictionaries/index.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/) <!--hide-->
