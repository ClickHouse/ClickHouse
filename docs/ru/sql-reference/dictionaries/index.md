---
toc_folder_title: "\u0421\u043b\u043e\u0432\u0430\u0440\u0438"
toc_priority: 35
toc_title: "\u0412\u0432\u0435\u0434\u0435\u043d\u0438\u0435"
---

# Словари {#slovari}

Словарь — это отображение (`ключ -> атрибуты`), которое удобно использовать для различного вида справочников.

ClickHouse поддерживает специальные функции для работы со словарями, которые можно использовать в запросах. Проще и эффективнее использовать словари с помощью функций, чем `JOIN` с таблицами-справочниками.

В словаре нельзя хранить значения [NULL](../../sql-reference/syntax.md#null-literal).

ClickHouse поддерживает:

-   [Встроенные словари](internal-dicts.md#internal_dicts) со специфическим [набором функций](../../sql-reference/dictionaries/external-dictionaries/index.md).
-   [Подключаемые (внешние) словари](external-dictionaries/external-dicts.md#dicts-external-dicts) с [набором функций](../../sql-reference/dictionaries/external-dictionaries/index.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/) <!--hide-->
