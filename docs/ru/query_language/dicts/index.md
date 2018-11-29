# Словари

Словарь — это отображение (`ключ -> атрибуты`), которое удобно использовать для различного вида справочников.

ClickHouse поддерживает специальные функции для работы со словарями, которые можно использовать в запросах. Проще и эффективнее использовать словари с помощью функций, чем `JOIN` с таблицами-справочниками.

В словаре нельзя хранить значения [NULL](../syntax.md#null-literal).

ClickHouse поддерживает:

- [Встроенные словари](internal_dicts.md#internal_dicts) со специфическим [набором функций](../functions/ym_dict_functions.md#ym_dict_functions).
- [Подключаемые (внешние) словари](external_dicts.md#dicts-external_dicts) с [набором функций](../functions/ext_dict_functions.md#ext_dict_functions).

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/dicts/) <!--hide-->
