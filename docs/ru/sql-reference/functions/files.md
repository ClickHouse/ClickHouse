---
sidebar_position: 43
sidebar_label: "Функции для работы с файлами"
---

# Функции для работы с файлами {#funktsii-dlia-raboty-s-failami}

## file {#file}

Читает файл как строку. Содержимое файла не разбирается (не парсится) и записывается в указанную колонку в виде единой строки.

**Синтаксис**

``` sql
file(path[, default])
```

**Аргументы**

-   `path` — относительный путь до файла от [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Путь к файлу может включать следующие символы подстановки и шаблоны: `*`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc', 'def'` — строки.
-   `default` — Значение возвращаемое в случае, если указанный файл не существует. Поддерживаемые типы данных: [String](../../sql-reference/data-types/string.md) и [NULL](../../sql-reference/syntax.md#null-literal).

**Примеры**

Вставка данных из файлов a.txt и b.txt в таблицу в виде строк:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```

**Смотрите также**

-   [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path)
-   [file](../table-functions/file.md)
