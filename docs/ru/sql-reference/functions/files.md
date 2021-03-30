---
toc_priority: 43
toc_title: "Функции для работы с файлами"
---

# Функции для работы с файлами {#funktsii-dlia-raboty-s-failami}

## file {#file}

Читает файл как строку. Содержимое файла не разбирается (не парсится) и записывается в указанную колонку в виде единой строки.

**Синтаксис**

``` sql
file(path)
```

**Аргументы**

-   `path` — относительный путь до файла от [user_files_path](../../sql-reference/table-functions/file.md#server_configuration_parameters-user_files_path). Путь к файлу поддерживает следующие шаблоны в режиме доступа только для чтения `*`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc', 'def'` — строки.

**Примеры**

Вставка данных из файлов a.txt и b.txt в таблицу в виде отдельных строк:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```

**Смотрите также**

-   [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path)
-   [file](../table-functions/file.md)
