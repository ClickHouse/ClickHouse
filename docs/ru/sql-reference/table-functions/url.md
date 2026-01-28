---
slug: /ru/sql-reference/table-functions/url
sidebar_position: 41
sidebar_label: url
---

# url {#url}

Функция `url` берет данные по указанному адресу `URL` и создает из них таблицу указанной структуры со столбцами указанного формата.

Функция `url` может быть использована в запросах `SELECT` и `INSERT` с таблицами на движке [URL](../../engines/table-engines/special/url.md).

**Синтаксис**

``` sql
url(URL [,format] [,structure])
```

**Параметры**

- `URL` — HTTP или HTTPS-адрес сервера, который может принимать запросы `GET` или `POST` (для запросов `SELECT` или `INSERT` соответственно). Тип: [String](../../sql-reference/data-types/string.md).
- `format` — [формат](/sql-reference/formats) данных. Тип: [String](../../sql-reference/data-types/string.md).
- `structure` — структура таблицы в формате `'UserID UInt64, Name String'`. Определяет имена и типы столбцов. Тип: [String](../../sql-reference/data-types/string.md).
- `headers` -  HTTP-заголовки в формате `'headers('key1'='value1', 'key2'='value2')'`. Определяет заголовки для HTTP вызова.

**Возвращаемое значение**

Таблица с указанными форматом и структурой, а также с данными, полученными из указанного адреса `URL`.

**Примеры**

Получение с HTTP-сервера первых 3 строк таблицы с данными в формате [CSV](../../interfaces/formats.md#csv), содержащей столбцы типа [String](../../sql-reference/data-types/string.md) и [UInt32](../../sql-reference/data-types/int-uint.md).

``` sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Вставка данных в таблицу:

``` sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

## Символы подстановки в URL {#globs-in-url}

Шаблоны в фигурных скобках `{ }` используются, чтобы сгенерировать список шардов или указать альтернативные адреса на случай отказа. Поддерживаемые типы шаблонов и примеры смотрите в описании функции [remote](remote.md#globs-in-addresses).
Символ `|` внутри шаблонов используется, чтобы задать адреса, если предыдущие оказались недоступны. Эти адреса перебираются в том же порядке, в котором они указаны в шаблоне. Количество адресов, которые могут быть сгенерированы, ограничено настройкой [glob_expansion_max_elements](../../operations/settings/settings.md#glob_expansion_max_elements).

## Виртуальные столбцы

-   `_path` — Путь до `URL`.
-   `_file` — Имя ресурса `URL`.

**Смотрите также**

-   [Виртуальные столбцы](index.md#table_engines-virtual_columns)
