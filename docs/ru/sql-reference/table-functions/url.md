---
toc_priority: 41
toc_title: url
---

# url {#url}

Функция `url` создает таблицу с помощью адреса `URL`, формата данных и структуры таблицы.

Функция `url` может быть использована в запросах `SELECT` и `INSERT` в таблицах движка [URL](../../engines/table-engines/special/url.md).

**Синтаксис**

``` sql
url(URL, format, structure)
```

**Параметры**

- `URL` — HTTP или HTTPS-адрес сервера, который может принимать запросы `GET` (для `SELECT`) или `POST` (для `INSERT`). Тип: [String](../../sql-reference/data-types/string.md).
- `format` — [формат](../../interfaces/formats.md#formats) данных. Тип: [String](../../sql-reference/data-types/string.md).
- `structure` — структура таблицы в формате `'UserID UInt64, Name String'`. Определяет имена и типы столбцов. Тип: [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

Таблица с указанными форматом и структурой, а также с данными, полученными из указанного адреса `URL`.

**Примеры**

Получение первых 3 строк таблицы, содержащей столбцы типа `String` и [UInt32](../../sql-reference/data-types/int-uint.md), с HTTP-сервера в формате [CSV](../../interfaces/formats.md/#csv).

``` sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3;
```

Вставка данных в таблицу:

``` sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/table-functions/url/) <!--hide-->
