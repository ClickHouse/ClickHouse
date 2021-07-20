# url {#url}

`url(URL, format, structure)` - возвращает таблицу со столбцами, указанными в
`structure`, созданную из данных находящихся по `URL` в формате `format`.

URL - адрес, по которому сервер принимает `GET` и/или `POST` запросы по
протоколу HTTP или HTTPS.

format - [формат](../../interfaces/formats.md#formats) данных.

structure - структура таблицы в форме `'UserID UInt64, Name String'`. Определяет имена и типы столбцов.

**Пример**

``` sql
-- получение 3-х строк таблицы, состоящей из двух колонк типа String и UInt32 от сервера, отдающего данные в формате CSV
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/url/) <!--hide-->
