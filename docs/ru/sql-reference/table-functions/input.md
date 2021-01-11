---
toc_priority: 46
toc_title: input
---

# input {#input}

`input(structure)` - табличная функция, позволяющая эффективно преобразовывать и вставлять отправленные на сервер данные,
имеющие структуру `structure`, в таблицу другой структуры.

`structure` - структура отправляемых на сервер данных в формате `'column1_name column1_type, column2_name column2_type, ...'`.
Например: `'id UInt32, name String'`.

Данная функция может быть использована только в запросе `INSERT SELECT` и только один раз, но в остальном ведет себя
как обычная табличная функция (можно указать в подзапросе и т.д.).

Данные можно отправлять любым стандартным способом как для обычного `INSERT` запроса и в любом
доступном [формате](../../interfaces/formats.md#formats), который указывается в конце
запроса (в отличие от обычного `INSERT SELECT`).

Главная особенность данной функции в том, что сервер при получении данных от клиента
одновременно преобразует их в соответствии со списком выражений в `SELECT` части и вставляет в целевую таблицу. Временная таблица
со всеми переданными данными не создается.

**Примеры**

-   Пусть у таблицы `test` следующая структура `(a String, b String)`,
    а в файле `data.csv` данные имеют другую структуру `(col1 String, col2 Date, col3 Int32)`. Запрос для вставки
    данных из файла `data.csv` в таблицу `test` с одновременным преобразованием и использованием функций выглядит так:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   Если в `data.csv` лежат данные той же структуры `test_structure`, что и у таблицы `test`, то следующие два запроса эквивалентны:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/input/) <!--hide-->
