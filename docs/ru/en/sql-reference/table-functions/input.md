---
description: 'Табличная функция, которая позволяет эффективно преобразовывать и вставлять данные,
  отправленные на сервер с заданной структурой, в таблицу с другой структурой.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` — табличная функция, которая позволяет эффективно преобразовывать и вставлять данные, отправленные на
сервер с заданной структурой, в таблицу с другой структурой.

`structure` — структура данных, отправляемых на сервер, в следующем формате: `'column1_name column1_type, column2_name column2_type, ...'`.
Например, `'id UInt32, name String'`.

Эту функцию можно использовать только в запросе `INSERT SELECT` и только один раз, но в остальном она ведёт себя как обычная табличная функция
(например, её можно использовать в подзапросе и т. д.).

Данные можно отправлять любым способом, как и для обычного запроса `INSERT`, и передавать в любом доступном [формате](/ru/sql-reference/formats),
который необходимо указать в конце запроса (в отличие от обычного `INSERT SELECT`).

Основная особенность этой функции в том, что, когда сервер получает данные от клиента, он одновременно преобразует их
в соответствии со списком выражений в предложении `SELECT` и вставляет в целевую таблицу. Временная таблица
со всеми переданными данными не создаётся.

<div id="examples">
  ## Примеры
</div>

* Пусть таблица `test` имеет следующую структуру `(a String, b String)`,
  а данные в `data.csv` — другую структуру `(col1 String, col2 Date, col3 Int32)`. Запрос на вставку
  данных из `data.csv` в таблицу `test` с одновременным преобразованием выглядит так:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* Если `data.csv` содержит данные той же структуры, что и таблица `test`, а именно `test_structure`, то эти два запроса эквивалентны:

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```