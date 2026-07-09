---
alias: []
description: 'Документация для формата JSONObjectEachRow'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
doc_type: 'reference'
---

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

В этом формате все данные представлены в виде единого объекта JSON, а каждая строка — в виде отдельного поля этого объекта, как и в формате [`JSONEachRow`](./JSONEachRow.md).

<div id="example-usage">
  ## Пример использования
</div>

<div id="basic-example">
  ### Простой пример
</div>

Допустим, есть такой JSON:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

Чтобы использовать имя объекта в качестве значения столбца, можно воспользоваться специальной настройкой [`format_json_object_each_row_column_for_object_name`](/ru/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name).
В значении этой настройки указывается имя столбца, которое используется как ключ JSON для строки в результирующем объекте.

<div id="output">
  #### Результат
</div>

Допустим, у нас есть таблица `test` с двумя столбцами:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Выведем это в формате `JSONObjectEachRow` и используем настройку `format_json_object_each_row_column_for_object_name`:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

<div id="input">
  #### Входные данные
</div>

Допустим, мы сохранили результат предыдущего примера в файл с именем `data.json`:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Это также работает при автоматическом определении схемы:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

<div id="json-inserting-data">
  ### Вставка данных
</div>

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse допускает:

* Любой порядок пар ключ-значение в объекте.
* Опускать некоторые значения.

ClickHouse игнорирует пробелы между элементами и запятые после объектов. Вы можете передать все объекты в одной строке. Разделять их переводами строк не требуется.

<div id="omitted-values-processing">
  #### Обработка пропущенных значений
</div>

ClickHouse подставляет вместо пропущенных значений значения по умолчанию для соответствующих [типов данных](/ru/sql-reference/data-types/index.md).

Если указан `DEFAULT expr`, ClickHouse использует разные правила подстановки в зависимости от настройки [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ru/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields).

Рассмотрим следующую таблицу:

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

* Если `input_format_defaults_for_omitted_fields = 0`, то значение по умолчанию для `x` и `a` равно `0` (это значение по умолчанию для типа данных `UInt32`).
* Если `input_format_defaults_for_omitted_fields = 1`, то значение по умолчанию для `x` равно `0`, а для `a` — `x * 2`.

:::note
При вставке данных с `input_format_defaults_for_omitted_fields = 1` ClickHouse требует больше вычислительных ресурсов, чем при вставке с `input_format_defaults_for_omitted_fields = 0`.
:::

<div id="json-selecting-data">
  ### Выборка данных
</div>

Рассмотрим таблицу `UserActivity` в качестве примера:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Запрос `SELECT * FROM UserActivity FORMAT JSONEachRow` возвращает:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

В отличие от формата [JSON](/ru/interfaces/formats/JSON), недопустимые последовательности UTF-8 здесь не заменяются. Значения экранируются так же, как в `JSON`.

:::info
В строки можно выводить любые наборы байтов. Используйте формат [`JSONEachRow`](./JSONEachRow.md), если вы уверены, что данные в таблице можно представить в формате JSON без потери информации.
:::

<div id="jsoneachrow-nested">
  ### Использование структур Nested
</div>

Если у вас есть таблица со столбцами типа [`Nested`](/ru/sql-reference/data-types/nested-data-structures/index.md), вы можете вставлять JSON-данные с той же структурой. Включите эту возможность с помощью настройки [input&#95;format&#95;import&#95;nested&#95;json](/ru/operations/settings/settings-formats.md/#input_format_import_nested_json).

Например, рассмотрим следующую таблицу:

```sql title="Query"
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Как видно из описания типа данных `Nested`, ClickHouse рассматривает каждый компонент вложенной структуры как отдельный столбец (`n.s` и `n.i` для нашей таблицы). Вставить данные можно следующим образом:

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Чтобы выполнить вставку данных в виде иерархического объекта JSON, установите [`input_format_import_nested_json=1`](/ru/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Без этой настройки ClickHouse выдаёт исключение.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

<div id="format-settings">
  ## Настройки формата
</div>

| Параметр                                                                                                                                                                     | Описание                                                                                                                                                                                         | По умолчанию | Примечания                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`input_format_import_nested_json`](/ru/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                               | сопоставлять вложенные данные JSON с вложенными таблицами (работает для format JSONEachRow).                                                                                                     | `false`      |                                                                                                                                                                                                |
| [`input_format_json_read_bools_as_numbers`](/ru/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                               | разрешить разбирать логические значения как числа в JSON input formats.                                                                                                                          | `true`       |                                                                                                                                                                                                |
| [`input_format_json_read_bools_as_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                               | разрешить разбирать логические значения как строки во входных JSON-форматах.                                                                                                                     | `true`       |                                                                                                                                                                                                |
| [`input_format_json_read_numbers_as_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                           | разрешить разбирать числа как строки во входных JSON-форматах.                                                                                                                                   | `true`       |                                                                                                                                                                                                |
| [`input_format_json_read_arrays_as_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                             | разрешить разбирать JSON-массивы как строки во входных JSON-форматах.                                                                                                                            | `true`       |                                                                                                                                                                                                |
| [`input_format_json_read_objects_as_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                           | разрешить разбирать объекты JSON как строки в JSON-форматах ввода.                                                                                                                               | `true`       |                                                                                                                                                                                                |
| [`input_format_json_named_tuples_as_objects`](/ru/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                           | разбирать столбцы именованных кортежей как объекты JSON.                                                                                                                                         | `true`       |                                                                                                                                                                                                |
| [`input_format_json_try_infer_numbers_from_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                             | пытаться определять числа в строковых полях при автоматическом выводе схемы.                                                                                                                     | `false`      |                                                                                                                                                                                                |
| [`input_format_json_try_infer_named_tuples_from_objects`](/ru/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                   | пытаться определять именованный кортеж по объектам JSON при автоматическом определении схемы.                                                                                                    | `true`       |                                                                                                                                                                                                |
| [`input_format_json_infer_incomplete_types_as_strings`](/ru/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                       | использовать тип String для ключей, содержащих только NULL или пустые объекты/массивы, при автоматическом определении схемы в JSON input formats.                                                | `true`       |                                                                                                                                                                                                |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/ru/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | вставлять значения по умолчанию для отсутствующих элементов в объекте JSON при разборе именованного кортежа.                                                                                     | `true`       |                                                                                                                                                                                                |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/ru/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                     | игнорировать неизвестные ключи в объекте JSON для именованных Tuple.                                                                                                                             | `false`      |                                                                                                                                                                                                |
| [`input_format_json_compact_allow_variable_number_of_columns`](/ru/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)         | разрешить переменное число столбцов в формате JSONCompact/JSONCompactEachRow, игнорировать лишние столбцы и использовать значения по умолчанию для отсутствующих столбцов.                       | `false`      |                                                                                                                                                                                                |
| [`input_format_json_throw_on_bad_escape_sequence`](/ru/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                 | сгенерировать исключение, если JSON-строка содержит некорректную escape-последовательность. Если отключено, некорректные escape-последовательности останутся в данных как есть.                  | `true`       |                                                                                                                                                                                                |
| [`input_format_json_empty_as_default`](/ru/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                         | обрабатывает пустые поля во входном JSON как значения по умолчанию.                                                                                                                              | `false`.     | Для сложных выражений по умолчанию также необходимо включить [`input_format_defaults_for_omitted_fields`](/ru/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [`output_format_json_quote_64bit_integers`](/ru/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                               | управляет заключением 64-битных целых чисел в кавычки в формате вывода JSON.                                                                                                                     | `true`       |                                                                                                                                                                                                |
| [`output_format_json_quote_64bit_floats`](/ru/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                   | управляет заключением 64-битных чисел с плавающей точкой в кавычки в формате вывода JSON.                                                                                                        | `false`      |                                                                                                                                                                                                |
| [`output_format_json_quote_denormals`](/ru/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                         | включает вывод значений &#39;+nan&#39;, &#39;-nan&#39;, &#39;+inf&#39;, &#39;-inf&#39; в формате вывода JSON.                                                                                    | `false`      |                                                                                                                                                                                                |
| [`output_format_json_quote_decimals`](/ru/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                           | управляет заключением значений Decimal в кавычки в формате вывода JSON.                                                                                                                          | `false`      |                                                                                                                                                                                                |
| [`output_format_json_escape_forward_slashes`](/ru/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                           | управляет экранированием прямых слешей при выводе строк в формате вывода JSON.                                                                                                                   | `true`       |                                                                                                                                                                                                |
| [`output_format_json_named_tuples_as_objects`](/ru/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                         | сериализует столбцы named tuple как объекты JSON.                                                                                                                                                | `true`       |                                                                                                                                                                                                |
| [`output_format_json_array_of_rows`](/ru/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                             | выводит JSON-массив всех строк в формате JSONEachRow(Compact).                                                                                                                                   | `false`      |                                                                                                                                                                                                |
| [`output_format_json_validate_utf8`](/ru/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                             | включает проверку последовательностей UTF-8 в выходных форматах JSON (обратите внимание, что это не влияет на форматы JSON/JSONCompact/JSONColumnsWithMetadata: в них UTF-8 проверяется всегда). | `false`      |                                                                                                                                                                                                |