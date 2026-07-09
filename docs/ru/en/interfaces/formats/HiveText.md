---
alias: []
description: 'Документация по формату HiveText'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✗     |           |

<div id="description">
  ## Описание
</div>

`HiveText` читает текстовый формат сериализации, используемый в таблицах [Apache Hive](https://hive.apache.org/)
(формат, создаваемый `LazySimpleSerDe` в Hive). Это текстовый формат с
разделителями, похожий на [`CSV`](/ru/interfaces/formats/CSV), в котором поля
разделяются стандартным для Hive разделителем `\x01` (Ctrl-A). Разделитель полей
можно настроить с помощью [`input_format_hive_text_fields_delimiter`](#format-settings).

`HiveText` — формат только для ввода. У данных нет строки заголовка: значения
сопоставляются со столбцами целевой таблицы по порядку, поэтому имена столбцов
и типы берутся из таблицы (или из явно заданной структуры), а не определяются по данным
автоматически. При чтении ClickHouse разбирает даты и время в режиме best-effort (см. [`date_time_input_format`](/ru/operations/settings/formats#date_time_input_format)),
заполняет пропущенные конечные поля значениями столбцов по умолчанию и пропускает поля,
которые не распознаёт.

Внутри поля значения разбираются по тем же правилам экранирования, что и в `CSV`,
а не с использованием вложенных разделителей Hive. В частности, столбец типа
[`Array`](/ru/sql-reference/data-types/array) читается из представления в квадратных
скобках (например, `"['a','b','c']"`), а не из значений, разделённых
разделителем коллекций Hive `\x02`.

:::note Настройки вложенных разделителей не влияют на работу
Настройки [`input_format_hive_text_collection_items_delimiter`](#format-settings) и
[`input_format_hive_text_map_keys_delimiter`](#format-settings) поддерживаются для
совместимости, но в настоящее время не используются при разборе.
:::

По умолчанию строкам разрешено содержать переменное число полей (см.
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)):
в строках, где полей меньше, чем столбцов в таблице, отсутствующие столбцы
заполняются значениями по умолчанию, а в строках с лишними конечными полями
лишние поля пропускаются.

<div id="example-usage">
  ## Пример использования
</div>

В приведённых ниже примерах разделитель полей, используемый по умолчанию, переопределяется на запятую (`,`) с помощью
[`input_format_hive_text_fields_delimiter`](#format-settings), чтобы входные
файлы было удобнее читать.

<div id="reading-data">
  ### Чтение файла HiveText
</div>

Дан файл `hive_data.txt` с полями, разделёнными запятыми:

```text title="hive_data.txt"
1,3
3,5,9
```

Мы создаём таблицу с именами столбцов и типами, а затем вставляем в неё файл с помощью `FORMAT HiveText`:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

Обратите внимание, что первая строка, `1,3`, содержит только два поля, поэтому отсутствующий столбец `c`
заполняется значением по умолчанию `0`.

<div id="variable-number-of-columns">
  ### Переменное количество столбцов
</div>

При значении по умолчанию `input_format_hive_text_allow_variable_number_of_columns = 1`
в строках, содержащих больше полей, чем предусмотрено в таблице, лишние поля в конце
просто пропускаются:

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

Установка `input_format_hive_text_allow_variable_number_of_columns = 0`, напротив,
требует строгого количества полей, и строка, содержащая меньше полей, чем таблица, вызывает
исключение при разборе.

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                 | Описание                                                                                                                                                        | По умолчанию |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `input_format_hive_text_fields_delimiter`                 | Разделитель между полями в Hive Text File                                                                                                                       | `\x01`       |
| `input_format_hive_text_collection_items_delimiter`       | Разделитель между элементами коллекции (массива или Map) в Hive Text File. Принимается, но в настоящее время не используется при разборе.                      | `\x02`       |
| `input_format_hive_text_map_keys_delimiter`               | Разделитель между ключом и значением в паре Map в Hive Text File. Принимается, но в настоящее время не используется при разборе.                               | `\x03`       |
| `input_format_hive_text_allow_variable_number_of_columns` | Игнорировать лишние столбцы во входных данных Hive Text (если в файле больше столбцов, чем ожидается) и трактовать отсутствующие поля как значения по умолчанию | `1`          |