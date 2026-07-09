---
alias: []
description: 'Документация по формату CSV'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'reference'
---

<div id="description">
  ## Описание
</div>

Формат Comma Separated Values ([RFC](https://tools.ietf.org/html/rfc4180)).
При форматировании строки заключаются в двойные кавычки. Двойная кавычка внутри строки выводится как две двойные кавычки подряд.
Других правил экранирования символов нет.

* Значения типа Date и date-time заключаются в двойные кавычки.
* Числа выводятся без кавычек.
* Значения разделяются символом-разделителем, которым по умолчанию является `,`. Символ-разделитель задается в настройке [format&#95;csv&#95;delimiter](/ru/operations/settings/settings-formats.md/#format_csv_delimiter).
* Строки разделяются символом перевода строки Unix (LF).
* Массивы сериализуются в CSV следующим образом:
  * сначала массив сериализуется в строку, как в формате TabSeparated
  * затем полученная строка выводится в CSV в двойных кавычках.
* Tuple в формате CSV сериализуются как отдельные столбцы (то есть их вложенность в кортеже теряется).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
По умолчанию используется разделитель `,`
Дополнительные сведения см. в настройке [format&#95;csv&#95;delimiter](/ru/operations/settings/settings-formats.md/#format_csv_delimiter).
:::

При разборе все значения можно обрабатывать как с кавычками, так и без них. Поддерживаются и двойные, и одинарные кавычки.

Строки также могут быть записаны без кавычек. В этом случае они разбираются до символа-разделителя или символа перевода строки (CR или LF).
Однако, в нарушение RFC, при разборе строк без кавычек начальные и конечные пробелы и символы табуляции игнорируются.
Поддерживаются следующие типы перевода строки: Unix (LF), Windows (CR LF) и Mac OS Classic (CR LF).

`NULL` форматируется в соответствии с настройкой [format&#95;csv&#95;null&#95;representation](/ru/operations/settings/settings-formats.md/#format_csv_null_representation) (значение по умолчанию — `\N`).

Во входных данных значения `ENUM` могут быть представлены как именами, так и идентификаторами.
Сначала выполняется попытка сопоставить входное значение с именем ENUM.
Если это не удаётся и входное значение является числом, выполняется попытка сопоставить это число с идентификатором ENUM.
Если входные данные содержат только идентификаторы ENUM, для оптимизации разбора `ENUM` рекомендуется включить настройку [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ru/operations/settings/settings-formats.md/#input_format_csv_enum_as_number).

<div id="example-usage">
  ## Пример использования
</div>

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                                                                                | Описание                                                                                                                                             | По умолчанию | Примечания                                                                                                                                                                                                       |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/ru/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | символ, который считается разделителем в данных CSV.                                                                                                 | `,`          |                                                                                                                                                                                                                  |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/ru/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | разрешить строки в одинарных кавычках.                                                                                                               | `true`       |                                                                                                                                                                                                                  |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/ru/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | разрешить строки в двойных кавычках.                                                                                                                 | `true`       |                                                                                                                                                                                                                  |
| [format&#95;csv&#95;null&#95;representation](/ru/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | настраиваемое представление значения NULL в формате CSV.                                                                                             | `\N`         |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/ru/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | обрабатывать пустые поля во входных данных CSV как значения по умолчанию.                                                                            | `true`       | Для сложных выражений по умолчанию также должен быть включен [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ru/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/ru/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | обрабатывать вставленные значения enum в форматах CSV как индексы enum.                                                                              | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/ru/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | использовать дополнительные приемы и эвристики для определения схемы в формате CSV. Если параметр отключен, все поля будут определены как String.    | `true`       |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/ru/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | при чтении Array из CSV ожидать, что его элементы были сериализованы во вложенный CSV, а затем помещены в строку.                                    | `false`      |                                                                                                                                                                                                                  |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/ru/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | если установлено значение `true`, окончанием строки в выходном формате CSV будет `\r\n` вместо `\n`.                                                 | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/ru/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | пропустить указанное количество строк в начале данных.                                                                                               | `0`          |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;detect&#95;header](/ru/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | автоматически определять заголовок с именами и типами в формате CSV.                                                                                 | `true`       |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/ru/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | пропускать пустые строки в конце данных.                                                                                                             | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/ru/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | удалять пробелы и символы табуляции в строках CSV без кавычек.                                                                                       | `true`       |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/ru/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | разрешить использовать пробел или табуляцию как разделитель полей в строках CSV.                                                                     | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/ru/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | разрешить переменное количество столбцов в формате CSV, игнорировать лишние столбцы и использовать значения по умолчанию для отсутствующих столбцов. | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/ru/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | разрешить задавать для столбца значение по умолчанию, если десериализация поля CSV завершилась ошибкой из-за некорректного значения.                 | `false`      |                                                                                                                                                                                                                  |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ru/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | пытаться определять числа в строковых полях при определении схемы.                                                                                   | `false`      |                                                                                                                                                                                                                  |