---
description: 'Документация по функциям преобразования типов'
sidebar_label: 'Преобразование типов'
slug: /sql-reference/functions/type-conversion-functions
title: 'Функции преобразования типов'
doc_type: 'reference'
---

<div id="common-issues-with-data-conversion">
  ## Распространённые проблемы при преобразовании данных
</div>

ClickHouse обычно использует [то же поведение, что и программы на C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

Функции `to<type>` и [cast](#CAST) в некоторых случаях ведут себя по-разному, например для [LowCardinality](../data-types/lowcardinality.md): [cast](#CAST) убирает свойство [LowCardinality](../data-types/lowcardinality.md), а функции `to<type>` — нет. То же самое относится и к [Nullable](../data-types/nullable.md): такое поведение несовместимо со стандартом SQL, и его можно изменить с помощью настройки [cast&#95;keep&#95;nullable](../../operations/settings/settings.md/#cast_keep_nullable).

:::note
Помните о возможной потере данных, если значения одного типа данных преобразуются в меньший тип данных (например, из `Int64` в `Int32`) или между
несовместимыми типами данных (например, из `String` в `Int`). Обязательно внимательно проверяйте, соответствует ли результат ожидаемому.
:::

Пример:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

<div id="to-string-functions">
  ## Примечания о функциях `toString`
</div>

Семейство функций `toString` позволяет преобразовывать числа, строки (но не строки фиксированной длины), даты и дату и время.
Все эти функции принимают один аргумент.

* При преобразовании в строку или из строки значение форматируется или разбирается по тем же правилам, что и для формата TabSeparated (и почти всех других текстовых форматов). Если строку не удаётся разобрать, генерируется исключение, а запрос отменяется.
* При преобразовании дат в числа или наоборот дате соответствует количество дней с начала Unix epoch.
* При преобразовании даты и времени в числа или наоборот дате и времени соответствует количество секунд с начала Unix epoch.
* Функция `toString` для аргумента `DateTime` может принимать второй аргумент типа String, содержащий имя часового пояса, например: `Europe/Amsterdam`. В этом случае время форматируется в соответствии с указанным часовым поясом.

<div id="to-date-and-date-time-functions">
  ## Примечания к функциям `toDate`/`toDateTime`
</div>

Форматы даты и даты со временем для функций `toDate`/`toDateTime` задаются следующим образом:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

В качестве исключения, при преобразовании числовых типов UInt32, Int32, UInt64 или Int64 в Date, если число больше или равно 65536, оно интерпретируется как Unix-временная метка (а не как количество дней) и округляется до даты.
Это позволяет поддерживать распространённый вариант записи `toDate(unix_timestamp)`, который в противном случае приводил бы к ошибке и требовал бы использования более громоздкой конструкции `toDate(toDateTime(unix_timestamp))`.

Преобразование между датой и датой со временем выполняется естественным образом: путём добавления нулевого времени или отбрасывания времени.

При преобразовании между числовыми типами используются те же правила, что и при присваивании между различными числовыми типами в C++.

**Пример**

```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

См. также функцию [`toUnixTimestamp`](/ru/sql-reference/functions/date-time-functions#toUnixTimestamp).

{/* 
  Внутреннее содержимое тегов ниже заменяется на этапе сборки фреймворка документации 
  документацией, сгенерированной из system.functions. Пожалуйста, не изменяйте и не удаляйте эти теги.
  См.: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }