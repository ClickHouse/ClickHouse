---
alias: ['TSV']
description: 'Документация по формату TSV'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     | `TSV`     |

<div id="description">
  ## Описание
</div>

В формате TabSeparated данные записываются построчно. Каждая строка содержит значения, разделённые символами табуляции. За каждым значением следует символ табуляции, кроме последнего значения в строке — после него идёт символ перевода строки. Во всех случаях предполагается строго Unix-стиль перевода строк. Последняя строка также должна оканчиваться символом перевода строки. Значения записываются в текстовом формате, без кавычек, а специальные символы экранируются.

Этот формат также доступен под именем `TSV`.

Формат `TabSeparated` удобен для обработки данных с помощью пользовательских программ и скриптов. Он используется по умолчанию в HTTP interface и в batch mode клиента командной строки. Этот формат также позволяет передавать данные между различными СУБД. Например, можно получить дамп из MySQL и загрузить его в ClickHouse, или наоборот.

Формат `TabSeparated` поддерживает вывод итоговых значений (при использовании WITH TOTALS) и экстремальных значений (когда параметр &#39;extremes&#39; равен 1). В этих случаях итоговые значения и экстремумы выводятся после основных данных. Основной результат, итоговые значения и экстремумы отделяются друг от друга пустой строкой. Пример:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## Форматирование данных
</div>

Целые числа записываются в десятичной форме. Числа могут содержать дополнительный символ &quot;+&quot; в начале (он игнорируется при разборе и не сохраняется при форматировании). Неотрицательные числа не могут содержать знак минус. При чтении допускается разбирать пустую строку как ноль или (для знаковых типов) строку, состоящую только из знака минус, как ноль. Числа, которые не помещаются в соответствующий тип данных, могут быть разобраны как другое значение без сообщения об ошибке.

Числа с плавающей запятой записываются в десятичной форме. В качестве десятичного разделителя используется точка. Поддерживается экспоненциальная запись, а также &#39;inf&#39;, &#39;+inf&#39;, &#39;-inf&#39; и &#39;nan&#39;. Запись числа с плавающей запятой может начинаться или заканчиваться десятичной точкой.
При форматировании для чисел с плавающей запятой возможна потеря точности.
При разборе не требуется обязательно считывать ближайшее число, представимое машиной.

Даты записываются в формате YYYY-MM-DD и разбираются в том же формате, но с любыми символами в качестве разделителей.
Дата и время записываются в формате `YYYY-MM-DD hh:mm:ss` и разбираются в том же формате, но с любыми символами в качестве разделителей.
Все это происходит в системном часовом поясе, действующем на момент запуска клиента или сервера (в зависимости от того, кто именно форматирует данные). Для даты и времени переход на летнее время не определен. Поэтому, если дамп содержит время в период действия летнего времени, он не соответствует данным однозначно, и при разборе будет выбрано одно из двух значений времени.
При чтении некорректные даты и значения даты и времени могут быть разобраны с естественным переполнением или как нулевые даты и время без сообщения об ошибке.

В качестве исключения также поддерживается разбор даты и времени в формате Unix-временной метки, если она состоит ровно из 10 десятичных цифр. Результат не зависит от часового пояса. Форматы `YYYY-MM-DD hh:mm:ss` и `NNNNNNNNNN` различаются автоматически.

Строки выводятся с экранированием специальных символов обратной косой чертой. Для вывода используются следующие escape-последовательности: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. При разборе также поддерживаются последовательности `\a`, `\v` и `\xHH` (шестнадцатеричные escape-последовательности), а также любые последовательности `\c`, где `c` — любой символ (эти последовательности преобразуются в `c`). Таким образом, чтение данных поддерживает форматы, в которых символ перевода строки может быть записан как `\n`, как `\` или как сам символ перевода строки. Например, строка `Hello world` с символом перевода строки между словами вместо пробела может быть разобрана в любом из следующих вариантов:

```text
Hello\nworld

Hello\
world
```

Второй вариант поддерживается, поскольку MySQL использует его при записи дампов в формате с разделителями табуляции.

Минимальный набор символов, которые нужно экранировать при передаче данных в формате TabSeparated: табуляция, перевод строки (LF) и обратная косая черта.

Экранируется лишь небольшой набор символов. Поэтому вам легко может попасться строковое значение, вывод которого терминал исказит.

Массивы записываются как список значений в `[]`, разделённых запятыми. Числовые элементы массива форматируются как обычно. Типы `Date` и `DateTime` записываются в одинарных кавычках. Строки также записываются в одинарных кавычках с теми же правилами экранирования, что и выше.

[NULL](/ru/sql-reference/syntax.md) форматируется в соответствии с настройкой [format&#95;tsv&#95;null&#95;representation](/ru/operations/settings/settings-formats.md/#format_tsv_null_representation) (значение по умолчанию — `\N`).

Во входных данных значения ENUM могут быть представлены как именами, так и идентификаторами. Сначала мы пытаемся сопоставить входное значение с именем ENUM. Если это не удаётся и входное значение является числом, мы пытаемся сопоставить это число с идентификатором ENUM.
Если входные данные содержат только идентификаторы ENUM, для оптимизации разбора ENUM рекомендуется включить настройку [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/ru/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number).

Каждый элемент структур [Nested](/ru/sql-reference/data-types/nested-data-structures/index.md) представляется в виде массива.

Например:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-data">
  ### Вставка данных
</div>

Используйте следующий файл в формате TSV с именем `football.tsv`:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Вставьте данные:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### Чтение данных
</div>

Прочитайте данные в формате `TabSeparated`:

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

Вывод будет в формате с разделителями-табуляцией:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                                                | Описание                                                                                                                                                                                                                                                                                                   | По умолчанию |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| [`format_tsv_null_representation`](/ru/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Настраиваемое представление значения NULL в формате TSV.                                                                                                                                                                                                                                                   | `\N`         |
| [`input_format_tsv_empty_as_default`](/ru/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | считать пустые поля во входных данных TSV значениями по умолчанию. Для сложных выражений со значениями по умолчанию также должен быть включен параметр [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ru/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields). | `false`      |
| [`input_format_tsv_enum_as_number`](/ru/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | считать вставленные значения enum в форматах TSV индексами enum.                                                                                                                                                                                                                                           | `false`      |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/ru/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | использовать дополнительные приемы и эвристики для определения схемы в формате TSV. Если параметр отключен, все поля будут определены как Strings.                                                                                                                                                         | `true`       |
| [`output_format_tsv_crlf_end_of_line`](/ru/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | если установлено значение true, окончанием строки в выходном формате TSV будет `\r\n` вместо `\n`.                                                                                                                                                                                                         | `false`      |
| [`input_format_tsv_crlf_end_of_line`](/ru/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | если установлено значение true, окончанием строки во входном формате TSV будет `\r\n` вместо `\n`.                                                                                                                                                                                                         | `false`      |
| [`input_format_tsv_skip_first_lines`](/ru/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | пропускать указанное количество строк в начале данных.                                                                                                                                                                                                                                                     | `0`          |
| [`input_format_tsv_detect_header`](/ru/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | автоматически определять заголовок с именами и типами в формате TSV.                                                                                                                                                                                                                                       | `true`       |
| [`input_format_tsv_skip_trailing_empty_lines`](/ru/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | пропускать пустые строки в конце данных.                                                                                                                                                                                                                                                                   | `false`      |
| [`input_format_tsv_allow_variable_number_of_columns`](/ru/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | разрешить переменное количество столбцов в формате TSV, игнорировать лишние столбцы и использовать значения по умолчанию для отсутствующих столбцов.                                                                                                                                                       | `false`      |