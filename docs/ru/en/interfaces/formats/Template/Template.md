---
alias: []
description: 'Документация по формату Template'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
doc_type: 'guide'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Если вам нужна более гибкая настройка, чем могут предложить другие стандартные форматы,
формат `Template` позволяет указать собственную строку формата с плейсхолдерами для значений,
а также задать правила экранирования данных.

Для него используются следующие настройки:

| Setting                                                                                                                         | Description                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| [`format_template_row`](#format_template_row)                                                                                   | Указывает путь к файлу, содержащему строки формата для строк.                                                                    |
| [`format_template_resultset`](#format_template_resultset)                                                                       | Указывает путь к файлу, содержащему строки формата для строк                                                                     |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                                             | Указывает разделитель между строками, который выводится (или ожидается) после каждой строки, кроме последней (`\n` по умолчанию) |
| `format_template_row_format`                                                                                                    | Указывает строку формата для строк [непосредственно](#inline_specification).                                                     |
| `format_template_resultset_format`                                                                                              | Указывает строку формата для результирующего набора [непосредственно](#inline_specification).                                    |
| Некоторые настройки других форматов (например, `output_format_json_quote_64bit_integers` при использовании экранирования `JSON` |                                                                                                                                  |

<div id="settings-and-escaping-rules">
  ## Настройки и правила экранирования
</div>

<div id="format_template_row">
  ### format_template_row
</div>

Настройка `format_template_row` задаёт путь к файлу, содержащему строки формата для строк в следующем синтаксисе:

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

Где:

| Part of syntax  | Description                                                                                               |
| --------------- | --------------------------------------------------------------------------------------------------------- |
| `delimiter_i`   | Разделитель между значениями (символ `$` можно экранировать как `$$`)                                     |
| `column_i`      | Имя или индекс столбца, значения которого нужно выбрать или вставить (если пусто, столбец будет пропущен) |
| `serializeAs_i` | Правило экранирования для значений столбца                                                                |

Поддерживаются следующие правила экранирования:

| Escaping Rule        | Description                              |
| -------------------- | ---------------------------------------- |
| `CSV`, `JSON`, `XML` | Аналогично форматам с теми же названиями |
| `Escaped`            | Аналогично `TSV`                         |
| `Quoted`             | Аналогично `Values`                      |
| `Raw`                | Без экранирования, аналогично `TSVRaw`   |
| `None`               | Без экранирования — см. примечание ниже  |

:::note
Если правило экранирования не указано, используется `None`. `XML` подходит только для вывода.
:::

Рассмотрим пример. Пусть задана следующая строка формата:

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

Следующие значения будут выводиться (при использовании `SELECT`) или ожидаться (при использовании `INPUT`)
между разделителями столбцов `Search phrase:`, `, count:`, `, ad price: $` и `;` соответственно:

* `s` (с правилом экранирования `Quoted`)
* `c` (с правилом экранирования `Escaped`)
* `p` (с правилом экранирования `JSON`)

Например:

* При выполнении `INSERT` строка ниже соответствует ожидаемому шаблону, и значения `bathroom interior design`, `2166`, `$3` будут прочитаны в столбцы `Search phrase`, `count`, `ad price`.
* При выполнении `SELECT` строка ниже будет выводом, если значения `bathroom interior design`, `2166`, `$3` уже хранятся в таблице в столбцах `Search phrase`, `count`, `ad price`.

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

<div id="format_template_rows_between_delimiter">
  ### format_template_rows_between_delimiter
</div>

Настройка `format_template_rows_between_delimiter` определяет разделитель между строками, который выводится (или ожидается) после каждой строки, кроме последней (`\n` по умолчанию)

<div id="format_template_resultset">
  ### format_template_resultset
</div>

Параметр `format_template_resultset` задаёт путь к файлу, содержащему строку формата для результирующего набора.

Строка формата для результирующего набора имеет тот же синтаксис, что и строка формата для строк.
Она позволяет задать префикс, суффикс и способ вывода дополнительной информации, а также содержит следующие плейсхолдеры вместо имён столбцов:

* `data` — строки с данными в формате `format_template_row`, разделённые `format_template_rows_between_delimiter`. Этот плейсхолдер должен быть первым в строке формата.
* `totals` — строка с итоговыми значениями в формате `format_template_row` (при использовании WITH TOTALS).
* `min` — строка с минимальными значениями в формате `format_template_row` (если `extremes` установлено в 1).
* `max` — строка с максимальными значениями в формате `format_template_row` (если `extremes` установлено в 1).
* `rows` — общее количество строк на выходе.
* `rows_before_limit` — минимальное количество строк, которое было бы без LIMIT. Выводится только если запрос содержит LIMIT. Если запрос содержит GROUP BY, rows&#95;before&#95;limit&#95;at&#95;least — точное количество строк, которое было бы без LIMIT.
* `time` — время выполнения request в секундах.
* `rows_read` — количество прочитанных строк.
* `bytes_read` — количество прочитанных байтов (в несжатом виде).

Для плейсхолдеров `data`, `totals`, `min` и `max` не должно быть указано правило экранирования (или должно быть явно указано `None`). Для остальных плейсхолдеров можно указать любое правило экранирования.

:::note
Если параметр `format_template_resultset` — пустая строка, по умолчанию используется `${data}`.
:::

В формате для запросов вставки можно пропускать некоторые столбцы или поля при наличии префикса или суффикса (см. пример).

<div id="inline_specification">
  ### Встроенная спецификация
</div>

Зачастую развернуть конфигурации формата
(задаваемые с помощью `format_template_row`, `format_template_resultset`) для формата Template в каталог на всех узлах кластера затруднительно или невозможно.
Кроме того, формат может быть настолько простым, что его не нужно выносить в отдельный файл.

В таких случаях можно использовать `format_template_row_format` (для `format_template_row`) и `format_template_resultset_format` (для `format_template_resultset`), чтобы задать строку шаблона непосредственно в запросе,
а не указывать путь к содержащему её файлу.

:::note
Правила для строк формата и escape-последовательностей такие же, как и для:

* [`format_template_row`](#format_template_row) при использовании `format_template_row_format`.
* [`format_template_resultset`](#format_template_resultset) при использовании `format_template_resultset_format`.
  :::

<div id="example-usage">
  ## Пример использования
</div>

Рассмотрим два примера использования формата `Template`: сначала для выборки данных, а затем для вставки данных.

<div id="selecting-data">
  ### Выборка данных
</div>

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

<div id="inserting-data">
  ### Вставка данных
</div>

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` и `Sign` внутри плейсхолдеров — это названия столбцов таблицы. Значения после `Useless field` в строках и после `\nTotal rows:` в суффиксе будут игнорироваться.
Все разделители во входных данных должны строго совпадать с разделителями в указанных строках формата.

<div id="inline_specification">
  ### Встроенная спецификация
</div>

Устали вручную форматировать таблицы Markdown? В этом примере мы рассмотрим, как с помощью формата `Template` и настроек встроенной спецификации решить простую задачу: выбрать с помощью `SELECT` имена некоторых форматов ClickHouse из таблицы `system.formats` и оформить их в виде таблицы Markdown. Это легко сделать с помощью формата `Template` и настроек `format_template_row_format` и `format_template_resultset_format`.

В предыдущих примерах мы задавали строки формата для результирующего набора и строк в отдельных файлах, а пути к этим файлам указывали с помощью настроек `format_template_resultset` и `format_template_row` соответственно. Здесь мы сделаем это встроенно, потому что наш шаблон совсем простой и состоит лишь из нескольких символов `|` и `-`, образующих таблицу Markdown. Строку шаблона для результирующего набора мы зададим с помощью настройки `format_template_resultset_format`. Чтобы создать заголовок таблицы, мы добавили `|ClickHouse Formats|\n|---|\n` перед `${data}`. Для задания строки шаблона ``|`{0:XML}`|`` для наших строк используется настройка `format_template_row_format`. Формат `Template` вставит наши строки в указанном формате в плейсхолдер `${data}`. В этом примере у нас только один столбец, но при желании можно добавить и другие, включив `{1:XML}`, `{2:XML}` и т. д. в строку шаблона строки и выбрав подходящее правило экранирования. В этом примере мы используем правило экранирования `XML`.

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

Смотрите! Нам не пришлось вручную добавлять все эти `|` и `-`, чтобы сделать эту таблицу в Markdown:

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```