# Форматы входных и выходных данных {#formats}

ClickHouse может принимать (`INSERT`) и отдавать (`SELECT`) данные в различных форматах.

Поддерживаемые форматы и возможность использовать их в запросах `INSERT` и `SELECT` перечислены в таблице ниже.

| Формат                                                          | INSERT | SELECT |
|-----------------------------------------------------------------|--------|--------|
| [TabSeparated](#tabseparated)                                   | ✔      | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗      | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔      | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔      | ✔      |
| [Template](#format-template)                                    | ✔      | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔      | ✗      |
| [CSV](#csv)                                                     | ✔      | ✔      |
| [CSVWithNames](#csvwithnames)                                   | ✔      | ✔      |
| [CustomSeparated](#format-customseparated)                      | ✔      | ✔      |
| [Values](#data-format-values)                                   | ✔      | ✔      |
| [Vertical](#vertical)                                           | ✗      | ✔      |
| [JSON](#json)                                                   | ✗      | ✔      |
| [JSONCompact](#jsoncompact)                                     | ✗      | ✔      |
| [JSONEachRow](#jsoneachrow)                                     | ✔      | ✔      |
| [TSKV](#tskv)                                                   | ✔      | ✔      |
| [Pretty](#pretty)                                               | ✗      | ✔      |
| [PrettyCompact](#prettycompact)                                 | ✗      | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗      | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗      | ✔      |
| [PrettySpace](#prettyspace)                                     | ✗      | ✔      |
| [Protobuf](#protobuf)                                           | ✔      | ✔      |
| [Parquet](#data-format-parquet)                                 | ✔      | ✔      |
| [ORC](#data-format-orc)                                         | ✔      | ✗      |
| [RowBinary](#rowbinary)                                         | ✔      | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)       | ✔      | ✔      |
| [Native](#native)                                               | ✔      | ✔      |
| [Null](#null)                                                   | ✗      | ✔      |
| [XML](#xml)                                                     | ✗      | ✔      |
| [CapnProto](#capnproto)                                         | ✔      | ✗      |

Вы можете регулировать некоторые параметры работы с форматами с помощью настроек ClickHouse. За дополнительной информацией обращайтесь к разделу [Настройки](../operations/settings/settings.md).

## TabSeparated {#tabseparated}

В TabSeparated формате данные пишутся по строкам. Каждая строчка содержит значения, разделённые табами. После каждого значения идёт таб, кроме последнего значения в строке, после которого идёт перевод строки. Везде подразумеваются исключительно unix-переводы строк. Последняя строка также обязана содержать перевод строки на конце. Значения пишутся в текстовом виде, без обрамляющих кавычек, с экранированием служебных символов.

Этот формат также доступен под именем `TSV`.

Формат `TabSeparated` удобен для обработки данных произвольными программами и скриптами. Он используется по умолчанию в HTTP-интерфейсе, а также в batch-режиме клиента командной строки. Также формат позволяет переносить данные между разными СУБД. Например, вы можете получить дамп из MySQL и загрузить его в ClickHouse, или наоборот.

Формат `TabSeparated` поддерживает вывод тотальных значений (при использовании WITH TOTALS) и экстремальных значений (при настройке extremes выставленной в 1). В этих случаях, после основных данных выводятся тотальные значения, и экстремальные значения. Основной результат, тотальные значения и экстремальные значения, отделяются друг от друга пустой строкой. Пример:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
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

### Форматирование данных {#formatirovanie-dannykh}

Целые числа пишутся в десятичной форме. Числа могут содержать лишний символ «+» в начале (игнорируется при парсинге, а при форматировании не пишется). Неотрицательные числа не могут содержать знак отрицания. При чтении допустим парсинг пустой строки, как числа ноль, или (для знаковых типов) строки, состоящей из одного минуса, как числа ноль. Числа, не помещающиеся в соответствующий тип данных, могут парсится, как некоторое другое число, без сообщения об ошибке.

Числа с плавающей запятой пишутся в десятичной форме. При этом, десятичный разделитель - точка. Поддерживается экспоненциальная запись, а также inf, +inf, -inf, nan. Запись числа с плавающей запятой может начинаться или заканчиваться на десятичную точку.
При форматировании возможна потеря точности чисел с плавающей запятой.
При парсинге, допустимо чтение не обязательно наиболее близкого к десятичной записи машинно-представимого числа.

Даты выводятся в формате YYYY-MM-DD, парсятся в том же формате, но с любыми символами в качестве разделителей.
Даты-с-временем выводятся в формате YYYY-MM-DD hh:mm:ss, парсятся в том же формате, но с любыми символами в качестве разделителей.
Всё это происходит в системном часовом поясе на момент старта клиента (если клиент занимается форматированием данных) или сервера. Для дат-с-временем не указывается, действует ли daylight saving time. То есть, если в дампе есть времена во время перевода стрелок назад, то дамп не соответствует данным однозначно, и при парсинге будет выбрано какое-либо из двух времён.
При парсинге, некорректные даты и даты-с-временем могут парситься с естественным переполнением или как нулевые даты/даты-с-временем без сообщения об ошибке.

В качестве исключения, поддерживается также парсинг даты-с-временем в формате unix timestamp, если он состоит ровно из 10 десятичных цифр. Результат не зависит от часового пояса. Различение форматов YYYY-MM-DD hh:mm:ss и NNNNNNNNNN делается автоматически.

Строки выводятся с экранированием спецсимволов с помощью обратного слеша. При выводе, используются следующие escape-последовательности: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Парсер также поддерживает последовательности `\a`, `\v`, и `\xHH` (последовательности hex escape) и любые последовательности вида `\c`, где `c` — любой символ (такие последовательности преобразуются в `c`). Таким образом, при чтении поддерживаются форматы, где перевод строки может быть записан как `\n` и как `\` и перевод строки. Например, строка `Hello world`, где между словами вместо пробела стоит перевод строки, может быть считана в любом из следующих вариантов:

``` text
Hello\nworld

Hello\
world
```

Второй вариант поддерживается, так как его использует MySQL при записи tab-separated дампа.

Минимальный набор символов, которых вам необходимо экранировать при передаче в TabSeparated формате: таб, перевод строки (LF) и обратный слеш.

Экранируется лишь небольшой набор символов. Вы можете легко наткнуться на строковое значение, которое испортит ваш терминал при выводе в него.

Массивы форматируются в виде списка значений через запятую в квадратных скобках. Элементы массива - числа форматируются как обычно, а даты, даты-с-временем и строки - в одинарных кавычках с такими же правилами экранирования, как указано выше.

[NULL](../sql-reference/syntax.md) форматируется как `\N`.

Каждый элемент структуры типа [Nested](../sql-reference/data-types/nested-data-structures/nested.md) представляется как отдельный массив.

Например:

``` sql
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

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` text
1 [1] ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

Отличается от формата `TabSeparated` тем, что строки выводятся без экранирования.
Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).

Этот формат также доступен под именем `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Отличается от формата `TabSeparated` тем, что в первой строке пишутся имена столбцов.
При парсинге, первая строка полностью игнорируется. Вы не можете использовать имена столбцов, чтобы указать их порядок расположения, или чтобы проверить их корректность.
(Поддержка обработки заголовка при парсинге может быть добавлена в будущем.)

Этот формат также доступен под именем `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Отличается от формата `TabSeparated` тем, что в первой строке пишутся имена столбцов, а во второй - типы столбцов.
При парсинге, первая и вторая строка полностью игнорируется.

Этот формат также доступен под именем `TSVWithNamesAndTypes`.

## Template {#format-template}

Этот формат позволяет указать произвольную форматную строку, в которую подставляются значения, сериализованные выбранным способом.

Для этого используются настройки `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` и настройки экранирования других форматов (например, `output_format_json_quote_64bit_integers` при экранировании как в `JSON`, см. далее)

Настройка `format_template_row` задаёт путь к файлу, содержащему форматную строку для строк таблицы, которая должна иметь вид:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

    где `delimiter_i` - разделители между значениями (символ `$` в разделителе экранируется как `$$`),
    `column_i` - имена или номера столбцов, значения которых должны быть выведены или считаны (если имя не указано - столбец пропускается),
    `serializeAs_i` - тип экранирования для значений соответствующего столбца. Поддерживаются следующие типы экранирования:

    - `CSV`, `JSON`, `XML` (как в одноимённых форматах)
    - `Escaped` (как в `TSV`)
    - `Quoted` (как в `Values`)
    - `Raw` (без экранирования, как в `TSVRaw`)
    - `None` (тип экранирования отсутствует, см. далее)

    Если для столбца не указан тип экранирования, используется `None`. `XML` и `Raw` поддерживаются только для вывода.

    Так, в форматной строке

    `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

    между разделителями `Search phrase: `, `, count: `, `, ad price: $` и `;` при выводе будут подставлены (при вводе - будут ожидаться) значения столбцов `SearchPhrase`, `c` и `price`, сериализованные как `Quoted`, `Escaped` и `JSON` соответственно, например:

    `Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

Настройка `format_template_rows_between_delimiter` задаёт разделитель между строками, который выводится (или ожмдается при вводе) после каждой строки, кроме последней. По умолчанию `\n`.

Настройка `format_template_resultset` задаёт путь к файлу, содержащему форматную строку для результата. Форматная строка для результата имеет синтаксис аналогичный форматной строке для строк таблицы и позволяет указать префикс, суффикс и способ вывода дополнительной информации. Вместо имён столбцов в ней указываются следующие имена подстановок:

-   `data` - строки с данными в формате `format_template_row`, разделённые `format_template_rows_between_delimiter`. Эта подстановка должна быть первой подстановкой в форматной строке.
-   `totals` - строка с тотальными значениями в формате `format_template_row` (при использовании WITH TOTALS)
-   `min` - строка с минимальными значениями в формате `format_template_row` (при настройке extremes, выставленной в 1)
-   `max` - строка с максимальными значениями в формате `format_template_row` (при настройке extremes, выставленной в 1)
-   `rows` - общее количество выведенных стрчек
-   `rows_before_limit` - не менее скольких строчек получилось бы, если бы не было LIMIT-а. Выводится только если запрос содержит LIMIT. В случае, если запрос содержит GROUP BY, `rows_before_limit` - точное число строк, которое получилось бы, если бы не было LIMIT-а.
-   `time` - время выполнения запроса в секундах
-   `rows_read` - сколько строк было прочитано при выполнении запроса
-   `bytes_read` - сколько байт (несжатых) было прочитано при выполнении запроса

У подстановок `data`, `totals`, `min` и `max` не должны быть указаны типы экранирования (или должен быть указан `None`). Остальные подстановки - это отдельные значения, для них может быть указан любой тип экранирования.
Если строка `format_template_resultset` пустая, то по-умолчанию используется `${data}`.
Из всех перечисленных подстановок форматная строка `format_template_resultset` для ввода может содержать только `data`.
Также при вводе формат поддерживает пропуск значений столбцов и пропуск значений в префиксе и суффиксе (см. пример).

Пример вывода:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

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

`/some/path/row.format`:

    <tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>

Резутьтат:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
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

Пример ввода:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

    Some header\n${data}\nTotal rows: ${:CSV}\n

`/some/path/row.format`:

    Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}

`PageViews`, `UserID`, `Duration` и `Sign` внутри подстановок - имена столбцов в таблице, в которую вставляются данные. Значения после `Useless field` в строках и значение после `\nTotal rows:` в суффиксе будут проигнорированы.
Все разделители во входных данных должны строго соответствовать разделителям в форматных строках.

## TemplateIgnoreSpaces {#templateignorespaces}

Подходит только для ввода. Отличается от формата `Template` тем, что пропускает пробельные символы между разделителями и значениями во входном потоке. Также в этом формате можно указать пустые подстановки с типом экранирования `None` (`${}` или `${:None}`), чтобы разбить разделители на несколько частей, пробелы между которыми должны игнорироваться. Такие подстановки используются только для пропуска пробелов. С помощью этого формата можно считывать `JSON`, если значения столбцов в нём всегда идут в одном порядке в каждой строке. Например, для вставки данных из примера вывода формата [JSON](#json) в таблицу со столбцами `phrase` и `cnt` можно использовать следующий запрос:

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_schema = '{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}',
format_schema_rows = '{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}',
format_schema_rows_between_delimiter = ','
```

## TSKV {#tskv}

Похож на TabSeparated, но выводит значения в формате name=value. Имена экранируются так же, как строки в формате TabSeparated и, дополнительно, экранируется также символ =.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=интерьер ванной комнаты    count()=2166
SearchPhrase=яндекс     count()=1655
SearchPhrase=весна 2014 мода    count()=1549
SearchPhrase=фриформ фото       count()=1480
SearchPhrase=анджелина джоли    count()=1245
SearchPhrase=омск       count()=1112
SearchPhrase=фото собак разных пород    count()=1091
SearchPhrase=дизайн штор        count()=1064
SearchPhrase=баку       count()=1000
```

[NULL](../sql-reference/syntax.md) форматируется как `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1 y=\N
```

При большом количестве маленьких столбцов, этот формат существенно неэффективен, и обычно нет причин его использовать. Впрочем, он не хуже формата JSONEachRow по производительности.

Поддерживается как вывод, так и парсинг данных в этом формате. При парсинге, поддерживается расположение значений разных столбцов в произвольном порядке. Допустимо отсутствие некоторых значений - тогда они воспринимаются как равные значениям по умолчанию. В этом случае в качестве значений по умолчанию используются нули и пустые строки. Сложные значения, которые могут быть заданы в таблице не поддерживаются как значения по умолчанию.

При парсинге, в качестве дополнительного поля, может присутствовать `tskv` без знака равенства и без значения. Это поле игнорируется.

## CSV {#csv}

Формат Comma Separated Values ([RFC](https://tools.ietf.org/html/rfc4180)).

При форматировании, строки выводятся в двойных кавычках. Двойная кавычка внутри строки выводится как две двойные кавычки подряд. Других правил экранирования нет. Даты и даты-с-временем выводятся в двойных кавычках. Числа выводятся без кавычек. Значения разделяются символом-разделителем, по умолчанию — `,`. Символ-разделитель определяется настройкой [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). Строки разделяются unix переводом строки (LF). Массивы сериализуются в CSV следующим образом: сначала массив сериализуется в строку, как в формате TabSeparated, а затем полученная строка выводится в CSV в двойных кавычках. Кортежи в формате CSV сериализуются, как отдельные столбцы (то есть, теряется их вложенность в кортеж).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*По умолчанию — `,`. См. настройку [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) для дополнительной информации.

При парсинге, все значения могут парситься как в кавычках, так и без кавычек. Поддерживаются как двойные, так и одинарные кавычки. Строки также могут быть без кавычек. В этом случае они парсятся до символа-разделителя или перевода строки (CR или LF). В нарушение RFC, в случае парсинга строк не в кавычках, начальные и конечные пробелы и табы игнорируются. В качестве перевода строки, поддерживаются как Unix (LF), так и Windows (CR LF) и Mac OS Classic (LF CR) варианты.

`NULL` форматируется в виде `\N` или `NULL` или пустой неэкранированной строки (см. настройки [input\_format\_csv\_unquoted\_null\_literal\_as\_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) и [input\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

Если установлена настройка [input\_format\_defaults\_for\_omitted\_fields = 1](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) и тип столбца не `Nullable(T)`, то пустые значения без кавычек заменяются значениями по умолчанию для типа данных столбца.

Формат CSV поддерживает вывод totals и extremes аналогично `TabSeparated`.

## CSVWithNames {#csvwithnames}

Выводит также заголовок, аналогично `TabSeparatedWithNames`.

## CustomSeparated {#format-customseparated}

Аналогичен [Template](#format-template), но выводит (или считывает) все столбцы, используя для них правило экранирования из настройки `format_custom_escaping_rule` и разделители из настроек `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` и `format_custom_result_after_delimiter`, а не из форматных строк.
Также существует формат `CustomSeparatedIgnoreSpaces`, аналогичный `TemplateIgnoreSpaces`.

## JSON {#json}

Выводит данные в формате JSON. Кроме таблицы с данными, также выводятся имена и типы столбцов, и некоторая дополнительная информация - общее количество выведенных строк, а также количество строк, которое могло бы быть выведено, если бы не было LIMIT-а. Пример:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

JSON совместим с JavaScript. Для этого, дополнительно экранируются некоторые символы: символ прямого слеша `/` экранируется в виде `\/`; альтернативные переводы строк `U+2028`, `U+2029`, на которых ломаются некоторые браузеры, экранируются в виде `\uXXXX`-последовательностей. Экранируются ASCII control characters: backspace, form feed, line feed, carriage return, horizontal tab в виде `\b`, `\f`, `\n`, `\r`, `\t` соответственно, а также остальные байты из диапазона 00-1F с помощью `\uXXXX`-последовательностей. Невалидные UTF-8 последовательности заменяются на replacement character � и, таким образом, выводимый текст будет состоять из валидных UTF-8 последовательностей. Числа типа UInt64 и Int64, для совместимости с JavaScript, по умолчанию выводятся в двойных кавычках. Чтобы они выводились без кавычек, можно установить конфигурационный параметр [output\_format\_json\_quote\_64bit\_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) равным 0.

`rows` - общее количество выведенных строчек.

`rows_before_limit_at_least` - не менее скольких строчек получилось бы, если бы не было LIMIT-а. Выводится только если запрос содержит LIMIT.
В случае, если запрос содержит GROUP BY, rows\_before\_limit\_at\_least - точное число строк, которое получилось бы, если бы не было LIMIT-а.

`totals` - тотальные значения (при использовании WITH TOTALS).

`extremes` - экстремальные значения (при настройке extremes, выставленной в 1).

Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).

ClickHouse поддерживает [NULL](../sql-reference/syntax.md), который при выводе JSON будет отображен как `null`.

Смотрите также формат [JSONEachRow](#jsoneachrow) .

## JSONCompact {#jsoncompact}

Отличается от JSON только тем, что строчки данных выводятся в массивах, а не в object-ах.

Пример:

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["интерьер ванной комнаты", "2166"],
                ["яндекс", "1655"],
                ["весна 2014 мода", "1549"],
                ["фриформ фото", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).
Смотрите также формат `JSONEachRow`.

## JSONEachRow {#jsoneachrow}

При использовании этого формата, ClickHouse выводит каждую запись как объект JSON (каждый объект отдельной строкой), при этом данные в целом — невалидный JSON.

``` json
{"SearchPhrase":"дизайн штор","count()":"1064"}
{"SearchPhrase":"баку","count()":"1000"}
{"SearchPhrase":"","count":"8267016"}
```

При вставке данных необходимо каждую запись передавать как отдельный объект JSON.

### Вставка данных {#vstavka-dannykh}

    INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}

ClickHouse допускает:

-   Любой порядок пар ключ-значение в объекте.
-   Пропуск отдельных значений.

ClickHouse игнорирует пробелы между элементами и запятые после объектов. Вы можете передать все объекты одной строкой. Вам не нужно разделять их переносами строк.

**Обработка пропущенных значений**

ClickHouse заменяет опущенные значения значениями по умолчанию для соответствующих [data types](../sql-reference/data-types/index.md).

Если указано `DEFAULT expr`, то ClickHouse использует различные правила подстановки в зависимости от настройки [input\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields).

Рассмотрим следующую таблицу:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   Если `input_format_defaults_for_omitted_fields = 0`, то значение по умолчанию для `x` и `a` равняется `0` (поскольку это значение по умолчанию для типа данных `UInt32`.)
-   Если `input_format_defaults_for_omitted_fields = 1`, то значение по умолчанию для `x` равно `0`, а значение по умолчанию `a` равно `x * 2`.

!!! note "Предупреждение"
    Если `input_format_defaults_for_omitted_fields = 1`, то при обработке запросов ClickHouse потребляет больше вычислительных ресурсов, чем если `input_format_defaults_for_omitted_fields = 0`.

### Выборка данных {#vyborka-dannykh}

Рассмотрим в качестве примера таблицу `UserActivity`:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Запрос `SELECT * FROM UserActivity FORMAT JSONEachRow` возвращает:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

В отличие от формата [JSON](#json), для `JSONEachRow` ClickHouse не заменяет невалидные UTF-8 последовательности. Значения экранируются так же, как и для формата `JSON`.

!!! note "Примечание"
    В строках может выводиться произвольный набор байт. Используйте формат `JSONEachRow`, если вы уверены, что данные в таблице могут быть представлены в формате JSON без потери информации.

### Использование вложенных структур {#jsoneachrow-nested}

Если у вас есть таблица со столбцами типа [Nested](../sql-reference/data-types/nested-data-structures/nested.md), то в неё можно вставить данные из JSON-документа с такой же структурой. Функциональность включается настройкой [input\_format\_import\_nested\_json](../operations/settings/settings.md#settings-input_format_import_nested_json).

Например, рассмотрим следующую таблицу:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Из описания типа данных `Nested` видно, что ClickHouse трактует каждый компонент вложенной структуры как отдельный столбец (для нашей таблицы `n.s` и `n.i`). Можно вставить данные следующим образом:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Чтобы вставить данные как иерархический объект JSON, установите [input\_format\_import\_nested\_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Без этой настройки ClickHouse сгенерирует исключение.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Native {#native}

Самый эффективный формат. Данные пишутся и читаются блоками в бинарном виде. Для каждого блока пишется количество строк, количество столбцов, имена и типы столбцов, а затем кусочки столбцов этого блока, один за другим. То есть, этот формат является «столбцовым» - не преобразует столбцы в строки. Именно этот формат используется в родном интерфейсе - при межсерверном взаимодействии, при использовании клиента командной строки, при работе клиентов, написанных на C++.

Вы можете использовать этот формат для быстрой генерации дампов, которые могут быть прочитаны только СУБД ClickHouse. Вряд ли имеет смысл работать с этим форматом самостоятельно.

## Null {#null}

Ничего не выводит. При этом, запрос обрабатывается, а при использовании клиента командной строки, данные ещё и передаются на клиент. Используется для тестов, в том числе, тестов производительности.
Очевидно, формат подходит только для вывода, но не для парсинга.

## Pretty {#pretty}

Выводит данные в виде Unicode-art табличек, также используя ANSI-escape последовательности для установки цветов в терминале.
Рисуется полная сетка таблицы и, таким образом, каждая строчка занимает две строки в терминале.
Каждый блок результата выводится в виде отдельной таблицы. Это нужно, чтобы можно было выводить блоки без буферизации результата (буферизация потребовалась бы, чтобы заранее вычислить видимую ширину всех значений.)

[NULL](../sql-reference/syntax.md) выводится как `ᴺᵁᴸᴸ`.

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

В форматах `Pretty*` строки выводятся без экранирования. Ниже приведен пример для формата [PrettyCompact](#prettycompact):

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and   character │
└──────────────────────────────────────┘
```

Для защиты от вываливания слишком большого количества данных в терминал, выводится только первые 10 000 строк. Если строк больше или равно 10 000, то будет написано «Showed first 10 000.»
Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).

Формат `Pretty` поддерживает вывод тотальных значений (при использовании WITH TOTALS) и экстремальных значений (при настройке extremes выставленной в 1). В этих случаях, после основных данных выводятся тотальные значения, и экстремальные значения, в отдельных табличках. Пример (показан для формата [PrettyCompact](#prettycompact)):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyCompact {#prettycompact}

Отличается от [Pretty](#pretty) тем, что не рисуется сетка между строками - результат более компактный.
Этот формат используется по умолчанию в клиенте командной строки в интерактивном режиме.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Отличается от [PrettyCompact](#prettycompact) тем, что строки (до 10 000 штук) буферизуются и затем выводятся в виде одной таблицы, а не по блокам.

## PrettyNoEscapes {#prettynoescapes}

Отличается от Pretty тем, что не используются ANSI-escape последовательности. Это нужно для отображения этого формата в браузере, а также при использовании утилиты командной строки watch.

Пример:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

Для отображения в браузере, вы можете использовать HTTP интерфейс.

### PrettyCompactNoEscapes {#prettycompactnoescapes}

Аналогично.

### PrettySpaceNoEscapes {#prettyspacenoescapes}

Аналогично.

## PrettySpace {#prettyspace}

Отличается от [PrettyCompact](#prettycompact) тем, что вместо сетки используется пустое пространство (пробелы).

## RowBinary {#rowbinary}

Форматирует и парсит данные по строкам, в бинарном виде. Строки и значения уложены подряд, без разделителей.
Формат менее эффективен, чем формат Native, так как является строковым.

Числа представлены в little endian формате фиксированной длины. Для примера, UInt64 занимает 8 байт.
DateTime представлены как UInt32, содержащий unix timestamp в качестве значения.
Date представлены как UInt16, содержащий количество дней, прошедших с 1970-01-01 в качестве значения.
String представлены как длина в формате varint (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), а затем байты строки.
FixedString представлены просто как последовательность байт.

Array представлены как длина в формате varint (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), а затем элементы массива, подряд.

Для поддержки [NULL](../sql-reference/syntax.md#null-literal) перед каждым значением типа [Nullable](../sql-reference/data-types/nullable.md) следует байт содержащий 1 или 0. Если байт 1, то значение равно NULL, и этот байт интерпретируется как отдельное значение (т.е. после него следует значение следующего поля). Если байт 0, то после байта следует значение поля (не равно NULL).

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

То же самое что [RowBinary](#rowbinary), но добавляется заголовок:

-   Количество колонок - N, закодированное [LEB128](https://en.wikipedia.org/wiki/LEB128),
-   N строк (`String`) с именами колонок,
-   N строк (`String`) с типами колонок.

## Values {#data-format-values}

Выводит каждую строку в скобках. Строки разделены запятыми. После последней строки запятой нет. Значения внутри скобок также разделены запятыми. Числа выводятся в десятичном виде без кавычек. Массивы выводятся в квадратных скобках. Строки, даты, даты-с-временем выводятся в кавычках. Правила экранирования и особенности парсинга аналогичны формату [TabSeparated](#tabseparated). При форматировании, лишние пробелы не ставятся, а при парсинге - допустимы и пропускаются (за исключением пробелов внутри значений типа массив, которые недопустимы). [NULL](../sql-reference/syntax.md) представляется как `NULL`.

Минимальный набор символов, которых вам необходимо экранировать при передаче в Values формате: одинарная кавычка и обратный слеш.

Именно этот формат используется в запросе `INSERT INTO t VALUES ...`, но вы также можете использовать его для форматирования результатов запросов.

## Vertical {#vertical}

Выводит каждое значение на отдельной строке, с указанием имени столбца. Формат удобно использовать для вывода одной-нескольких строк, если каждая строка состоит из большого количества столбцов.

[NULL](../sql-reference/syntax.md) выводится как `ᴺᵁᴸᴸ`.

Пример:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

В формате `Vertical` строки выводятся без экранирования. Например:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and   with some special
 characters
```

Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).

## XML {#xml}

Формат XML подходит только для вывода данных, не для парсинга. Пример:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>интерьер ванной комнаты</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>яндекс</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>весна 2014 мода</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>фриформ фото</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>анджелина джоли</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>омск</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>фото собак разных пород</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>дизайн штор</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>баку</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

Если имя столбца не имеет некоторый допустимый вид, то в качестве имени элемента используется просто field. В остальном, структура XML повторяет структуру в формате JSON.
Как и для формата JSON, невалидные UTF-8 последовательности заменяются на replacement character � и, таким образом, выводимый текст будет состоять из валидных UTF-8 последовательностей.

В строковых значениях, экранируются символы `<` и `&` как `&lt;` и `&amp;`.

Массивы выводятся как `<array><elem>Hello</elem><elem>World</elem>...</array>`,
а кортежи как `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap’n Proto - формат бинарных сообщений, похож на Protocol Buffers и Thrift, но не похож на JSON или MessagePack.

Сообщения Cap’n Proto строго типизированы и не самоописывающиеся, т.е. нуждаются во внешнем описании схемы. Схема применяется «на лету» и кешируется между запросами.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

Где `schema.capnp` выглядит следующим образом:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Десериализация эффективна и обычно не повышает нагрузку на систему.

См. также [схема формата](#formatschema).

## Protobuf {#protobuf}

Protobuf - формат [Protocol Buffers](https://developers.google.com/protocol-buffers/).

Формат нуждается во внешнем описании схемы. Схема кэшируется между запросами.
ClickHouse поддерживает как синтаксис `proto2`, так и `proto3`; все типы полей (repeated/optional/required) поддерживаются.

Пример использования формата:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

или

``` bash
$ cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

Где файл `schemafile.proto` может выглядеть так:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

Соответствие между столбцами таблицы и полями сообщения `Protocol Buffers` устанавливается по имени,
при этом игнорируется регистр букв и символы `_` (подчеркивание) и `.` (точка) считаются одинаковыми.
Если типы столбцов не соответствуют точно типам полей сообщения `Protocol Buffers`, производится необходимая конвертация.

Вложенные сообщения поддерживаются, например, для поля `z` в таком сообщении

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse попытается найти столбец с именем `x.y.z` (или `x_y_z`, или `X.y_Z` и т.п.).
Вложенные сообщения удобно использовать в качестве соответствия для [вложенной структуры данных](../sql-reference/data-types/nested-data-structures/nested.md).

Значения по умолчанию, определённые в схеме `proto2`, например,

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

не применяются; вместо них используются определенные в таблице [значения по умолчанию](../sql-reference/statements/create.md#create-default-values).

ClickHouse пишет и читает сообщения `Protocol Buffers` в формате `length-delimited`. Это означает, что перед каждым сообщением пишется его длина
в формате [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints). См. также [как читать и записывать сообщения Protocol Buffers в формате length-delimited в различных языках программирования](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

## AvroConfluent {#data-format-avro-confluent}

Для формата `AvroConfluent` ClickHouse поддерживает декодирование сообщений `Avro` с одним объектом. Такие сообщения используются с [Kafka] (http://kafka.apache.org/) и  реестром схем [Confluent](https://docs.confluent.io/current/schema-registry/index.html). 

Каждое сообщение `Avro` содержит идентификатор схемы, который может быть разрешен для фактической схемы с помощью реестра схем.

Схемы кэшируются после разрешения.

URL-адрес реестра схем настраивается с помощью [format\_avro\_schema\_registry\_url](../operations/settings/settings.md#format_avro_schema_registry_url).

### Соответствие типов данных {#sootvetstvie-tipov-dannykh-0}

Такое же, как в [Avro](#data-format-avro).

### Использование {#ispolzovanie}

Чтобы быстро проверить разрешение схемы, используйте [kafkacat](https://github.com/edenhill/kafkacat) с языком запросов [clickhouse-local](../operations/utilities/clickhouse-local.md): 

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

Чтобы использовать `AvroConfluent` с [Kafka](../engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```
!!! note "Внимание"
    `format_avro_schema_registry_url` необходимо настроить в `users.xml`, чтобы сохранить значение после перезапуска. Также можно использовать настройку `format_avro_schema_registry_url` табличного движка `Kafka`.

## Parquet {#data-format-parquet}

[Apache Parquet](http://parquet.apache.org/) — формат поколоночного хранения данных, который распространён в экосистеме Hadoop. Для формата `Parquet` ClickHouse поддерживает операции чтения и записи.

### Соответствие типов данных {#sootvetstvie-tipov-dannykh}

Таблица ниже содержит поддерживаемые типы данных и их соответствие [типам данных](../sql-reference/data-types/index.md) ClickHouse для запросов `INSERT` и `SELECT`.

| Тип данных Parquet (`INSERT`) | Тип данных ClickHouse                                     | Тип данных Parquet (`SELECT`) |
|-------------------------------|-----------------------------------------------------------|-------------------------------|
| `UINT8`, `BOOL`               | [UInt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                       |
| `INT8`                        | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                        |
| `UINT16`                      | [UInt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                      |
| `INT16`                       | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                       |
| `UINT32`                      | [UInt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                      |
| `INT32`                       | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                       |
| `UINT64`                      | [UInt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                      |
| `INT64`                       | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                       |
| `FLOAT`, `HALF_FLOAT`         | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                       |
| `DOUBLE`                      | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                      |
| `DATE32`                      | [Date](../sql-reference/data-types/date.md)               | `UINT16`                      |
| `DATE64`, `TIMESTAMP`         | [DateTime](../sql-reference/data-types/datetime.md)       | `UINT32`                      |
| `STRING`, `BINARY`            | [String](../sql-reference/data-types/string.md)           | `STRING`                      |
| —                             | [FixedString](../sql-reference/data-types/fixedstring.md) | `STRING`                      |
| `DECIMAL`                     | [Decimal](../sql-reference/data-types/decimal.md)         | `DECIMAL`                     |

ClickHouse поддерживает настраиваемую точность для формата `Decimal`. При обработке запроса `INSERT`, ClickHouse обрабатывает тип данных Parquet `DECIMAL` как `Decimal128`.

Неподдержанные типы данных Parquet: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Типы данных столбцов в ClickHouse могут отличаться от типов данных соответствующих полей файла в формате Parquet. При вставке данных, ClickHouse интерпретирует типы данных в соответствии с таблицей выше, а затем [приводит](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) данные к тому типу, который установлен для столбца таблицы.

### Вставка и выборка данных {#vstavka-i-vyborka-dannykh}

Чтобы вставить в ClickHouse данные из файла в формате Parquet, выполните команду следующего вида:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

Чтобы получить данные из таблицы ClickHouse и сохранить их в файл формата Parquet, используйте команду следующего вида:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Для обмена данными с экосистемой Hadoop можно использовать движки таблиц [HDFS](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) - это column-oriented формат данных, распространённый в экосистеме Hadoop. Вы можете только вставлять данные этого формата в ClickHouse.

### Соответствие типов данных {#sootvetstvie-tipov-dannykh-1}

Таблица показывает поддержанные типы данных и их соответствие [типам данных](../sql-reference/data-types/index.md) ClickHouse для запросов `INSERT`.

| Тип данных ORC (`INSERT`) | Тип данных ClickHouse                               |
|---------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`           | [UInt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                    | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`                  | [UInt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                   | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`                  | [UInt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                   | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`                  | [UInt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                   | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`     | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`                  | [Float64](../sql-reference/data-types/float.md)     |
| `DATE32`                  | [Date](../sql-reference/data-types/date.md)         |
| `DATE64`, `TIMESTAMP`     | [DateTime](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`        | [String](../sql-reference/data-types/string.md)     |
| `DECIMAL`                 | [Decimal](../sql-reference/data-types/decimal.md)   |

ClickHouse поддерживает настраиваемую точность для формата `Decimal`. При обработке запроса `INSERT`, ClickHouse обрабатывает тип данных Parquet `DECIMAL` как `Decimal128`.

Неподдержанные типы данных ORC: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Типы данных столбцов в таблицах ClickHouse могут отличаться от типов данных для соответствующих полей ORC. При вставке данных, ClickHouse интерпретирует типы данных ORC согласно таблице соответствия, а затем [приводит](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) данные к типу, установленному для столбца таблицы ClickHouse.

### Вставка данных {#vstavka-dannykh-1}

Данные ORC можно вставить в таблицу ClickHouse командой:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Для обмена данных с Hadoop можно использовать [движок таблиц HDFS](../engines/table-engines/integrations/hdfs.md).

## Схема формата {#formatschema}

Имя файла со схемой записывается в настройке `format_schema`. При использовании форматов `Cap'n Proto` и `Protobuf` требуется указать схему.
Схема представляет собой имя файла и имя типа в этом файле, разделенные двоеточием, например `schemafile.proto:MessageType`.
Если файл имеет стандартное расширение для данного формата (например `.proto` для `Protobuf`),
то можно его не указывать и записывать схему так `schemafile:MessageType`.

Если для ввода/вывода данных используется [клиент](../interfaces/cli.md) в [интерактивном режиме](../interfaces/cli.md#cli_usage), то при записи схемы можно использовать абсолютный путь или записывать путь
относительно текущей директории на клиенте. Если клиент используется в [batch режиме](../interfaces/cli.md#cli_usage), то в записи схемы допускается только относительный путь, из соображений безопасности.

Если для ввода/вывода данных используется [HTTP-интерфейс](../interfaces/http.md), то файл со схемой должен располагаться на сервере в каталоге,
указанном в параметре [format\_schema\_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path) конфигурации сервера.

[Оригинальная статья](https://clickhouse.tech/docs/ru/interfaces/formats/) <!--hide-->
