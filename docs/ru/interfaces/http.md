# HTTP-интерфейс {#http-interface}

HTTP интерфейс позволяет использовать ClickHouse на любой платформе, из любого языка программирования. У нас он используется для работы из Java и Perl, а также из shell-скриптов. В других отделах, HTTP интерфейс используется из Perl, Python и Go. HTTP интерфейс более ограничен по сравнению с родным интерфейсом, но является более совместимым.

По умолчанию, clickhouse-server слушает HTTP на порту 8123 (это можно изменить в конфиге).
Если запросить GET / без параметров, то вернётся строка заданная с помощью настройки [http\_server\_default\_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response). Значение по умолчанию «Ok.» (с переводом строки на конце).

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

В скриптах проверки доступности вы можете использовать GET /ping без параметров. Если сервер доступен всегда возвращается «Ok.» (с переводом строки на конце).

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

Запрос отправляется в виде URL параметра с именем query. Или как тело запроса при использовании метода POST.
Или начало запроса в URL параметре query, а продолжение POST-ом (зачем это нужно, будет объяснено ниже). Размер URL ограничен 16KB, это следует учитывать при отправке больших запросов.

В случае успеха, вам вернётся код ответа 200 и результат обработки запроса в теле ответа.
В случае ошибки, вам вернётся код ответа 500 и текст с описанием ошибки в теле ответа.

При использовании метода GET, выставляется настройка readonly. То есть, для запросов, модифицирующие данные, можно использовать только метод POST. Сам запрос при этом можно отправлять как в теле POST-а, так и в параметре URL.

Примеры:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

Как видно, curl немного неудобен тем, что надо URL-эскейпить пробелы.
Хотя wget сам всё эскейпит, но его не рекомендуется использовать, так как он плохо работает по HTTP 1.1 при использовании keep-alive и Transfer-Encoding: chunked.

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

Если часть запроса отправляется в параметре, а часть POST-ом, то между этими двумя кусками данных ставится перевод строки.
Пример (так работать не будет):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

По умолчанию, данные возвращаются в формате TabSeparated (подробнее смотри раздел «Форматы»).
Можно попросить любой другой формат - с помощью секции FORMAT запроса.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

Возможность передавать данные POST-ом нужна для INSERT-запросов. В этом случае вы можете написать начало запроса в параметре URL, а вставляемые данные передать POST-ом. Вставляемыми данными может быть, например, tab-separated дамп, полученный из MySQL. Таким образом, запрос INSERT заменяет LOAD DATA LOCAL INFILE из MySQL.

Примеры:
Создаём таблицу:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

Используем привычный запрос INSERT для вставки данных:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

Данные можно отправить отдельно от запроса:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

Можно указать любой формат для данных. Формат Values - то же, что используется при записи INSERT INTO t VALUES:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

Можно вставить данные из tab-separated дампа, указав соответствующий формат:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

Прочитаем содержимое таблицы. Данные выводятся в произвольном порядке из-за параллельной обработки запроса:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

Удаляем таблицу.

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

Для запросов, которые не возвращают таблицу с данными, в случае успеха, выдаётся пустое тело ответа.

Вы можете использовать внутренний формат сжатия Clickhouse при передаче данных. Формат сжатых данных нестандартный, и вам придётся использовать для работы с ним специальную программу `clickhouse-compressor` (устанавливается вместе с пакетом `clickhouse-client`). Для повышения эффективности вставки данных можно отключить проверку контрольной суммы на стороне сервера с помощью настройки[http\_native\_compression\_disable\_checksumming\_on\_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress).

Если вы указали `compress = 1` в URL, то сервер сжимает данные, которые он отправляет.
Если вы указали `decompress = 1` в URL, сервер распаковывает те данные, которые вы передаёте методом `POST`.

Также, можно использовать [HTTP compression](https://en.wikipedia.org/wiki/HTTP_compression). Для отправки сжатого запроса `POST`, добавьте заголовок `Content-Encoding: compression_method`. Чтобы ClickHouse сжимал ответ, добавьте заголовок `Accept-Encoding: compression_method`. ClickHouse поддерживает следующие [методы сжатия](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens): `gzip`, `br`, and `deflate`. Чтобы включить HTTP compression, используйте настройку ClickHouse [enable\_http\_compression](../operations/settings/settings.md#settings-enable_http_compression). Уровень сжатия данных для всех методов сжатия можно настроить с помощью настройки [http\_zlib\_compression\_level](#settings-http_zlib_compression_level).

Это может быть использовано для уменьшения трафика по сети при передаче большого количества данных, а также для создания сразу сжатых дампов.

Примеры отправки данных со сжатием:

``` bash
$ #Отправка данных на сервер:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

$ #Отправка данных клиенту:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "Примечание"
    Некоторые HTTP-клиенты могут по умолчанию распаковывать данные (`gzip` и `deflate`) с сервера в фоновом режиме и вы можете получить распакованные данные, даже если правильно используете настройки сжатия.

В параметре URL database может быть указана БД по умолчанию.

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

По умолчанию используется БД, которая прописана в настройках сервера, как БД по умолчанию. По умолчанию, это - БД default. Также вы всегда можете указать БД через точку перед именем таблицы.

Имя пользователя и пароль могут быть указаны в одном из трёх вариантов:

1.  С использованием HTTP Basic Authentication. Пример:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  В параметрах URL user и password. Пример:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  С использованием заголовков ‘X-ClickHouse-User’ и ‘X-ClickHouse-Key’. Пример:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

Если пользователь не задан,то используется `default`. Если пароль не задан, то используется пустой пароль.
Также в параметрах URL вы можете указать любые настройки, которые будут использованы для обработки одного запроса, или целые профили настроек. Пример:http://localhost:8123/?profile=web&max\_rows\_to\_read=1000000000&query=SELECT+1

Подробнее смотрите в разделе [Настройки](../operations/settings/index.md).

``` bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

Об остальных параметрах смотри раздел «SET».

Аналогично можно использовать ClickHouse-сессии в HTTP-протоколе. Для этого необходимо добавить к запросу GET параметр `session_id`. В качестве идентификатора сессии можно использовать произвольную строку. По умолчанию через 60 секунд бездействия сессия будет прервана. Можно изменить этот таймаут, изменяя настройку `default_session_timeout` в конфигурации сервера, или добавив к запросу GET параметр `session_timeout`. Статус сессии можно проверить с помощью параметра `session_check=1`. В рамках одной сессии одновременно может исполняться только один запрос.

Прогресс выполнения запроса можно отслеживать с помощью заголовков ответа `X-ClickHouse-Progress`. Для этого включите [send\_progress\_in\_http\_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). Пример последовательности заголовков:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

Возможные поля заголовка:

-   `read_rows` — количество прочитанных строк.
-   `read_bytes` — объём прочитанных данных в байтах.
-   `total_rows_to_read` — общее количество строк для чтения.
-   `written_rows` — количество записанных строк.
-   `written_bytes` — объём прочитанных данных в байтах.

Запущенные запросы не останавливаются автоматически при разрыве HTTP соединения. Парсинг и форматирование данных производится на стороне сервера и использование сети может быть неэффективным.
Может быть передан необязательный параметр query\_id - идентификатор запроса, произвольная строка. Подробнее смотрите раздел «Настройки, replace\_running\_query».

Может быть передан необязательный параметр quota\_key - ключ квоты, произвольная строка. Подробнее смотрите раздел «Квоты».

HTTP интерфейс позволяет передать внешние данные (внешние временные таблицы) для использования запроса. Подробнее смотрите раздел «Внешние данные для обработки запроса»

## Буферизация ответа {#buferizatsiia-otveta}

Существует возможность включить буферизацию ответа на стороне сервера. Для этого предусмотрены параметры URL `buffer_size` и `wait_end_of_query`.

`buffer_size` определяет количество байт результата которые будут буферизованы в памяти сервера. Если тело результата больше этого порога, то буфер будет переписан в HTTP канал, а оставшиеся данные будут отправляться в HTTP-канал напрямую.

Чтобы гарантировать буферизацию всего ответа необходимо выставить `wait_end_of_query=1`. В этом случае данные, не поместившиеся в памяти, будут буферизованы во временном файле сервера.

Пример:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

Буферизация позволяет избежать ситуации когда код ответа и HTTP-заголовки были отправлены клиенту, после чего возникла ошибка выполнения запроса. В такой ситуации сообщение об ошибке записывается в конце тела ответа, и на стороне клиента ошибка может быть обнаружена только на этапе парсинга.

### Запросы с параметрами {#cli-queries-with-parameters}

Можно создать запрос с параметрами и передать для них значения из соответствующих параметров HTTP-запроса. Дополнительную информацию смотрите в [Запросы с параметрами для консольного клиента](cli.md#cli-queries-with-parameters).

### Пример {#primer}

``` bash
$ curl -sS "<address>?param_id=2¶m_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/interfaces/http_interface/) <!--hide-->
