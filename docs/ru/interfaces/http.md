---
toc_priority: 19
toc_title: "HTTP-интерфейс"
---

# HTTP-интерфейс {#http-interface}

HTTP интерфейс позволяет использовать ClickHouse на любой платформе, из любого языка программирования. У нас он используется для работы из Java и Perl, а также из shell-скриптов. В других отделах HTTP интерфейс используется из Perl, Python и Go. HTTP интерфейс более ограничен по сравнению с родным интерфейсом, но является более совместимым.

По умолчанию `clickhouse-server` слушает HTTP на порту 8123 (это можно изменить в конфиге).
Если запросить `GET /` без параметров, то вернётся строка заданная с помощью настройки [http_server_default_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response). Значение по умолчанию «Ok.» (с переводом строки на конце).

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

Веб-интерфейс доступен по адресу: `http://localhost:8123/play`.

![Веб-интерфейс](../images/play.png)

В скриптах проверки доступности вы можете использовать `GET /ping` без параметров. Если сервер доступен, всегда возвращается «Ok.» (с переводом строки на конце).

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

Запрос отправляется в виде URL параметра с именем `query`. Или как тело запроса при использовании метода POST.
Или начало запроса в URL параметре query, а продолжение POST-ом (зачем это нужно, будет объяснено ниже). Размер URL ограничен 16KB, это следует учитывать при отправке больших запросов.

В случае успеха возвращается код ответа 200 и результат обработки запроса в теле ответа, в случае ошибки — код ответа 500 и текст с описанием ошибки в теле ответа.

При использовании метода GET выставляется настройка readonly. То есть, для запросов, модифицирующих данные, можно использовать только метод POST. Сам запрос при этом можно отправлять как в теле POST запроса, так и в параметре URL.

Примеры:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -nv -O- 'http://localhost:8123/?query=SELECT 1'
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

Как видно, `curl` немного неудобен тем, что надо URL-эскейпить пробелы.
Хотя `wget` сам всё эскейпит, но его не рекомендуется использовать, так как он плохо работает по HTTP 1.1 при использовании `keep-alive` и `Transfer-Encoding: chunked`.

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

Если часть запроса отправляется в параметре, а часть POST запросом, то между этими двумя кусками данных ставится перевод строки.
Пример (так работать не будет):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

По умолчанию данные возвращаются в формате [TabSeparated](formats.md#tabseparated).

Можно указать любой другой формат с помощью секции FORMAT запроса.

Кроме того, вы можете использовать параметр URL-адреса `default_format` или заголовок `X-ClickHouse-Format`, чтобы указать формат по умолчанию, отличный от `TabSeparated`.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

Возможность передавать данные с помощью POST нужна для запросов `INSERT`. В этом случае вы можете написать начало запроса в параметре URL, а вставляемые данные передать POST запросом. Вставляемыми данными может быть, например, tab-separated дамп, полученный из MySQL. Таким образом, запрос `INSERT` заменяет `LOAD DATA LOCAL INFILE` из MySQL.

**Примеры**

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

Для запросов, которые не возвращают таблицу с данными, в случае успеха выдаётся пустое тело ответа.


## Сжатие {#compression}

Сжатие можно использовать для уменьшения трафика по сети при передаче большого количества данных, а также для создания сразу сжатых дампов.

Вы можете использовать внутренний формат сжатия Clickhouse при передаче данных. Формат сжатых данных нестандартный, и вам придётся использовать для работы с ним специальную программу `clickhouse-compressor`. Она устанавливается вместе с пакетом `clickhouse-client`. Для повышения эффективности вставки данных можно отключить проверку контрольной суммы на стороне сервера с помощью настройки [http_native_compression_disable_checksumming_on_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress).

Если вы указали `compress=1` в URL, то сервер сжимает данные, которые он отправляет. Если вы указали `decompress=1` в URL, сервер распаковывает те данные, которые вы передаёте методом `POST`.

Также можно использовать [сжатие HTTP](https://en.wikipedia.org/wiki/HTTP_compression). ClickHouse поддерживает следующие [методы сжатия](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens):

- `gzip`
- `br`
- `deflate`
- `xz`

Для отправки сжатого запроса `POST` добавьте заголовок `Content-Encoding: compression_method`.
Чтобы ClickHouse сжимал ответ, разрешите сжатие настройкой [enable_http_compression](../operations/settings/settings.md#settings-enable_http_compression) и добавьте заголовок `Accept-Encoding: compression_method`. Уровень сжатия данных для всех методов сжатия можно задать с помощью настройки [http_zlib_compression_level](../operations/settings/settings.md#settings-http_zlib_compression_level).

!!! note "Примечание"
    Некоторые HTTP-клиенты могут по умолчанию распаковывать данные (`gzip` и `deflate`) с сервера в фоновом режиме и вы можете получить распакованные данные, даже если правильно используете настройки сжатия.

**Примеры**

``` bash
# Отправка сжатых данных на сервер
$ echo "SELECT 1" | gzip -c | \
  curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

``` bash
# Получение сжатых данных с сервера
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" \
    -H 'Accept-Encoding: gzip' --output result.gz -d 'SELECT number FROM system.numbers LIMIT 3'
$ zcat result.gz
0
1
2
```

## База данных по умолчанию {#default-database}

Вы можете использовать параметр URL `database` или заголовок `X-ClickHouse-Database`, чтобы указать БД по умолчанию.

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
Также в параметрах URL вы можете указать любые настройки, которые будут использованы для обработки одного запроса, или целые профили настроек. Пример:http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1

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

Прогресс выполнения запроса можно отслеживать с помощью заголовков ответа `X-ClickHouse-Progress`. Для этого включите [send_progress_in_http_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). Пример последовательности заголовков:

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
-   `written_bytes` — объём записанных данных в байтах.

Запущенные запросы не останавливаются автоматически при разрыве HTTP соединения. Парсинг и форматирование данных производится на стороне сервера и использование сети может быть неэффективным.
Может быть передан необязательный параметр query_id - идентификатор запроса, произвольная строка. Подробнее смотрите раздел «Настройки, replace_running_query».

Может быть передан необязательный параметр quota_key - ключ квоты, произвольная строка. Подробнее смотрите раздел «Квоты».

HTTP интерфейс позволяет передать внешние данные (внешние временные таблицы) для использования запроса. Подробнее смотрите раздел «Внешние данные для обработки запроса»

## Буферизация ответа {#response-buffering}

Существует возможность включить буферизацию ответа на стороне сервера. Для этого предусмотрены параметры URL `buffer_size` и `wait_end_of_query`.

`buffer_size` определяет количество байт результата которые будут буферизованы в памяти сервера. Если тело результата больше этого порога, то буфер будет переписан в HTTP канал, а оставшиеся данные будут отправляться в HTTP-канал напрямую.

Чтобы гарантировать буферизацию всего ответа, необходимо выставить `wait_end_of_query=1`. В этом случае данные, не поместившиеся в памяти, будут буферизованы во временном файле сервера.

Пример:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

Буферизация позволяет избежать ситуации, когда код ответа и HTTP-заголовки были отправлены клиенту, после чего возникла ошибка выполнения запроса. В такой ситуации сообщение об ошибке записывается в конце тела ответа, и на стороне клиента ошибка может быть обнаружена только на этапе парсинга.

### Запросы с параметрами {#cli-queries-with-parameters}

Можно создать запрос с параметрами и передать для них значения из соответствующих параметров HTTP-запроса. Дополнительную информацию смотрите в [Запросы с параметрами для консольного клиента](cli.md#cli-queries-with-parameters).

### Пример {#primer}

``` bash
$ curl -sS "http://localhost:8123/?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## Предопределенный HTTP интерфейс {#predefined_http_interface}

ClickHouse поддерживает определенные запросы через HTTP-интерфейс. Например, вы можете записать данные в таблицу следующим образом:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse также поддерживает предопределенный HTTP-интерфейс, который может помочь вам легче интегрироваться со сторонними инструментами, такими как [Prometheus exporter](https://github.com/percona-lab/clickhouse_exporter).

Пример:

-   Прежде всего, добавьте раздел в конфигурационный файл сервера:

<!-- -->

``` xml
<http_handlers>
    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </handler>
    </rule>
    <rule>...</rule>
    <rule>...</rule>
</http_handlers>
```

-   Теперь вы можете напрямую запросить URL-адрес для получения данных в формате Prometheus:

<!-- -->

``` bash
$ curl -v 'http://localhost:8123/predefined_query'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /predefined_query HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Tue, 28 Apr 2020 08:52:56 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-mloy5trc
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: 96fe0052-01e6-43ce-b12a-6b7370de6e8a
< X-ClickHouse-Format: Template
< X-ClickHouse-Timezone: Asia/Shanghai
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact

* Connection #0 to host localhost left intact
```

Как вы можете видеть из примера, `http_handlers` настраивается в файле config.xml и может содержать несколько правил. ClickHouse будет сопоставлять полученные HTTP-запросы с предопределенным типом в правиле, и первое совпадение запустит обработчик. Затем ClickHouse выполнит соответствующий предопределенный запрос.

В настоящий момент с помощью `rule` можно настроить `method`, `headers`, `url`, `handler`:
 -  `method` отвечает за соответствие метода HTTP-запроса. `method` соответствует методу [method](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) протокола HTTP. Это необязательная настройка. Если она не определена в файле конфигурации, она не соответствует методу HTTP-запроса.

 -  `url` отвечает за соответствие URL HTTP-запроса. Она совместима с регулярными выражениями [RE2](https://github.com/google/re2). Это необязательная настройка. Если она не определена в файле конфигурации, она не соответствует URL-адресу HTTP-запроса.

 -  `headers` отвечают за соответствие заголовка HTTP-запроса. Она совместим с регулярными выражениями RE2. Это необязательная настройка. Если она не определен в файле конфигурации, она не соответствует заголовку HTTP-запроса.

 -  `handler` содержит основную часть обработчика. Сейчас `handler` может настраивать `type`, `status`, `content_type`, `response_content`, `query`, `query_param_name`.
 `type` на данный момент поддерживает три типа: [predefined_query_handler](#predefined_query_handler), [dynamic_query_handler](#dynamic_query_handler), [static](#static).

    -   `query` — используется с типом `predefined_query_handler`, выполняет запрос при вызове обработчика.

    -   `query_param_name` — используется с типом `dynamic_query_handler`, извлекает и выполняет значение, соответствующее значению `query_param_name` в параметрах HTTP-запроса.

    -   `status` — используется с типом `static`, возвращает код состояния ответа.

    -   `content_type` — используется с типом `static`, возвращает [content-type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).

    -   `response_content` — используется с типом`static`, содержимое ответа, отправленное клиенту, при использовании префикса ‘file://’ or ‘config://’, находит содержимое из файла или конфигурации, отправленного клиенту.

Далее приведены методы настройки для различных типов.

### predefined_query_handler {#predefined_query_handler}

`predefined_query_handler` поддерживает настройки `Settings` и `query_params` значений. Вы можете настроить запрос в типе `predefined_query_handler`.

Значение `query` — это предопределенный запрос `predefined_query_handler`, который выполняется ClickHouse при совпадении HTTP-запроса и возврате результата запроса. Это обязательная настройка.

В следующем примере определяются настройки [max_threads](../operations/settings/settings.md#settings-max_threads) и `max_alter_threads`, а затем запрашивается системная таблица, чтобы проверить, были ли эти параметры успешно установлены.

Пример:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "Предупреждение"
    В одном `predefined_query_handler` поддерживается только один запрос типа `INSERT`.

### dynamic_query_handler {#dynamic_query_handler}

В `dynamic_query_handler`, запрос пишется в виде параметров HTTP-запроса. Разница в том, что в `predefined_query_handler`, запрос записывается в конфигурационный файл. Вы можете настроить `query_param_name` в `dynamic_query_handler`.

ClickHouse извлекает и выполняет значение, соответствующее значению `query_param_name` URL-адресе HTTP-запроса. Значение по умолчанию `query_param_name` — это `/query` . Это необязательная настройка. Если в файле конфигурации нет определения, параметр не передается.

Чтобы поэкспериментировать с этой функциональностью, в примере определяются значения [max_threads](../operations/settings/settings.md#settings-max_threads) и `max_alter_threads` и запрашивается, успешно ли были установлены настройки.

Пример:

``` xml
<http_handlers>
    <rule>
    <headers>
        <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>    </headers>
    <handler>
        <type>dynamic_query_handler</type>
        <query_param_name>query_param</query_param_name>
    </handler>
    </rule>
</http_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_alter_threads=2&param_name_1=max_threads&param_name_2=max_alter_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_alter_threads   2
```

### static {#static}

`static` может возвращать [content_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [status](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) и `response_content`. `response_content` может возвращать конкретное содержимое.

Пример:

Возвращает сообщение.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ curl -vv  -H 'XXX:xxx' 'http://localhost:8123/hi'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /hi HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 402 Payment Required
< Date: Wed, 29 Apr 2020 03:51:26 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

Находит содержимое настроек отправленных клиенту.

``` xml
<get_config_static_handler><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></get_config_static_handler>

<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_config_static_handler</url>
            <handler>
                <type>static</type>
                <response_content>config://get_config_static_handler</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ curl -v  -H 'XXX:xxx' 'http://localhost:8123/get_config_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_config_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:01:24 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

Находит содержимое файла, отправленного клиенту.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file:///absolute_path_file.html</response_content>
            </handler>
        </rule>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_relative_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>file://./relative_path_file.html</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
$ user_files_path='/var/lib/clickhouse/user_files'
$ sudo echo "<html><body>Relative Path File</body></html>" > $user_files_path/relative_path_file.html
$ sudo echo "<html><body>Absolute Path File</body></html>" > $user_files_path/absolute_path_file.html
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_absolute_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_absolute_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:16 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Absolute Path File</body></html>
* Connection #0 to host localhost left intact
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_relative_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_relative_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:31 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```
