# URL(URL, Format) {#table_engines-url}

Управляет данными на удаленном HTTP/HTTPS сервере. Данный движок похож
на движок [File](file.md).

## Использование движка в сервере ClickHouse {#ispolzovanie-dvizhka-v-servere-clickhouse}

`Format` должен быть таким, который ClickHouse может использовать в запросах
`SELECT` и, если есть необходимость, `INSERT`. Полный список поддерживаемых форматов смотрите в
разделе [Форматы](../../../interfaces/formats.md#formats).

`URL` должен соответствовать структуре Uniform Resource Locator. По указанному URL должен находится сервер
работающий по протоколу HTTP или HTTPS. При этом не должно требоваться никаких
дополнительных заголовков для получения ответа от сервера.

Запросы `INSERT` и `SELECT` транслируются в `POST` и `GET` запросы
соответственно. Для обработки `POST`-запросов удаленный сервер должен поддерживать
[Chunked transfer encoding](https://ru.wikipedia.org/wiki/Chunked_transfer_encoding).

Максимальное количество переходов по редиректам при выполнении HTTP-запроса методом GET можно ограничить с помощью настройки [max\_http\_get\_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects).

**Пример:**

**1.** Создадим на сервере таблицу `url_engine_table`:

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Создадим простейший http-сервер стандартными средствами языка python3 и
запустим его:

``` python3
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

``` bash
$ python3 server.py
```

**3.** Запросим данные:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## Особенности использования {#osobennosti-ispolzovaniia}

-   Поддерживается многопоточное чтение и запись.
-   Не поддерживается:
    -   использование операций `ALTER` и `SELECT...SAMPLE`;
    -   индексы;
    -   репликация.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/url/) <!--hide-->
