---
description: 'Выполняет запросы данных к удаленному HTTP/HTTPS-серверу и от него. Этот движок похож
  на движок File.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'движок таблицы URL'
doc_type: 'reference'
---

Выполняет запросы данных к удаленному HTTP/HTTPS-серверу и от него. Этот движок похож на [File](../../../engines/table-engines/special/file.md).

Синтаксис: `URL(URL [,Format] [,CompressionMethod])`

* Параметр `URL` должен соответствовать структуре Uniform Resource Locator. Для URL `http`/`https` (backend по умолчанию) он должен указывать на сервер, использующий HTTP или HTTPS, при этом для получения ответа от сервера не должны требоваться дополнительные заголовки. URL с распознаваемой схемой, отличной от HTTP (`file://`, `s3://`, `az://`, `hdfs://`, …), вместо этого передается соответствующему движку — см. [Маршрутизация по схеме URL](#scheme-dispatch) ниже.

* `Format` должен быть таким, который ClickHouse может использовать в запросах `SELECT` и, при необходимости, при `INSERT`. Полный список поддерживаемых форматов см. в разделе [Formats](/ru/interfaces/formats#formats-overview).

  Если этот аргумент не указан, ClickHouse автоматически определяет формат по суффиксу параметра `URL`. Если суффикс параметра `URL` не соответствует ни одному из поддерживаемых форматов, создать таблицу не удастся. Например, для выражения движка `URL('http://localhost/test.json')` применяется формат `JSON`.

* `CompressionMethod` указывает, должно ли HTTP body быть сжато. Если сжатие включено, HTTP-пакеты, отправляемые движком URL, содержат заголовок `Content-Encoding`, указывающий, какой метод сжатия используется.

Чтобы включить сжатие, сначала убедитесь, что удаленная HTTP-конечная точка, указанная в параметре `URL`, поддерживает соответствующий алгоритм сжатия.

Поддерживаемый `CompressionMethod` должен быть одним из следующих:

* gzip or gz
* deflate
* brotli or br
* lzma or xz
* zstd or zst
* lz4
* bz2
* snappy
* none
* auto

Если `CompressionMethod` не указан, по умолчанию используется `auto`. Это означает, что ClickHouse автоматически определяет метод сжатия по суффиксу параметра `URL`. Если суффикс соответствует любому из перечисленных выше методов сжатия, применяется соответствующее сжатие, в противном случае сжатие не используется.

Например, для выражения движка `URL('http://localhost/test.gzip')` применяется метод сжатия `gzip`, а для `URL('http://localhost/test.fr')` сжатие не используется, потому что суффикс `fr` не соответствует ни одному из перечисленных выше методов сжатия.

<div id="scheme-dispatch">
  ## Маршрутизация по схеме URL
</div>

Движок `URL` — это унифицированная обёртка над другими движками для файловых и объектных хранилищ: в зависимости от схемы URL он направляет запрос к нужному backend. Схемы `http`/`https` (а также любая нераспознанная схема) обрабатываются самим движком `URL`; `file://` — движком [File](../../../engines/table-engines/special/file.md); `s3://`, `gs://`, `gcs://`, `oss://` — движком [S3](/ru/engines/table-engines/integrations/s3); `az://`, `azure://`, `abfss://`, `abfs://` — движком [AzureBlobStorage](/ru/engines/table-engines/integrations/azureBlobStorage); а `hdfs://` — движком [HDFS](/ru/engines/table-engines/integrations/hdfs).

Маршрутизируются только те схемы S3, которые сопоставитель URI S3 может разрешить в конкретную конечную точку без дополнительной настройки (`s3`, а также `gs`/`gcs`/`oss`). Другие S3-compatible схемы поставщиков (`cos`, `obs`, `eos`, …) зависят от региона и не имеют сопоставления с конечной точкой по умолчанию, поэтому передача такого URL в движок `URL` считается использованием нераспознанной схемы и приводит к ошибке; для таких backend используйте движок [S3](/ru/engines/table-engines/integrations/s3) напрямую (с настроенным `url_scheme_mappers`).

Настройка [url&#95;base](/ru/operations/settings/settings.md#url_base) применяется до маршрутизации по схеме, поэтому относительная ссылка сначала разрешается относительно базового URL, а затем направляется в соответствующий движок.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## Использование
</div>

Запросы `INSERT` и `SELECT` преобразуются в запросы `POST` и `GET`
соответственно. Для обработки запросов `POST` удалённый сервер должен поддерживать
[передачу данных по частям (Chunked transfer encoding)](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

Вы можете ограничить максимальное число HTTP GET-перенаправлений с помощью настройки [max&#95;http&#95;get&#95;redirects](/ru/operations/settings/settings#max_http_get_redirects).

<div id="wildcards-with-http-index-pages">
  ## Подстановочные шаблоны с HTTP-страницами индексов
</div>

Когда включена настройка [allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/ru/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages), движок таблицы `URL` может разворачивать подстановочные шаблоны, загружая HTTP-страницы индексов и извлекая из них ссылки.
Это тот же механизм, что используется в табличной функции [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages).

Разворачивание ограничивается параметром [max&#95;http&#95;index&#95;page&#95;size](/ru/operations/server-configuration-parameters/settings.md#max_http_index_page_size) для каждой загруженной страницы индекса и параметром [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ru/operations/settings/settings.md#url_wildcard_max_directories_to_read) при рекурсивном обходе каталогов.

<div id="example">
  ## Пример
</div>

**1.** Создайте таблицу `url_engine_table` на сервере:

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Создайте простой HTTP-сервер с помощью стандартных средств Python 3 и
запустите его:

```python3
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

```bash
$ python3 server.py
```

**3.** Запросите данные:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## Детали реализации
</div>

* Чтение и запись могут выполняться параллельно
* Не поддерживаются:
  * Операции `ALTER` и `SELECT...SAMPLE`.
  * Индексы.
  * Репликация.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к `URL`. Тип: `LowCardinality(String)`.
* `_file` — Имя ресурса `URL`. Тип: `LowCardinality(String)`.
* `_size` — Размер ресурса в байтах. Тип: `Nullable(UInt64)`. Если размер неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_headers` - HTTP-заголовки ответа. Тип: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="resolving-relative-urls">
  ## Разрешение относительных URL
</div>

Настройка [url&#95;base](/ru/operations/settings/settings.md#url_base) позволяет использовать в движке `URL` относительный URL. Если задан `url_base`, URL, переданный в движок, разрешается относительно него в соответствии с [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). Подробное описание правил разрешения см. в [документации по табличной функции url](../../../sql-reference/table-functions/url.md#resolving-relative-urls).

**Пример**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## Настройки хранилища
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ru/operations/settings/settings.md#engine_url_skip_empty_files) — позволяет пропускать пустые файлы при чтении. По умолчанию отключен.
* [enable&#95;url&#95;encoding](/ru/operations/settings/settings.md#enable_url_encoding) — позволяет включать или отключать декодирование/кодирование пути в URI. По умолчанию включен.
* [url&#95;base](/ru/operations/settings/settings.md#url_base) — базовый URL для разрешения относительных URL, передаваемых движку.