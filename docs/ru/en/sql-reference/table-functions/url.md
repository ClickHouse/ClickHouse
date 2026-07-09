---
description: 'Создаёт таблицу по `URL` с указанными `format` и `structure`'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # Табличная функция url
</div>

Функция `url` создаёт таблицу по `URL` с указанными `format` и `structure`.

Функцию `url` можно использовать в запросах `SELECT` и `INSERT` к данным в таблицах [URL](../../engines/table-engines/special/url.md).

<div id="syntax">
  ## Синтаксис
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## Параметры
</div>

| Параметр    | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `URL`       | URL в одинарных кавычках, схема которого определяет backend. URL со схемой `http`/`https` (или с нераспознанной схемой) — это адрес сервера, принимающего запросы `GET` или `POST` (для запросов `SELECT` и `INSERT` соответственно); URL с распознанной схемой, отличной от HTTP (`file://`, `s3://`, `az://`, `hdfs://`, …), передаётся соответствующей табличной функции — см. [Маршрутизация по схеме URL](#scheme-dispatch). Тип: [String](../../sql-reference/data-types/string.md). |
| `format`    | [Формат](/ru/sql-reference/formats) данных. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                          |
| `structure` | Структура таблицы в формате `'UserID UInt64, Name String'`. Определяет имена столбцов и их типы. Тип: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                  |
| `headers`   | Заголовки в формате `'headers('key1'='value1', 'key2'='value2')'`. Позволяет задать заголовки для HTTP-запроса.                                                                                                                                                                                                                                                                                                                                                                            |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица указанного формата и структуры с данными из заданного `URL`.

<div id="examples">
  ## Примеры
</div>

Получение первых 3 строк таблицы со столбцами типа `String` и [UInt32](../../sql-reference/data-types/int-uint.md) с HTTP-сервера, который возвращает данные в формате [CSV](/ru/interfaces/formats/CSV).

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Вставка данных из `URL` в таблицу:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## Маршрутизация по схеме URL
</div>

Функция `url` служит единой обёрткой над другими табличными функциями для файлов и объектных хранилищ: она направляет запрос в нужный backend в зависимости от схемы URL. Это позволяет читать данные из любого поддерживаемого расположения, используя единый синтаксис.

| Scheme                                        | Dispatches to                                     |
| --------------------------------------------- | ------------------------------------------------- |
| `http`, `https` (and any unrecognized scheme) | сам движок `URL` (HTTP `GET`/`POST`)              |
| `file`                                        | функция [`file`](file.md)                         |
| `s3`, `gs`, `gcs`, `oss`                      | функция [`s3`](s3.md)                             |
| `az`, `azure`, `abfss`, `abfs`                | функция [`azureBlobStorage`](azureBlobStorage.md) |
| `hdfs`                                        | функция [`hdfs`](hdfs.md)                         |

Маршрутизация выполняется только для тех схем S3, которые сопоставитель S3 URI может преобразовать в конкретную конечную точку без дополнительной настройки (`s3`, а также `gs`/`gcs`/`oss`). Другие S3-совместимые схемы поставщиков (`cos`, `obs`, `eos`, …) зависят от региона и не имеют сопоставления с конечной точкой по умолчанию, поэтому URL вида `cos://…` рассматривается как нераспознанная схема и приводит к ошибке; для таких backend’ов используйте функцию [`s3`](s3.md) напрямую (с настроенным `url_scheme_mappers`).

Для `file://` относительный путь (`file://data.csv`) разрешается внутри каталога [user&#95;files](/ru/operations/server-configuration-parameters/settings#user_files_path), а абсолютный путь (`file:///home/user/data.csv`) должен, как обычно, указывать на расположение внутри него.

Аргументы `format`, `structure` и `compression_method`, а также настройка [url&#95;base](#resolving-relative-urls), работают одинаково независимо от цели маршрутизации.

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

Поддержка выбора по схеме в [`urlCluster`](urlCluster.md) пока не реализована: схема, отличная от `http(s)`, переданная в `urlCluster`, отклоняется с ошибкой. Вместо этого для таких backend&#39;ов используйте соответствующую cluster-функцию (`s3Cluster`, `azureBlobStorageCluster`, `hdfsCluster`, …).

<div id="globs-in-url">
  ## Глоб-шаблоны в URL
</div>

Шаблоны в `{ }` используются для формирования набора сегментов или для указания адресов переключения при отказе. Поддерживаемые типы шаблонов и примеры см. в описании функции [remote](remote.md#globs-in-addresses).
Символ `|` внутри шаблонов используется для указания адресов переключения при отказе. Они перебираются в том же порядке, в котором перечислены в шаблоне. Количество сгенерированных адресов ограничено настройкой [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).
Описание синтаксиса glob для пути в URL (например, `*`, `{a,b}`, `{N..M}` и `**`) см. в разделе [Глоб-шаблоны в пути](file.md#globs-in-path). Обратите внимание, что `?` начинает строку запроса в URL и не может использоваться как подстановочный знак в компоненте пути.

<div id="wildcards-with-http-index-pages">
  ## Подстановочные шаблоны с HTTP-индексными страницами
</div>

Для `url` и движка таблицы `URL` ClickHouse может разворачивать подстановочные шаблоны, получая HTTP-индексные страницы (HTML или обычный текст) и извлекая URL из тела ответа. Это позволяет использовать шаблоны вида `/**/`, когда сервер предоставляет списки каталогов.

Примечания:

* Относительные URL вычисляются относительно URL индексной страницы.
* Шаблоны `URL` разворачиваются до получения индексных страниц, включая расширение сегментов по запятым и числовым диапазонам, а также варианты переключения при отказе `|` вне компонента пути.
* Шаблоны переключения при отказе `|` внутри компонента пути не поддерживаются при разворачивании HTTP-индексных страниц.
* Сопоставление с подстановочными шаблонами применяется к компоненту пути URL.
* Если URL из списка уже содержит строку запроса или фрагмент, они имеют приоритет над значениями из исходного URL. В противном случае используются строка запроса и фрагмент из исходного URL.
* Пустой список допустим; HTTP-ошибки (например, 404) для индексных страниц вызывают исключения.
* Максимальный размер индексной страницы ограничен параметром [max&#95;http&#95;index&#95;page&#95;size](/ru/operations/server-configuration-parameters/settings.md#max_http_index_page_size).
* Максимальное число каталогов, читаемых при рекурсивном разворачивании, ограничено параметром [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ru/operations/settings/settings.md#url_wildcard_max_directories_to_read).

Пример:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к `URL`. Type: `LowCardinality(String)`.
* `_file` — Имя ресурса из `URL`. Type: `LowCardinality(String)`.
* `_size` — Размер ресурса в байтах. Type: `Nullable(UInt64)`. Если размер неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Type: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_headers` - HTTP-заголовки ответа. Type: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="hive-style-partitioning">
  ## настройка use_hive_partitioning
</div>

Если для настройки `use_hive_partitioning` установлено значение 1, ClickHouse будет распознавать секционирование в стиле Hive в пути (`/name=value/`) и позволит использовать столбцы партиции как виртуальные столбцы в запросе. Эти виртуальные столбцы будут иметь те же имена, что и в секционированном пути.

**Пример**

Использование виртуального столбца, созданного с помощью секционирования в стиле Hive

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## Разрешение относительных URL
</div>

Параметр [url&#95;base](/ru/operations/settings/settings.md#url_base) позволяет передавать в функцию `url` относительный URL. Когда задан `url_base`, а аргумент функции является относительной ссылкой, она разрешается относительно базового URL в соответствии с [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).

Правила разрешения следующие:

* **Относительно пути** (например, `data.csv`): объединяется с путем базового URL — все после последнего `/` в базовом пути заменяется. Наличие завершающего слеша имеет значение: `https://example.com/dir/` + `data.csv` дает `https://example.com/dir/data.csv`, а `https://example.com/dir` + `data.csv` дает `https://example.com/data.csv`. Сегменты с точкой (`./` и `../`) нормализуются.
* **Относительно хоста** (например, `/test/data.csv`): разрешается с использованием схемы и хоста базового URL.
* **Относительно схемы** (например, `//other.com/test/data.csv`): разрешается с использованием схемы базового URL.
* **Только запрос** (например, `?x=1`): добавляется к полному базовому пути, заменяя существующий запрос или фрагмент.
* **Только фрагмент** (например, `#frag`): добавляется к базовому URL, сохраняя запрос и заменяя существующий фрагмент.
* **Пустой**: возвращает базовый URL без фрагмента.
* **Абсолютный URL**: передается без изменений; `url_base` игнорируется.

**Пример**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## Настройки хранилища
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ru/operations/settings/settings.md#engine_url_skip_empty_files) - позволяет пропускать пустые файлы при чтении. По умолчанию отключена.
* [enable&#95;url&#95;encoding](/ru/operations/settings/settings.md#enable_url_encoding) - позволяет включать и отключать декодирование/кодирование пути в URI. По умолчанию включена.
* [url&#95;base](/ru/operations/settings/settings.md#url_base) - базовый URL для разрешения относительных URL, передаваемых в функцию `url`.

<div id="permissions">
  ## Разрешения
</div>

Для функции `url` требуется разрешение `CREATE TEMPORARY TABLE`. Поэтому она не работает для пользователей с настройкой [readonly](/ru/operations/settings/permissions-for-queries#readonly) = 1. Требуется как минимум readonly = 2.

<div id="related">
  ## Связанные материалы
</div>

* [Виртуальные столбцы](/ru/engines/table-engines/index.md#table_engines-virtual_columns)