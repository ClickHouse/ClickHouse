---
description: 'Позволяет параллельно обрабатывать файлы по URL с нескольких узлов указанного
  кластера.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

Позволяет параллельно обрабатывать файлы по URL с нескольких узлов указанного кластера. На узле-инициаторе создаётся соединение со всеми узлами кластера, раскрывается символ * в пути к файлу в URL, и каждый файл динамически распределяется. На узле-воркере у инициатора запрашивается следующая задача для обработки, после чего она обрабатывается. Это повторяется, пока не будут завершены все задачи.

<div id="syntax">
  ## Синтаксис
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент       | Описание                                                                                                                                                      |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Имя кластера, используемое для формирования набора адресов и параметров подключения к удалённым и локальным серверам.                                         |
| `URL`          | HTTP- или HTTPS-адрес сервера, который может принимать `GET`-запросы. Тип: [String](../../sql-reference/data-types/string.md).                                |
| `format`       | [Формат](/ru/sql-reference/formats) данных. Тип: [String](../../sql-reference/data-types/string.md).                                                             |
| `structure`    | Структура таблицы в формате `'UserID UInt64, Name String'`. Определяет имена столбцов и типы данных. Тип: [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанными форматом и структурой, содержащая данные из заданного `URL`.

<div id="examples">
  ## Примеры
</div>

Получение первых 3 строк из таблицы, содержащей столбцы типов `String` и [UInt32](../../sql-reference/data-types/int-uint.md), с HTTP-сервера, который возвращает данные в формате [CSV](/ru/interfaces/formats/CSV).

1. Создайте простой HTTP-сервер с помощью стандартных средств Python 3 и запустите его:

```python
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

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## Глоб-шаблоны в URL
</div>

Шаблоны в `{ }` используются для генерации набора сегментов или для указания резервных адресов. Описание поддерживаемых типов шаблонов и примеры см. в описании функции [remote](remote.md#globs-in-addresses).
Символ `|` внутри шаблонов используется для указания резервных адресов. Они перебираются в том же порядке, в котором перечислены в шаблоне. Количество сгенерированных адресов ограничено настройкой [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).

<div id="related">
  ## См. также
</div>

* [Движок HDFS](/ru/engines/table-engines/integrations/hdfs)
* [Табличная функция URL](/ru/engines/table-engines/special/url)