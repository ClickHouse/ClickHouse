---
description: 'Документация по мосту ODBC'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

Простой HTTP-сервер, который работает как прокси для ODBC-драйвера. Основная причина его появления —
возможные segfault и другие ошибки в реализациях ODBC, которые могут привести к сбою всего процесса clickhouse-server.

Этот инструмент работает через HTTP, а не через каналы, общую память или TCP, потому что:

* Его проще реализовать
* Его проще отлаживать
* jdbc-bridge можно реализовать таким же образом

<div id="usage">
  ## Использование
</div>

`clickhouse-server` использует этот инструмент в табличной функции odbc и StorageODBC.
Однако его можно использовать как автономный инструмент из командной строки со следующими
параметрами в URL POST-запроса:

* `connection_string` -- строка подключения ODBC.
* `sample_block` -- описание столбцов в формате ClickHouse NamesAndTypesList: имя в обратных кавычках,
  тип в виде строки. Имя и тип разделяются пробелом, строки разделяются
  символом новой строки.
* `max_block_size` -- необязательный параметр, задаёт максимальный размер одного блока.
  Запрос передаётся в теле POST-запроса. Ответ возвращается в формате RowBinary.

<div id="example">
  ## Пример:
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```