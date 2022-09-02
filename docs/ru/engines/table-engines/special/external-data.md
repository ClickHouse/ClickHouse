---
toc_priority: 45
toc_title: "Внешние данные для обработки запроса"
---

# Внешние данные для обработки запроса {#vneshnie-dannye-dlia-obrabotki-zaprosa}

ClickHouse позволяет отправить на сервер данные, необходимые для обработки одного запроса, вместе с запросом SELECT. Такие данные будут положены во временную таблицу (см. раздел «Временные таблицы») и смогут использоваться в запросе (например, в операторах IN).

Для примера, если у вас есть текстовый файл с важными идентификаторами посетителей, вы можете загрузить его на сервер вместе с запросом, в котором используется фильтрация по этому списку.

Если вам нужно будет выполнить более одного запроса с достаточно большими внешними данными - лучше не использовать эту функциональность, а загрузить данные в БД заранее.

Внешние данные могут быть загружены как с помощью клиента командной строки (в не интерактивном режиме), так и через HTTP-интерфейс.

В клиенте командной строки, может быть указана секция параметров вида

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

Таких секций может быть несколько - по числу передаваемых таблиц.

**–external** - маркер начала секции.
**–file** - путь к файлу с дампом таблицы, или -, что обозначает stdin.
Из stdin может быть считана только одна таблица.

Следующие параметры не обязательные:
**–name** - имя таблицы. Если не указано - используется _data.
**–format** - формат данных в файле. Если не указано - используется TabSeparated.

Должен быть указан один из следующих параметров:
**–types** - список типов столбцов через запятую. Например, `UInt64,String`. Столбцы будут названы _1, _2, …
**–structure** - структура таблицы, в форме `UserID UInt64`, `URL String`. Определяет имена и типы столбцов.

Файлы, указанные в file, будут разобраны форматом, указанным в format, с использованием типов данных, указанных в types или structure. Таблица будет загружена на сервер, и доступна там в качестве временной таблицы с именем name.

Примеры:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

При использовании HTTP интерфейса, внешние данные передаются в формате multipart/form-data. Каждая таблица передаётся отдельным файлом. Имя таблицы берётся из имени файла. В query_string передаются параметры name_format, name_types, name_structure, где name - имя таблицы, которой соответствуют эти параметры. Смысл параметров такой же, как при использовании клиента командной строки.

Пример:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

При распределённой обработке запроса, временные таблицы передаются на все удалённые серверы.

