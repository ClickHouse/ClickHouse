---
toc_priority: 43
toc_title: DETACH
---

# DETACH {#detach-statement}

Удаляет из сервера информацию о таблице name. Сервер перестаёт знать о существовании таблицы.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Но ни данные, ни метаданные таблицы не удаляются. При следующем запуске сервера, сервер прочитает метаданные и снова узнает о таблице.
Также, «отцепленную» таблицу можно прицепить заново запросом `ATTACH` (за исключением системных таблиц, для которых метаданные не хранятся).

Запроса `DETACH DATABASE` нет.

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/detach/) <!--hide-->
