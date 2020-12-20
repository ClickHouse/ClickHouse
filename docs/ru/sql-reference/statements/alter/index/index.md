---
toc_hidden_folder: true
toc_priority: 42
toc_title: "\u041c\u0430\u043d\u0438\u043f\u0443\u043b\u044f\u0446\u0438\u0438\u0020\u0441\u0020\u0438\u043d\u0434\u0435\u043a\u0441\u0430\u043c\u0438"
---

# Манипуляции с индексами {#manipuliatsii-s-indeksami}

Добавить или удалить индекс можно с помощью операций

``` sql
ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [AFTER name]
ALTER TABLE [db].name DROP INDEX name
```

Поддерживается только таблицами семейства `*MergeTree`.

Команда `ADD INDEX` добавляет описание индексов в метаданные, а `DROP INDEX` удаляет индекс из метаданных и стирает файлы индекса с диска, поэтому они легковесные и работают мгновенно.

Если индекс появился в метаданных, то он начнет считаться в последующих слияниях и записях в таблицу, а не сразу после выполнения операции `ALTER`.

Запрос на изменение индексов реплицируется, сохраняя новые метаданные в ZooKeeper и применяя изменения на всех репликах.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/index/index/) <!--hide-->