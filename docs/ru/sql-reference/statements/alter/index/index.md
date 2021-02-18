---
toc_hidden_folder: true
toc_priority: 42
toc_title: INDEX
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