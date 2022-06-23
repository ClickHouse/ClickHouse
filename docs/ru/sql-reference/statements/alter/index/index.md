---
toc_hidden_folder: true
toc_priority: 42
toc_title: "Манипуляции с индексами"
---

# Манипуляции с индексами {#manipuliatsii-s-indeksami}

Добавить или удалить индекс можно с помощью операций

``` sql
ALTER TABLE [db.]name ADD INDEX name expression TYPE type GRANULARITY value [FIRST|AFTER name]
ALTER TABLE [db.]name DROP INDEX name
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

Поддерживается только таблицами семейства `*MergeTree`.

Команда `ADD INDEX` добавляет описание индексов в метаданные, а `DROP INDEX` удаляет индекс из метаданных и стирает файлы индекса с диска, поэтому они легковесные и работают мгновенно.

Если индекс появился в метаданных, то он начнет считаться в последующих слияниях и записях в таблицу, а не сразу после выполнения операции `ALTER`.
`MATERIALIZE INDEX` - перестраивает индекс в указанной партиции. Реализовано как мутация.

Запрос на изменение индексов реплицируется, сохраняя новые метаданные в ZooKeeper и применяя изменения на всех репликах.

