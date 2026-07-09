---
description: 'Документация по управлению индексами пропуска данных'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'Управление индексами пропуска данных'
toc_hidden_folder: true
doc_type: 'reference'
---

Доступны следующие операции:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` — добавляет описание индекса в метаданные таблицы.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` — удаляет описание индекса из метаданных таблицы и файлы индекса с диска. Реализовано в виде [мутации](/ru/sql-reference/statements/alter/index.md#mutations).

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` — перестраивает вторичный индекс `name` для указанной партиции `partition_name`. Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations). Если часть `IN PARTITION` опущена, индекс перестраивается для данных всей таблицы.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` — удаляет файлы вторичного индекса с диска, не удаляя его описание. Реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

Команды `ADD`, `DROP` и `CLEAR` являются легковесными в том смысле, что они лишь изменяют метаданные или удаляют файлы.
Кроме того, они реплицируются: метаданные индексов синхронизируются через ClickHouse Keeper или ZooKeeper.

:::note
Управление индексами поддерживается только для таблиц с движком [`*MergeTree`](/ru/engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](/ru/engines/table-engines/mergetree-family/replication.md) варианты).
:::