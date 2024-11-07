---
slug: /ru/sql-reference/statements/alter/projection
sidebar_position: 49
sidebar_label: PROJECTION
---

# Манипуляции с проекциями {#manipulations-with-projections}

Доступны следующие операции с [проекциями](../../../engines/table-engines/mergetree-family/mergetree.md#projections):

-   `ALTER TABLE [db].name ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY] )` — добавляет описание проекции в метаданные.

-   `ALTER TABLE [db].name DROP PROJECTION [IF EXISTS] name` — удаляет описание проекции из метаданных и удаляет файлы проекции с диска.

-   `ALTER TABLE [db.]table MATERIALIZE PROJECTION name IN PARTITION partition_name` — перестраивает проекцию в указанной партиции. Реализовано как [мутация](../../../sql-reference/statements/alter/index.md#mutations).

-   `ALTER TABLE [db.]table CLEAR PROJECTION [IF EXISTS] name IN PARTITION partition_name` — удаляет файлы проекции с диска без удаления описания.

Команды `ADD`, `DROP` и `CLEAR` — легковесны, поскольку они только меняют метаданные или удаляют файлы.

Также команды реплицируются, синхронизируя описания проекций в метаданных с помощью ZooKeeper.

:::note Примечание
Манипуляции с проекциями поддерживаются только для таблиц с движком [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [replicated](../../../engines/table-engines/mergetree-family/replication.md) варианты).
:::
