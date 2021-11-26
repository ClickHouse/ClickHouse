---
toc_priority: 49
toc_title: PROJECTION
---

# Манипуляции с проекциями {#manipulations-with-projections}

Доступны следующие операции с [проекциями](../../../engines/table-engines/mergetree-family/mergetree.md#projections):

-   `ALTER TABLE [db].name ADD PROJECTION name AS SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]` — добавляет описание проекции в метаданные.

-   `ALTER TABLE [db].name DROP PROJECTION name` — удаляет описание проекции из метаданных и удаляет файлы проекции с диска.

-   `ALTER TABLE [db.]table MATERIALIZE PROJECTION name IN PARTITION partition_name` — перестраивает проекцию в указанной партиции. Реализовано как [мутация](../../../sql-reference/statements/alter/index.md#mutations).

-   `ALTER TABLE [db.]table CLEAR PROJECTION name IN PARTITION partition_name` — удаляет файлы проекции с диска без удаления описания.

Команды `ADD`, `DROP` и `CLEAR` — легковесны, поскольку они только меняют метаданные или удаляют файлы.

Также команды реплицируются, синхронизируя описания проекций в метаданных с помощью ZooKeeper.

!!! note "Note"
    Манипуляции с проекциями поддерживаются только для таблиц с движком [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (включая [replicated](../../../engines/table-engines/mergetree-family/replication.md) варианты).
