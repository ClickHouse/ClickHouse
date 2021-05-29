---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。detached\_parts {#system_tables-detached_parts}

包含有关分离部分的信息 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 桌子 该 `reason` 列指定分离部件的原因。

对于用户分离的部件，原因是空的。 这些部件可以附加 [ALTER TABLE ATTACH PARTITION\|PART](../../sql-reference/statements/alter.md#alter_attach-partition) 指挥部

有关其他列的说明，请参阅 [系统。零件](../../operations/system-tables/parts.md#system_tables-parts).

如果部件名称无效，某些列的值可能为 `NULL`. 这些部分可以删除 [ALTER TABLE DROP DETACHED PART](../../sql-reference/statements/alter.md#alter_drop-detached).
