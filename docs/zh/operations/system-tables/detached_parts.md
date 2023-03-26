# system.detached_parts {#system_tables-detached_parts}

包含关于 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表的分离分区的信息。`reason` 列详细说明了该分区被分离的原因。

对于用户分离的分区，原因是空的。你可以通过 [ALTER TABLE ATTACH PARTITION\|PART](../../sql-reference/statements/alter/partition.md#alter_attach-partition) 命令添加这些分区。

关于其他列的描述，请参见 [system.parts](../../operations/system-tables/parts.md#system_tables-parts)。

如果分区名称无效，一些列的值可能是`NULL`。你可以通过[ALTER TABLE DROP DETACHED PART](../../sql-reference/statements/alter/partition.md#alter_drop-detached)来删除这些分区。

[原文](https://clickhouse.com/docs/zh/operations/system-tables/detached_parts) <!--hide-->
