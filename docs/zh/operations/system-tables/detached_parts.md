# system.detached_parts {#system_tables-detached_parts}

包含有关[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)表的分离part的信息。该 `reason` 列指定part分离的原因。

对于用户分离的part，reason是空的。 可以使用 [ALTER TABLE ATTACH PARTITION\|PART](../../sql-reference/statements/alter.md#alter_attach-partition) 命令添加这些part。

有关其他列的说明，请参见[system.parts](../../operations/system-tables/parts.md#system_tables-parts)。

如果part名称无效，某些列的值可能为 `NULL`。 可以使用 [ALTER TABLE DROP DETACHED PART](../../sql-reference/statements/alter.md#alter_drop-detached)删除这些part。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/detached_parts) <!--hide-->
