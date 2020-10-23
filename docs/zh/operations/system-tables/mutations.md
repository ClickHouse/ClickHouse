---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。突变 {#system_tables-mutations}

该表包含以下信息 [突变](../../sql-reference/statements/alter.md#alter-mutations) MergeTree表及其进展。 每个突变命令由一行表示。 该表具有以下列:

**数据库**, **表** -应用突变的数据库和表的名称。

**mutation\_id** -变异的ID 对于复制的表，这些Id对应于znode中的名称 `<table_path_in_zookeeper>/mutations/` 动物园管理员的目录。 对于未复制的表，Id对应于表的数据目录中的文件名。

**命令** -Mutation命令字符串（查询后的部分 `ALTER TABLE [db.]table`).

**create\_time** -当这个突变命令被提交执行。

**block\_numbers.partition\_id**, **block\_numbers.编号** -嵌套列。 对于复制表的突变，它包含每个分区的一条记录：分区ID和通过突变获取的块编号（在每个分区中，只有包含编号小于该分区中突变获取的块编号的块的 在非复制表中，所有分区中的块编号形成一个序列。 这意味着对于非复制表的突变，该列将包含一条记录，其中包含由突变获取的单个块编号。

**parts\_to\_do** -为了完成突变，需要突变的数据部分的数量。

**is\_done** -变异完成了?？ 请注意，即使 `parts_to_do = 0` 由于长时间运行的INSERT将创建需要突变的新数据部分，因此可能尚未完成复制表的突变。

如果在改变某些部分时出现问题，以下列将包含其他信息:

**latest\_failed\_part** -不能变异的最新部分的名称。

**latest\_fail\_time** -最近的部分突变失败的时间。

**latest\_fail\_reason** -导致最近部件变异失败的异常消息。
