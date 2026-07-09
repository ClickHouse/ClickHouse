---
description: '轻量级删除简化了从数据库中删除数据的操作。'
keywords: ['delete']
sidebar_label: 'DELETE'
sidebar_position: 36
slug: /sql-reference/statements/delete
title: '轻量级 DELETE 语句'
doc_type: 'reference'
---

轻量级 `DELETE` 语句会从表 `[db.]table` 中删除与表达式 `expr` 匹配的行。它仅适用于 *MergeTree 表引擎家族。

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

之所以称为“轻量级删除”，是为了与 [ALTER TABLE ... DELETE](/zh/sql-reference/statements/alter/delete) 命令相区别，后者是一个开销较大的操作。

<div id="examples">
  ## 示例
</div>

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

<div id="lightweight-delete-does-not-delete-data-immediately">
  ## 轻量级删除 不会立即删除数据
</div>

轻量级删除 是以一种[变更](/zh/sql-reference/statements/alter#mutations)的形式实现的：它会将行标记为已删除，但不会立即从物理层面删除这些行。

默认情况下，`DELETE` 语句会等待将行标记为已删除完成后才返回。如果数据量很大，这可能需要较长时间。或者，你也可以通过设置 [`lightweight_deletes_sync`](/zh/operations/settings/settings#lightweight_deletes_sync) 让它在后台异步运行。如果禁用该设置，`DELETE` 语句会立即返回，但在后台变更完成之前，查询仍可能看到这些数据。

该变更不会物理删除已标记为删除的行；只有在下一次 合并 时才会真正删除。因此，在一段未指定的时间内，数据实际上可能仍未从存储中移除，只是被标记为已删除。

如果你需要确保数据能在可预测的时间内从存储中删除，可以考虑使用表设置 [`min_age_to_force_merge_seconds`](/zh/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds)。或者，也可以使用 [ALTER TABLE ... DELETE](/zh/sql-reference/statements/alter/delete) 命令。请注意，使用 `ALTER TABLE ... DELETE` 删除数据可能会消耗大量资源，因为它会重新创建所有受影响的 parts。

<div id="deleting-large-amounts-of-data">
  ## 删除大量数据
</div>

大量删除操作可能会对 ClickHouse 的性能产生负面影响。如果你想删除表中的所有行，建议考虑使用 [`TRUNCATE TABLE`](/zh/sql-reference/statements/truncate) 命令。

如果你预计会频繁执行删除操作，建议考虑使用[自定义分区键](/zh/engines/table-engines/mergetree-family/custom-partitioning-key)。这样，你就可以使用 [`ALTER TABLE ... DROP PARTITION`](/zh/sql-reference/statements/alter/partition#drop-partitionpart) 命令，快速删除与该分区相关的所有行。

<div id="limitations-of-lightweight-delete">
  ## 轻量级删除 的局限性
</div>

<div id="lightweight-deletes-with-projections">
  ### 带 projections 的轻量级 `DELETE`
</div>

默认情况下，`DELETE` 不支持包含 projections 的表。这是因为 projection 中的行可能会受到 `DELETE` 操作的影响。不过，可以通过 [MergeTree 设置](/zh/operations/settings/merge-tree-settings) `lightweight_mutation_projection_mode` 来改变这一行为。

<div id="performance-considerations-when-using-lightweight-delete">
  ## 使用轻量级删除 时的性能注意事项
</div>

**使用轻量级删除 语句删除大量数据，可能会对 SELECT 查询性能产生负面影响。**

以下情况也可能对轻量级删除 的性能造成负面影响：

* `DELETE` 查询中的 `WHERE` 条件开销较大。
* 如果变更队列中积压了许多其他变更，可能会导致性能问题，因为表上的所有变更都会按顺序执行。
* 受影响的表包含数量非常多的数据分区片段。
* Compact parts 中存有大量数据。在 Compact part 中，所有列都存储在同一个文件中。

<div id="delete-permissions">
  ## DELETE 权限
</div>

`DELETE` 需要 `ALTER DELETE` 权限。要允许指定用户对特定表执行 `DELETE` 语句，请运行以下命令：

```sql
GRANT ALTER DELETE ON db.table to username;
```

<div id="how-lightweight-deletes-work-internally-in-clickhouse">
  ## ClickHouse 中轻量级删除 的内部工作原理
</div>

1. **对受影响的行应用“掩码”**

   执行 `DELETE FROM table ...` 查询时，ClickHouse 会保存一个掩码，将每一行标记为“existing”或“deleted”。这些被标记为“deleted”的行会在后续查询中被忽略。不过，这些行实际上要等到后续的合并过程中才会被真正移除。写入这个掩码的开销比 `ALTER TABLE ... DELETE` 查询执行的操作要小得多。

   该掩码通过隐藏的 `_row_exists` 系统列实现：对所有可见行存储 `True`，对已删除的行存储 `False`。只有当某个 part 中有部分行被删除时，这一列才会出现在该 part 中。如果某个 part 中这一列的所有值都为 `True`，则这一列不会存在。

2. **`SELECT` 查询会被改写为包含掩码的形式**

   当查询中使用了带掩码的列时，`SELECT ... FROM table WHERE condition` 查询在内部会附加 `_row_exists` 上的谓词条件，并改写为：

   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```

   执行时，系统会读取 `_row_exists` 列，以确定哪些行不应返回。如果已删除的行较多，ClickHouse 在读取其余列时还可以判断哪些粒度能够被完全跳过。

3. **`DELETE` 查询会被转换为 `ALTER TABLE ... UPDATE` 查询**

   `DELETE FROM table WHERE condition` 会被转换为一个 `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition` 变更。

   在内部，这个变更分两步执行：

   1. 对每个单独的 part 执行 `SELECT count() FROM table WHERE condition` 命令，以确定该 part 是否受影响。

   2. 根据上述命令的结果，随后会对受影响的 parts 执行变更，而对未受影响的 parts 创建硬链接。对于 wide parts，会更新每一行的 `_row_exists` 列，而其他所有列的文件都会创建硬链接。对于 compact parts，由于所有列都存储在同一个文件中，因此所有列都需要重写。

   从上述步骤可以看出，采用掩码技术的轻量级删除 比传统的 `ALTER TABLE ... DELETE` 性能更好，因为它无需为受影响的 parts 重写所有列文件。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 中的更新与删除处理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)