#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTDeleteQuery>();
    node = query;

    ParserKeyword s_delete(Keyword::DELETE);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_in_partition(Keyword::IN_PARTITION);
    ParserKeyword s_where(Keyword::WHERE);
    ParserExpression parser_exp_elem;
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_on{Keyword::ON};


    if (s_delete.ignore(pos, expected))
    {
        if (!s_from.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        if (s_on.ignore(pos, expected))
        {
            String cluster_str;
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = cluster_str;
        }

        if (s_in_partition.ignore(pos, expected))
        {
            ParserList partition_list_parser(
                std::make_unique<ParserPartition>(), std::make_unique<ParserToken>(TokenType::Comma), false);
            ASTPtr partition_list_ast;
            if (!partition_list_parser.parse(pos, partition_list_ast, expected))
                return false;

            auto & partition_list = partition_list_ast->as<ASTExpressionList &>();
            if (partition_list.children.size() == 1)
                query->partition = std::move(partition_list.children[0]);
            else
                query->partitions = std::move(partition_list_ast);
        }

        if (!s_where.ignore(pos, expected))
            return false;

        if (!parser_exp_elem.parse(pos, query->predicate, expected))
            return false;

        /// ParserExpression, in contrast to ParserExpressionWithOptionalAlias,
        /// does not expect an alias after the expression. However, in certain cases,
        /// it uses ParserExpressionWithOptionalAlias recursively, and use its result.
        /// This is the case when it parses a single expression in parentheses, e.g.,
        /// it does not allow
        /// 1 AS x
        /// but it can parse
        /// (1 AS x)
        /// which we should not allow as well.
        if (!query->predicate->tryGetAlias().empty())
            return false;

        if (s_settings.ignore(pos, expected))
        {
            ParserSetQuery parser_settings(true);

            if (!parser_settings.parse(pos, query->settings_ast, expected))
                return false;
        }
    }
    else
        return false;

    if (query->partition)
        query->children.push_back(query->partition);

    if (query->partitions)
        query->children.push_back(query->partitions);

    if (query->predicate)
        query->children.push_back(query->predicate);

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    if (query->settings_ast)
        query->children.push_back(query->settings_ast);

    return true;
}

}

namespace DB
{

void registerStatementDelete(StatementFactory & factory)
{
    factory.registerStatement("DELETE",
    {
        .description = R"DOCS_MD(
The lightweight `DELETE` statement removes rows from the table `[db.]table` that match the expression `expr`. It is only available for the *MergeTree table engine family.

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE expr;
```

It is called "lightweight `DELETE`" to contrast it to the [ALTER TABLE ... DELETE](/reference/statements/alter/delete) command, which is a heavyweight process.

The `IN PARTITION` clause limits the delete to the listed partitions. Without it, on tables of the `ReplicatedMergeTree` family, when the [optimize_mutations_with_partition_pruning](/reference/settings/session-settings/optimize) setting is enabled (the default), ClickHouse automatically detects partition key conditions in `expr` and only deletes from the affected partitions. On non-replicated `MergeTree` tables, use an explicit `IN PARTITION` clause to limit a delete to specific partitions.

## Examples {#examples}

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

## Lightweight `DELETE` does not delete data immediately {#lightweight-delete-does-not-delete-data-immediately}

Lightweight `DELETE` marks rows as deleted without immediately removing them from storage. By default, it uses a [mutation](/reference/statements/alter/index#mutations). It can also use patch parts, depending on the [`lightweight_delete_mode`](#lightweight-delete-mode) setting.

With the default mutation-based mode, `DELETE` statements wait until marking the rows as deleted is completed before returning. This can take a long time if the amount of data is large. Alternatively, you can run it asynchronously in the background using the setting [`lightweight_deletes_sync`](/reference/settings/session-settings/lightweight#lightweight_deletes_sync). If disabled, the `DELETE` statement is going to return immediately, but the data can still be visible to queries until the background mutation is finished.

In both modes, deleted rows remain in storage until cleanup. Background merges normally remove them from the affected data parts. To explicitly apply the deletion mask, use [`ALTER TABLE ... APPLY DELETED MASK`](/reference/statements/alter/apply-deleted-mask), which performs a heavyweight mutation.

If you need to guarantee that your data is deleted from storage in a predictable time, consider using the table setting [`min_age_to_force_merge_seconds`](/reference/settings/merge-tree-settings/min-age#min_age_to_force_merge_seconds). Or you can use the [ALTER TABLE ... DELETE](/reference/statements/alter/delete) command. Note that deleting data using `ALTER TABLE ... DELETE` may consume significant resources as it recreates all affected parts.

## Choose the delete mode {#lightweight-delete-mode}

The [`lightweight_delete_mode`](/reference/settings/session-settings/lightweight#lightweight_delete_mode) setting controls how ClickHouse marks rows as deleted:

| Value | Behavior |
| --- | --- |
| `alter_update` (default) | Runs an `ALTER TABLE ... UPDATE` mutation to update the `_row_exists` mask. |
| `lightweight_update` | Uses a lightweight `UPDATE` with patch parts when supported; otherwise, uses an `ALTER TABLE ... UPDATE` mutation. |
| `lightweight_update_force` | Uses a lightweight `UPDATE` with patch parts when supported; otherwise, throws an exception. |

With patch parts, ClickHouse writes `_row_exists = 0` only for the deleted rows, together with metadata that identifies those rows. It avoids rewriting the entire mask column in the affected parts. Subsequent `SELECT` queries apply these patches to exclude the deleted rows before physical cleanup.

The patch-part path waits for patch creation before returning, rather than submitting a background mutation controlled by `lightweight_deletes_sync`. It does not need to wait for existing merges and mutations to finish. Applying patches adds work to reads; see [lightweight update performance considerations](/reference/statements/update#performance-considerations).

To use this path, [`enable_lightweight_update`](/reference/settings/session-settings/enable-lightweight#enable_lightweight_update) must be enabled, and the table must meet the [lightweight update requirements](/reference/statements/update#lightweight-update-requirements), including the `enable_block_number_column` and `enable_block_offset_column` table settings. For example, for a supported `hits` table:

```sql
ALTER TABLE hits MODIFY SETTING
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DELETE FROM hits WHERE Title LIKE '%hello%';
```

This example uses `lightweight_update_force` so that an unsupported configuration produces an exception instead of a mutation-based delete.

## Deleting large amounts of data {#deleting-large-amounts-of-data}

Large deletes can negatively affect ClickHouse performance. If you are attempting to delete all rows from a table, consider using the [`TRUNCATE TABLE`](/reference/statements/truncate) command.

If you anticipate frequent deletes, consider using a [custom partitioning key](/reference/engines/table-engines/mergetree-family/custom-partitioning-key). You can then use the [`ALTER TABLE ... DROP PARTITION`](/reference/statements/alter/partition#drop-partitionpart) command to quickly drop all rows associated with that partition.

## Limitations of lightweight `DELETE` {#limitations-of-lightweight-delete}

### Lightweight `DELETE`s with projections {#lightweight-deletes-with-projections}

By default, `DELETE` does not work for tables with projections. This is because rows in a projection may be affected by a `DELETE` operation. But there is a [MergeTree setting](/reference/settings/merge-tree-settings) `lightweight_mutation_projection_mode` to change the behavior.

## Performance considerations when using lightweight `DELETE` {#performance-considerations-when-using-lightweight-delete}

**Deleting large volumes of data with the lightweight `DELETE` statement can negatively affect SELECT query performance.**

The following can also negatively impact lightweight `DELETE` performance:

- A heavy `WHERE` condition in a `DELETE` query.
- When using mutation-based deletes, if the mutations queue is filled with many other mutations, this can possibly lead to performance issues as all mutations on a table are executed sequentially.
- The affected table has a very large number of data parts.
- When using mutation-based deletes, having a lot of data in compact parts. In a compact part, all columns are stored in one file and must be rewritten together.

## Delete permissions {#delete-permissions}

`DELETE` requires the `ALTER DELETE` privilege. To enable `DELETE` statements on a specific table for a given user, run the following command:

```sql
GRANT ALTER DELETE ON db.table to username;
```

## How lightweight DELETEs work internally in ClickHouse {#how-lightweight-deletes-work-internally-in-clickhouse}

1. **A "mask" is applied to affected rows**

   When a `DELETE FROM table ...` query is executed, ClickHouse saves a mask where each row is marked as either "existing" or as "deleted". Those "deleted" rows are omitted for subsequent queries. However, rows are physically removed later, normally during background merges. Writing this mask is much more lightweight than what is done by an `ALTER TABLE ... DELETE` query.

   The mask is implemented as a hidden `_row_exists` system column that stores `True` for all visible rows and `False` for deleted ones. This column is only present in a part if some rows in the part were deleted. This column does not exist when a part has all values equal to `True`.

2. **`SELECT` queries are transformed to include the mask**

   When a masked column is used in a query, the `SELECT ... FROM table WHERE condition` query internally is extended by the predicate on `_row_exists` and is transformed to:
   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```
   At execution time, the column `_row_exists` is read to determine which rows should not be returned. If there are many deleted rows, ClickHouse can determine which granules can be fully skipped when reading the rest of the columns.

3. **`DELETE` queries update the mask using the selected mode**

   With patch parts, `DELETE FROM table WHERE condition` is translated into `UPDATE table SET _row_exists = 0 WHERE condition`. The resulting patch parts store the mask changes for the deleted rows and are applied when reading and merging data.

   With the default `alter_update` mode, `DELETE FROM table WHERE condition` is translated into an `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition` mutation.

   Internally, this mutation is executed in two steps:

   1. A `SELECT count() FROM table WHERE condition` command is executed for each individual part to determine if the part is affected.

   2. Based on the commands above, affected parts are then mutated, and hardlinks are created for unaffected parts. In the case of wide parts, the `_row_exists` column for each row is updated, and all other columns' files are hardlinked. For compact parts, all columns are re-written because they are all stored together in one file.

   From the steps above, we can see that lightweight `DELETE` using the masking technique improves performance over traditional `ALTER TABLE ... DELETE` because it does not re-write all the columns' files for affected parts.

## Related content {#related-content}

- Blog: [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
- Blog: [Lightweight deletes are now even lighter](https://clickhouse.com/blog/updates-in-clickhouse-2-sql-style-updates#lightweight-deletes-are-now-even-lighter)
)DOCS_MD",
        .syntax = R"(
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr1 [, partition_expr2 ...]] WHERE expr
)",
        .related = {"ALTER TABLE ... DELETE", "ALTER TABLE ... APPLY DELETED MASK", "UPDATE", "TRUNCATE"},
    });
}

}
