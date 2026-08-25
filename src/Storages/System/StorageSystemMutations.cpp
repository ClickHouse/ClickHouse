#include <Columns/ColumnString.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/StorageSystemMutations.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{


ColumnsDescription StorageSystemMutations::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "database",                      std::make_shared<DataTypeString>(), "The name of the database to which the mutation was applied."},
        { "table",                         std::make_shared<DataTypeString>(), "The name of the table to which the mutation was applied."},
        { "mutation_id",                   std::make_shared<DataTypeString>(), "The ID of the mutation. For replicated tables these IDs correspond to znode names in the `<table_path_in_clickhouse_keeper>/mutations/` directory in ClickHouse Keeper. For non-replicated tables the IDs correspond to file names in the data directory of the table."},
        { "command",                       std::make_shared<DataTypeString>(), "The mutation command string (the part of the query after ALTER TABLE [db.]table)."},
        { "create_time",                   std::make_shared<DataTypeDateTime>(), "Date and time when the mutation command was submitted for execution."},
        { "finish_time",                   std::make_shared<DataTypeDateTime>(),
            "Date and time when the mutation was completed. Zero if the mutation is not completed yet or if its completion time is unknown. "
            "For non-replicated tables the value is tracked in memory and is reset when the table is reloaded (e.g. on server restart). "
            "For replicated tables the value is per-replica; after a restart, the completion time of the most recently completed mutation "
            "is restored from Keeper, while older completed mutations report zero."
        },
        { "block_numbers.partition_id",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "For mutations of replicated tables, the array contains the partitions' IDs (one record for each partition). For mutations of non-replicated tables the array is empty."},
        { "block_numbers.number",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()),
            "For mutations of replicated tables, the array contains one record for each partition, with the block number that was acquired by the mutation. "
            "Only parts that contain blocks with numbers less than this number will be mutated in the partition."
            "In non-replicated tables, block numbers in all partitions form a single sequence. "
            "This means that for mutations of non-replicated tables, the column will contain one record with a single block number acquired by the mutation."
        },
        { "parts_in_progress_names",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of names of data parts that are currently being mutated."},
        { "parts_to_do_names",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of names of data parts that need to be mutated for the mutation to complete."},
        { "parts_to_do",                   std::make_shared<DataTypeInt64>(), "The number of data parts that need to be mutated for the mutation to complete. Note: even if `parts_to_do` = 0, a mutation of a replicated table may not be completed yet due to a long-running INSERT that is creating a new data part that will need to be mutated."},
        { "parts_postpone_reasons",        std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "A map of part names to reasons why they are postponed."},
        { "is_done",                       std::make_shared<DataTypeUInt8>(),
            "The flag whether the mutation is done or not. Possible values: "
            "1 if the mutation is completed, "
            "0 if the mutation is still in process. "
        },
        { "is_killed", std::make_shared<DataTypeUInt8>(),
            "Indicates whether a mutation has been killed. Only available in ClickHouse Cloud."
            "Note: is_killed=1 does not necessarily mean the mutation is completely finalized."
            "It is possible for a mutation to remain in a state where is_killed=1 and is_done=0 for an extended period."
            "This can occur if another long-running mutation is blocking the killed mutation. This is a normal situation."
        },
        { "latest_failed_part",           std::make_shared<DataTypeString>(), "The name of the most recent part that could not be mutated."},
        { "latest_fail_time",             std::make_shared<DataTypeDateTime>(), "The date and time of the most recent part mutation failure."},
        { "latest_fail_reason",           std::make_shared<DataTypeString>(), "The exception message that caused the most recent part mutation failure."},
        { "latest_fail_error_code_name",  std::make_shared<DataTypeString>(), "The error code of the exception that caused the most recent part mutation failure."},
    };
}

Block StorageSystemMutations::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemMutations::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// Collect a set of *MergeTree tables.
    std::map<String, std::map<String, StoragePtr>> merge_tree_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        /// Check if database can contain MergeTree tables
        if (db.second->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const MergeTreeData *>(table.get()))
                continue;

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;

            merge_tree_tables[db.first][iterator->name()] = table;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : merge_tree_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
    }

    for (size_t i_storage = 0; i_storage < col_database->size(); ++i_storage)
    {
        auto database = (*col_database)[i_storage].safeGet<String>();
        auto table = (*col_table)[i_storage].safeGet<String>();

        std::vector<MergeTreeMutationStatus> statuses;
        {
            const IStorage * storage = merge_tree_tables[database][table].get();
            if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage))
                statuses = merge_tree->getMutationsStatus();
        }

        for (const MergeTreeMutationStatus & status : statuses)
        {
            Array block_partition_ids;
            block_partition_ids.reserve(status.block_numbers.size());
            Array block_numbers;
            block_numbers.reserve(status.block_numbers.size());
            for (const auto & pair : status.block_numbers)
            {
                block_partition_ids.emplace_back(pair.first);
                block_numbers.emplace_back(pair.second);
            }
            Array parts_to_do_names;
            parts_to_do_names.reserve(status.parts_to_do_names.size());
            for (const String & part_name : status.parts_to_do_names)
                parts_to_do_names.emplace_back(part_name);

            Array parts_in_progress_names;
            parts_to_do_names.reserve(status.parts_in_progress_names.size());
            for (const String & part_name : status.parts_in_progress_names)
                parts_in_progress_names.emplace_back(part_name);

            Map parts_postpone_reasons_map;
            parts_postpone_reasons_map.reserve(status.parts_postpone_reasons.size());
            for (const auto & [part_name, reason] : status.parts_postpone_reasons)
            {
                Tuple key_value;
                key_value.emplace_back(part_name);
                key_value.emplace_back(reason);
                parts_postpone_reasons_map.emplace_back(std::move(key_value));
            }

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);

            res_columns[col_num++]->insert(status.id);
            res_columns[col_num++]->insert(status.command);
            res_columns[col_num++]->insert(UInt64(status.create_time));
            res_columns[col_num++]->insert(UInt64(status.finish_time));
            res_columns[col_num++]->insert(block_partition_ids);
            res_columns[col_num++]->insert(block_numbers);
            res_columns[col_num++]->insert(parts_in_progress_names);
            res_columns[col_num++]->insert(parts_to_do_names);
            res_columns[col_num++]->insert(parts_to_do_names.size());
            res_columns[col_num++]->insert(parts_postpone_reasons_map);
            res_columns[col_num++]->insert(status.is_done);
            res_columns[col_num++]->insert(status.is_killed);
            res_columns[col_num++]->insert(status.latest_failed_part);
            res_columns[col_num++]->insert(UInt64(status.latest_fail_time));
            res_columns[col_num++]->insert(status.latest_fail_reason);
            res_columns[col_num++]->insert(status.latest_fail_error_code_name);
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemMutations) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "mutations",
    .description = R"DOCS_MD(
The table contains information about [mutations](/reference/statements/alter/index#mutations) of [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables and their progress. Each mutation command is represented by a single row.
)DOCS_MD",
    .columns_notes = R"DOCS_MD(
<Note>
- If a part name is not in `parts_postpone_reasons` and has not yet been mutated, it means the part is yet not scheduled for mutation.
- The part name `all_parts` represents all parts that have not yet been mutated.
</Note>

- `is_killed` ([UInt8](/reference/data-types/int-uint)) — Indicates whether a mutation has been killed. **Only available in ClickHouse Cloud.**

<Note>
`is_killed=1` does not necessarily mean the mutation is completely finalized. It is possible for a mutation to remain in a state where `is_killed=1` and `is_done=0` for an extended period. This can happen if another long-running mutation is blocking the killed mutation. This is a normal situation.
</Note>

- `is_done` ([UInt8](/reference/data-types/int-uint)) — The flag whether the mutation is done or not. Possible values:
  - `1` if the mutation is completed,
  - `0` if the mutation is still in process.

<Note>
Even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not completed yet because of a long-running `INSERT` query, that will create a new data part needed to be mutated.
</Note>

If there were problems with mutating some data parts, the following columns contain additional information:

- `latest_failed_part` ([String](/reference/data-types/string)) — The name of the most recent part that could not be mutated.
- `latest_fail_time` ([DateTime](/reference/data-types/datetime)) — The date and time of the most recent part mutation failure.
- `latest_fail_reason` ([String](/reference/data-types/string)) — The exception message that caused the most recent part mutation failure.
)DOCS_MD",
    .additional_sections = R"DOCS_MD(
## Monitoring Mutations {#monitoring-mutations}

To track the progress on the `system.mutations` table, use the following query:

```sql
SELECT * FROM clusterAllReplicas('cluster_name', 'system', 'mutations')
WHERE is_done = 0 AND table = 'tmp';

-- or

SELECT * FROM clusterAllReplicas('cluster_name', 'system.mutations')
WHERE is_done = 0 AND table = 'tmp';
```

Note: this requires read permissions on the `system.*` tables.

<Tip>
**Cloud usage**

In ClickHouse Cloud the `system.mutations` table on each node has all the mutations in the cluster, and there is no need for `clusterAllReplicas`.
</Tip>
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Mutations](/reference/statements/alter/index#mutations)
- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) table engine
- [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) family
)DOCS_MD")

}
