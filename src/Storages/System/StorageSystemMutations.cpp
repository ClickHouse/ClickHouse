#include <Columns/ColumnString.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/StorageSystemMutations.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
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
        { "block_numbers.partition_id",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "For mutations of replicated tables, the array contains the partitions' IDs (one record for each partition). "
            "For mutations of non-replicated tables it holds a single empty string, whatever the number of partitions, because their block numbers form one sequence "
            "for the whole table rather than one per partition."
        },
        { "block_numbers.number",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()),
            "For mutations of replicated tables, the array contains one record for each partition, with the block number that was acquired by the mutation. "
            "Only parts that contain blocks with numbers less than this number will be mutated in the partition. "
            "In non-replicated tables, block numbers in all partitions form a single sequence, so the array holds exactly one number, paired with the empty partition id above, "
            "and that number applies to every partition of the table."
        },
        { "parts_in_progress_names",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of names of data parts that are currently being mutated."},
        { "parts_to_do_names",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of names of data parts that need to be mutated for the mutation to complete."},
        { "parts_to_do",                   std::make_shared<DataTypeInt64>(), "The number of data parts that need to be mutated for the mutation to complete. Note: even if `parts_to_do` = 0, a mutation of a replicated table may not be completed yet due to a long-running INSERT that is creating a new data part that will need to be mutated."},
        { "bytes_to_do",                   std::make_shared<DataTypeUInt64>(), "The total size on disk of the data parts that need to be mutated for the mutation to complete. Byte-weighted counterpart of `parts_to_do`. "
            "On a replicated table only the parts already on this replica have a size, so this is a lower bound while the replica still has parts to fetch or merge."},
        { "progress",                      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "The estimated fraction of the mutation's work that is finished, from 0 to 1: the on-disk size of the remaining parts relative to the size of the parts the mutation is "
            "responsible for rewriting, including the live fraction of the parts currently being rewritten (rows of `system.merges` with `is_mutation` = 1). "
            "A part is in that scope once its `min_block_number` precedes the mutation's block number, so a part inserted afterwards does not count towards it — unless a merge folded "
            "that part into one that does, which the mutation then has to rewrite whole, and whose bytes therefore do reach `progress`. "
            "Both sides are measured against the table as it stands, not against a snapshot taken when the mutation was submitted, so the value is an estimate: a regular merge can "
            "retire pending parts at any moment, which makes `progress` jump forward. An already-rewritten part also weighs its new size while the parts still to be rewritten weigh "
            "their old one, so `progress` is understated for a mutation that shrinks parts and overstated for one that grows them. `DELETE WHERE 1` is the worst case: each finished "
            "part becomes an empty part, so `progress` stays near 0 until the last part is rewritten and should be read as a lower bound. "
            "`NULL` when the remaining work is not known yet. On a replicated table that happens while a mutation that is not done waits for an in-flight INSERT whose part is not "
            "committed (`parts_to_do` = 0), and while any part it still has to rewrite has not been fetched or merged on this replica: neither has a size on disk to weigh."},
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
            res_columns[col_num++]->insert(block_partition_ids);
            res_columns[col_num++]->insert(block_numbers);
            res_columns[col_num++]->insert(parts_in_progress_names);
            res_columns[col_num++]->insert(parts_to_do_names);
            res_columns[col_num++]->insert(parts_to_do_names.size());
            res_columns[col_num++]->insert(status.bytes_to_do);
            res_columns[col_num++]->insert(status.progress ? Field(*status.progress) : Field());
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
