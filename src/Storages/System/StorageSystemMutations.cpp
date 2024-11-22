#include <Storages/System/StorageSystemMutations.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
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
        { "mutation_id",                   std::make_shared<DataTypeString>(), "The ID of the mutation. For replicated tables these IDs correspond to znode names in the <table_path_in_clickhouse_keeper>/mutations/ directory in ClickHouse Keeper. For non-replicated tables the IDs correspond to file names in the data directory of the table."},
        { "command",                       std::make_shared<DataTypeString>(), "The mutation command string (the part of the query after ALTER TABLE [db.]table)."},
        { "create_time",                   std::make_shared<DataTypeDateTime>(), "Date and time when the mutation command was submitted for execution."},
        { "block_numbers.partition_id",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "For mutations of replicated tables, the array contains the partitions' IDs (one record for each partition). For mutations of non-replicated tables the array is empty."},
        { "block_numbers.number",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()),
            "For mutations of replicated tables, the array contains one record for each partition, with the block number that was acquired by the mutation. "
            "Only parts that contain blocks with numbers less than this number will be mutated in the partition."
            "In non-replicated tables, block numbers in all partitions form a single sequence. "
            "This means that for mutations of non-replicated tables, the column will contain one record with a single block number acquired by the mutation."
        },
        { "parts_to_do_names",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of names of data parts that need to be mutated for the mutation to complete."},
        { "parts_to_do",                   std::make_shared<DataTypeInt64>(), "The number of data parts that need to be mutated for the mutation to complete."},
        { "is_done",                       std::make_shared<DataTypeUInt8>(),
            "The flag whether the mutation is done or not. Possible values: "
            "1 if the mutation is completed, "
            "0 if the mutation is still in process. "
        },
        { "is_killed",                    std::make_shared<DataTypeUInt8>(), "Only available in ClickHouse Cloud."},
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
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain MergeTree tables
        if (!db.second->canContainMergeTreeTables())
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

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);

            res_columns[col_num++]->insert(status.id);
            res_columns[col_num++]->insert(status.command);
            res_columns[col_num++]->insert(UInt64(status.create_time));
            res_columns[col_num++]->insert(block_partition_ids);
            res_columns[col_num++]->insert(block_numbers);
            res_columns[col_num++]->insert(parts_to_do_names);
            res_columns[col_num++]->insert(parts_to_do_names.size());
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
