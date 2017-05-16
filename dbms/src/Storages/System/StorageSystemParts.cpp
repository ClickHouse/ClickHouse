#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>


namespace DB
{


StorageSystemParts::StorageSystemParts(const std::string & name_)
    : name(name_),
    columns
    {
        {"partition",           std::make_shared<DataTypeString>()},
        {"name",                std::make_shared<DataTypeString>()},
        {"replicated",          std::make_shared<DataTypeUInt8>()},
        {"active",              std::make_shared<DataTypeUInt8>()},
        {"marks",               std::make_shared<DataTypeUInt64>()},
        {"rows",                std::make_shared<DataTypeUInt64>()},
        {"bytes",               std::make_shared<DataTypeUInt64>()},
        {"modification_time",   std::make_shared<DataTypeDateTime>()},
        {"remove_time",         std::make_shared<DataTypeDateTime>()},
        {"refcount",            std::make_shared<DataTypeUInt32>()},
        {"min_date",            std::make_shared<DataTypeDate>()},
        {"max_date",            std::make_shared<DataTypeDate>()},
        {"min_block_number",    std::make_shared<DataTypeInt64>()},
        {"max_block_number",    std::make_shared<DataTypeInt64>()},
        {"level",               std::make_shared<DataTypeUInt32>()},
        {"primary_key_bytes_in_memory", std::make_shared<DataTypeUInt64>()},
        {"primary_key_bytes_in_memory_allocated", std::make_shared<DataTypeUInt64>()},

        {"database",            std::make_shared<DataTypeString>()},
        {"table",               std::make_shared<DataTypeString>()},
        {"engine",              std::make_shared<DataTypeString>()},
    }
{
}

StoragePtr StorageSystemParts::create(const std::string & name_)
{
    return make_shared(name_);
}


BlockInputStreams StorageSystemParts::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    /// Will apply WHERE to subset of columns and then add more columns.
    /// This is kind of complicated, but we use WHERE to do less work.

    Block block_to_filter;

    std::map<std::pair<String, String>, StoragePtr> storages;

    {
        Databases databases = context.getDatabases();

        /// Add column 'database'.
        ColumnPtr database_column = std::make_shared<ColumnString>();
        for (const auto & database : databases)
            database_column->insert(database.first);
        block_to_filter.insert(ColumnWithTypeAndName(database_column, std::make_shared<DataTypeString>(), "database"));

        /// Filter block_to_filter with column 'database'.
        VirtualColumnUtils::filterBlockWithQuery(query, block_to_filter, context);

        if (!block_to_filter.rows())
            return BlockInputStreams();

        /// Add columns 'table', 'engine', 'active', 'replicated'.
        database_column = block_to_filter.getByName("database").column;
        size_t rows = database_column->size();

        IColumn::Offsets_t offsets(rows);
        ColumnPtr table_column = std::make_shared<ColumnString>();
        ColumnPtr engine_column = std::make_shared<ColumnString>();
        ColumnPtr replicated_column = std::make_shared<ColumnUInt8>();
        ColumnPtr active_column = std::make_shared<ColumnUInt8>();

        for (size_t i = 0; i < rows; ++i)
        {
            String database_name = (*database_column)[i].get<String>();
            const DatabasePtr database = databases.at(database_name);

            offsets[i] = i ? offsets[i - 1] : 0;
            for (auto iterator = database->getIterator(); iterator->isValid(); iterator->next())
            {
                String table_name = iterator->name();
                StoragePtr storage = iterator->table();
                String engine_name = storage->getName();

                if (!dynamic_cast<StorageMergeTree *>(&*storage) &&
                    !dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
                    continue;

                storages[std::make_pair(database_name, iterator->name())] = storage;

                /// Add all four combinations of flags 'replicated' and 'active'.
                for (UInt64 replicated : {0, 1})
                {
                    for (UInt64 active : {0, 1})
                    {
                        table_column->insert(table_name);
                        engine_column->insert(engine_name);
                        replicated_column->insert(replicated);
                        active_column->insert(active);
                    }
                }

                offsets[i] += 4;
            }
        }

        for (size_t i = 0; i < block_to_filter.columns(); ++i)
        {
            ColumnPtr & column = block_to_filter.safeGetByPosition(i).column;
            column = column->replicate(offsets);
        }

        block_to_filter.insert(ColumnWithTypeAndName(table_column, std::make_shared<DataTypeString>(), "table"));
        block_to_filter.insert(ColumnWithTypeAndName(engine_column, std::make_shared<DataTypeString>(), "engine"));
        block_to_filter.insert(ColumnWithTypeAndName(replicated_column, std::make_shared<DataTypeUInt8>(), "replicated"));
        block_to_filter.insert(ColumnWithTypeAndName(active_column, std::make_shared<DataTypeUInt8>(), "active"));
    }

    /// Filter block_to_filter with columns 'database', 'table', 'engine', 'replicated', 'active'.
    VirtualColumnUtils::filterBlockWithQuery(query, block_to_filter, context);

    /// If all was filtered out.
    if (!block_to_filter.rows())
        return {};

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;
    ColumnPtr filtered_replicated_column = block_to_filter.getByName("replicated").column;
    ColumnPtr filtered_active_column = block_to_filter.getByName("active").column;

    /// Finally, create the result.

    Block block = getSampleBlock();

    for (size_t i = 0; i < filtered_database_column->size();)
    {
        String database = (*filtered_database_column)[i].get<String>();
        String table = (*filtered_table_column)[i].get<String>();

        /// What combinations of 'replicated' and 'active' values we need.
        bool need[2][2]{}; /// [replicated][active]
        for (; i < filtered_database_column->size() &&
            (*filtered_database_column)[i].get<String>() == database &&
            (*filtered_table_column)[i].get<String>() == table; ++i)
        {
            bool replicated = !!(*filtered_replicated_column)[i].get<UInt64>();
            bool active = !!(*filtered_active_column)[i].get<UInt64>();
            need[replicated][active] = true;
        }

        StoragePtr storage = storages.at(std::make_pair(database, table));
        TableStructureReadLockPtr table_lock;

        try
        {
            table_lock = storage->lockStructure(false);    /// For table not to be dropped.
        }
        catch (const Exception & e)
        {
            /** There are case when IStorage::drop was called,
              *  but we still own the object.
              * Then table will throw exception at attempt to lock it.
              * Just skip the table.
              */
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                continue;
            else
                throw;
        }

        String engine = storage->getName();

        MergeTreeData * data[2]{}; /// [0] - unreplicated, [1] - replicated.

        if (StorageMergeTree * merge_tree = dynamic_cast<StorageMergeTree *>(&*storage))
        {
            data[0] = &merge_tree->getData();
        }
        else if (StorageReplicatedMergeTree * replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
        {
            data[0] = replicated_merge_tree->getUnreplicatedData();
            data[1] = &replicated_merge_tree->getData();
        }

        for (UInt64 replicated = 0; replicated <= 1; ++replicated)
        {
            if (!need[replicated][0] && !need[replicated][1])
                continue;
            if (!data[replicated])
                continue;

            MergeTreeData::DataParts active_parts = data[replicated]->getDataParts();
            MergeTreeData::DataParts all_parts;
            if (need[replicated][0])
                all_parts = data[replicated]->getAllDataParts();
            else
                all_parts = active_parts;

            /// Finally, we'll go through the list of parts.
            for (const MergeTreeData::DataPartPtr & part : all_parts)
            {
                LocalDate partition_date {part->month};
                String partition = toString(partition_date.year()) + (partition_date.month() < 10 ? "0" : "") + toString(partition_date.month());

                size_t i = 0;
                block.getByPosition(i++).column->insert(partition);
                block.getByPosition(i++).column->insert(part->name);
                block.getByPosition(i++).column->insert(replicated);
                block.getByPosition(i++).column->insert(static_cast<UInt64>(!need[replicated][0] || active_parts.count(part)));
                block.getByPosition(i++).column->insert(part->size);
                block.getByPosition(i++).column->insert(part->getExactSizeRows());
                block.getByPosition(i++).column->insert(static_cast<size_t>(part->size_in_bytes));
                block.getByPosition(i++).column->insert(part->modification_time);
                block.getByPosition(i++).column->insert(part->remove_time);

                /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
                block.getByPosition(i++).column->insert(part.use_count() - (active_parts.count(part) ? 2 : 1));

                block.getByPosition(i++).column->insert(static_cast<UInt64>(part->left_date));
                block.getByPosition(i++).column->insert(static_cast<UInt64>(part->right_date));
                block.getByPosition(i++).column->insert(part->left);
                block.getByPosition(i++).column->insert(part->right);
                block.getByPosition(i++).column->insert(static_cast<UInt64>(part->level));
                block.getByPosition(i++).column->insert(part->getIndexSizeInBytes());
                block.getByPosition(i++).column->insert(part->getIndexSizeInAllocatedBytes());

                block.getByPosition(i++).column->insert(database);
                block.getByPosition(i++).column->insert(table);
                block.getByPosition(i++).column->insert(engine);
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
