#include <Common/escapeForFileName.h>
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
        {"active",              std::make_shared<DataTypeUInt8>()},
        {"marks",               std::make_shared<DataTypeUInt64>()},
        {"marks_size",          std::make_shared<DataTypeUInt64>()},
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
        {"engine",              std::make_shared<DataTypeString>()}
    }
{
}


BlockInputStreams StorageSystemParts::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    bool has_state_column = false;
    Names real_column_names;

    for (const String & column_name : column_names)
    {
        if (column_name == "_state")
            has_state_column = true;
        else
            real_column_names.emplace_back(column_name);
    }

    /// Do not check if only _state column is requested
    if (!(has_state_column && real_column_names.empty()))
        check(real_column_names);

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
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

        if (!block_to_filter.rows())
            return BlockInputStreams();

        /// Add columns 'table', 'engine', 'active'
        database_column = block_to_filter.getByName("database").column;
        size_t rows = database_column->size();

        IColumn::Offsets_t offsets(rows);
        ColumnPtr table_column = std::make_shared<ColumnString>();
        ColumnPtr engine_column = std::make_shared<ColumnString>();
        ColumnPtr active_column = std::make_shared<ColumnUInt8>();

        for (size_t i = 0; i < rows; ++i)
        {
            String database_name = (*database_column)[i].get<String>();
            const DatabasePtr database = databases.at(database_name);

            offsets[i] = i ? offsets[i - 1] : 0;
            for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            {
                String table_name = iterator->name();
                StoragePtr storage = iterator->table();
                String engine_name = storage->getName();

                if (!dynamic_cast<StorageMergeTree *>(&*storage) &&
                    !dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
                    continue;

                storages[std::make_pair(database_name, iterator->name())] = storage;

                /// Add all combinations of flag 'active'.
                for (UInt64 active : {0, 1})
                {
                    table_column->insert(table_name);
                    engine_column->insert(engine_name);
                    active_column->insert(active);
                }

                offsets[i] += 2;
            }
        }

        for (size_t i = 0; i < block_to_filter.columns(); ++i)
        {
            ColumnPtr & column = block_to_filter.safeGetByPosition(i).column;
            column = column->replicate(offsets);
        }

        block_to_filter.insert(ColumnWithTypeAndName(table_column, std::make_shared<DataTypeString>(), "table"));
        block_to_filter.insert(ColumnWithTypeAndName(engine_column, std::make_shared<DataTypeString>(), "engine"));
        block_to_filter.insert(ColumnWithTypeAndName(active_column, std::make_shared<DataTypeUInt8>(), "active"));
    }

    /// Filter block_to_filter with columns 'database', 'table', 'engine', 'active'.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    /// If all was filtered out.
    if (!block_to_filter.rows())
        return {};

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;
    ColumnPtr filtered_active_column = block_to_filter.getByName("active").column;

    /// Finally, create the result.

    Block block = getSampleBlock();
    if (has_state_column)
        block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "_state"));

    for (size_t i = 0; i < filtered_database_column->size();)
    {
        String database = (*filtered_database_column)[i].get<String>();
        String table = (*filtered_table_column)[i].get<String>();

        /// What 'active' value we need.
        bool need[2]{}; /// [active]
        for (; i < filtered_database_column->size() &&
            (*filtered_database_column)[i].get<String>() == database &&
            (*filtered_table_column)[i].get<String>() == table; ++i)
        {
            bool active = !!(*filtered_active_column)[i].get<UInt64>();
            need[active] = true;
        }

        StoragePtr storage = storages.at(std::make_pair(database, table));
        TableStructureReadLockPtr table_lock;

        try
        {
            table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);    /// For table not to be dropped.
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

            throw;
        }

        String engine = storage->getName();

        MergeTreeData * data = nullptr;

        if (auto merge_tree = dynamic_cast<StorageMergeTree *>(&*storage))
        {
            data = &merge_tree->getData();
        }
        else if (auto replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(&*storage))
        {
            data = &replicated_merge_tree->getData();
        }
        else
        {
            throw Exception("Unknown engine " + engine, ErrorCodes::LOGICAL_ERROR);
        }

        using State = MergeTreeDataPart::State;
        MergeTreeData::DataPartStateVector all_parts_state;
        MergeTreeData::DataPartsVector all_parts;

        if (need[0])
        {
            /// If has_state_column is requested, return all states
            if (!has_state_column)
                all_parts = data->getDataPartsVector({State::Committed, State::Outdated}, &all_parts_state);
            else
                all_parts = data->getAllDataPartsVector(&all_parts_state);
        }
        else
            all_parts = data->getDataPartsVector({State::Committed}, &all_parts_state);


        /// Finally, we'll go through the list of parts.
        for (size_t part_number = 0; part_number < all_parts.size(); ++part_number)
        {
            const auto & part = all_parts[part_number];
            auto part_state = all_parts_state[part_number];

            size_t i = 0;
            {
                WriteBufferFromOwnString out;
                part->partition.serializeTextQuoted(*data, out);
                block.getByPosition(i++).column->insert(out.str());
            }
            block.getByPosition(i++).column->insert(part->name);
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part_state == State::Committed));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->marks_count));

            size_t marks_size = 0;
            for (const NameAndTypePair & it : part->columns)
            {
                String name = escapeForFileName(it.name);
                auto checksum = part->checksums.files.find(name + ".mrk");
                if (checksum != part->checksums.files.end())
                    marks_size += checksum->second.file_size;
            }
            block.getByPosition(i++).column->insert(static_cast<UInt64>(marks_size));

            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->rows_count));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->size_in_bytes));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->modification_time));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->remove_time));

            /// For convenience, in returned refcount, don't add references that was due to local variables in this method: all_parts, active_parts.
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part.use_count() - 1));

            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getMinDate()));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getMaxDate()));
            block.getByPosition(i++).column->insert(part->info.min_block);
            block.getByPosition(i++).column->insert(part->info.max_block);
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->info.level));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getIndexSizeInBytes()));
            block.getByPosition(i++).column->insert(static_cast<UInt64>(part->getIndexSizeInAllocatedBytes()));

            block.getByPosition(i++).column->insert(database);
            block.getByPosition(i++).column->insert(table);
            block.getByPosition(i++).column->insert(engine);

            if (has_state_column)
                block.getByPosition(i++).column->insert(part->stateString());
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}

NameAndTypePair StorageSystemParts::getColumn(const String & column_name) const
{
    if (column_name == "_state")
        return NameAndTypePair("_state", std::make_shared<DataTypeString>());

    return ITableDeclaration::getColumn(column_name);
}

bool StorageSystemParts::hasColumn(const String & column_name) const
{
    if (column_name == "_state")
        return true;

    return ITableDeclaration::hasColumn(column_name);
}


}
