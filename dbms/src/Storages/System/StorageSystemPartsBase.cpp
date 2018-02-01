#include <Storages/System/StorageSystemPartsBase.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Parsers/queryToString.h>


namespace DB
{

bool StorageSystemPartsBase::hasStateColumn(const Names & column_names)
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

    return has_state_column;
}


class StoragesInfoStream
{
public:
    StoragesInfoStream(const SelectQueryInfo & query_info, const Context & context, bool has_state_column)
            : has_state_column(has_state_column)
    {
        /// Will apply WHERE to subset of columns and then add more columns.
        /// This is kind of complicated, but we use WHERE to do less work.

        Block block_to_filter;

        MutableColumnPtr table_column_mut = ColumnString::create();
        MutableColumnPtr engine_column_mut = ColumnString::create();
        MutableColumnPtr active_column_mut = ColumnUInt8::create();

        {
            Databases databases = context.getDatabases();

            /// Add column 'database'.
            MutableColumnPtr database_column_mut = ColumnString::create();
            for (const auto & database : databases)
                database_column_mut->insert(database.first);
            block_to_filter.insert(ColumnWithTypeAndName(
                    std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

            /// Filter block_to_filter with column 'database'.
            VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);
            rows = block_to_filter.rows();

            /// Block contains new columns, update database_column.
            ColumnPtr database_column = block_to_filter.getByName("database").column;

            if (rows)
            {
                /// Add columns 'table', 'engine', 'active'

                IColumn::Offsets offsets(rows);

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
                            table_column_mut->insert(table_name);
                            engine_column_mut->insert(engine_name);
                            active_column_mut->insert(active);
                        }

                        offsets[i] += 2;
                    }
                }

                for (size_t i = 0; i < block_to_filter.columns(); ++i)
                {
                    ColumnPtr & column = block_to_filter.safeGetByPosition(i).column;
                    column = column->replicate(offsets);
                }
            }
        }

        if (rows)
        {
            block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
            block_to_filter.insert(ColumnWithTypeAndName(std::move(engine_column_mut), std::make_shared<DataTypeString>(), "engine"));
            block_to_filter.insert(ColumnWithTypeAndName(std::move(active_column_mut), std::make_shared<DataTypeUInt8>(), "active"));

            /// Filter block_to_filter with columns 'database', 'table', 'engine', 'active'.
            VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);
            rows = block_to_filter.rows();
        }

        database_column = block_to_filter.getByName("database").column;
        table_column = block_to_filter.getByName("table").column;
        active_column = block_to_filter.getByName("active").column;

        next_row = 0;
    }

    StorageSystemPartsBase::StoragesInfo next()
    {
        StorageSystemPartsBase::StoragesInfo info;
        info.storage = nullptr;

        while (next_row < rows)
        {

            info.database = (*database_column)[next_row].get<String>();
            info.table = (*table_column)[next_row].get<String>();

            auto isSameTable = [& info, this] (size_t next_row) -> bool
            {
                return (*database_column)[next_row].get<String>() == info.database &&
                       (*table_column)[next_row].get<String>() == info.table;
            };

            /// What 'active' value we need.
            bool need[2]{}; /// [active]
            for (; next_row < rows && isSameTable(next_row); ++next_row)
            {
                bool active = (*active_column)[next_row].get<UInt64>() != 0;
                need[active] = true;
            }

            info.storage = storages.at(std::make_pair(info.database, info.table));

            try
            {
                /// For table not to be dropped.
                info.table_lock = info.storage->lockStructure(false, __PRETTY_FUNCTION__);
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

            info.engine = info.storage->getName();

            info.data = nullptr;

            if (auto merge_tree = dynamic_cast<StorageMergeTree *>(&*info.storage))
            {
                info.data = &merge_tree->getData();
            }
            else if (auto replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(&*info.storage))
            {
                info.data = &replicated_merge_tree->getData();
            }
            else
            {
                throw Exception("Unknown engine " + info.engine, ErrorCodes::LOGICAL_ERROR);
            }

            using State = MergeTreeDataPart::State;
            auto & all_parts_state = info.all_parts_state;
            auto & all_parts = info.all_parts;

            if (need[0])
            {
                /// If has_state_column is requested, return all states.
                if (!has_state_column)
                    all_parts = info.data->getDataPartsVector({State::Committed, State::Outdated}, &all_parts_state);
                else
                    all_parts = info.data->getAllDataPartsVector(&all_parts_state);
            }
            else
                all_parts = info.data->getDataPartsVector({State::Committed}, &all_parts_state);

            break;
        }

        return info;
    }

private:
    bool has_state_column;

    ColumnPtr database_column;
    ColumnPtr table_column;
    ColumnPtr active_column;

    size_t next_row;
    size_t rows;

    using StoragesMap = std::map<std::pair<String, String>, StoragePtr>;
    StoragesMap storages;
};


BlockInputStreams StorageSystemPartsBase::read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/)
{
    bool has_state_column = hasStateColumn(column_names);

    processed_stage = QueryProcessingStage::FetchColumns;

    StoragesInfoStream stream(query_info, context, has_state_column);

    /// Create the result.

    MutableColumns columns = getSampleBlock().cloneEmptyColumns();
    if (has_state_column)
        columns.push_back(ColumnString::create());

    while (StoragesInfo info = stream.next())
    {
        processNextStorage(columns, info, has_state_column);
    }

    Block block = getSampleBlock();
    if (has_state_column)
        block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "_state"));

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block.cloneWithColumns(std::move(columns))));
}

NameAndTypePair StorageSystemPartsBase::getColumn(const String & column_name) const
{
    if (column_name == "_state")
        return NameAndTypePair("_state", std::make_shared<DataTypeString>());

    return ITableDeclaration::getColumn(column_name);
}

bool StorageSystemPartsBase::hasColumn(const String & column_name) const
{
    if (column_name == "_state")
        return true;

    return ITableDeclaration::hasColumn(column_name);
}

StorageSystemPartsBase::StorageSystemPartsBase(std::string name_, NamesAndTypesList && columns_)
    : name(std::move(name_))
{
    columns = columns_;
}

}
