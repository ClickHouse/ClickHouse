#include <Storages/System/StorageSystemColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/VirtualColumnUtils.h>
#include <Parsers/queryToString.h>
#include <Databases/IDatabase.h>


namespace DB
{

StorageSystemColumns::StorageSystemColumns(const std::string & name_)
    : name(name_)
    , columns{
        { "database",           std::make_shared<DataTypeString>() },
        { "table",              std::make_shared<DataTypeString>() },
        { "name",               std::make_shared<DataTypeString>() },
        { "type",               std::make_shared<DataTypeString>() },
        { "default_kind",       std::make_shared<DataTypeString>() },
        { "default_expression", std::make_shared<DataTypeString>() },
        { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>() },
        { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>() },
        { "marks_bytes",                std::make_shared<DataTypeUInt64>() },
    }
{
}


BlockInputStreams StorageSystemColumns::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    Block block;

    std::map<std::pair<std::string, std::string>, StoragePtr> storages;

    {
        Databases databases = context.getDatabases();

        /// Add `database` column.
        ColumnPtr database_column = std::make_shared<ColumnString>();
        for (const auto & database : databases)
            database_column->insert(database.first);
        block.insert(ColumnWithTypeAndName(database_column, std::make_shared<DataTypeString>(), "database"));

        /// Filter block with `database` column.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);

        if (!block.rows())
            return BlockInputStreams();

        database_column = block.getByName("database").column;
        size_t rows = database_column->size();

        /// Add `table` column.
        ColumnPtr table_column = std::make_shared<ColumnString>();
        IColumn::Offsets_t offsets(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string database_name = (*database_column)[i].get<std::string>();
            const DatabasePtr database = databases.at(database_name);
            offsets[i] = i ? offsets[i - 1] : 0;

            for (auto iterator = database->getIterator(); iterator->isValid(); iterator->next())
            {
                const String & table_name = iterator->name();
                storages.emplace(std::piecewise_construct,
                    std::forward_as_tuple(database_name, table_name),
                    std::forward_as_tuple(iterator->table()));
                table_column->insert(table_name);
                offsets[i] += 1;
            }
        }

        for (size_t i = 0; i < block.columns(); ++i)
        {
            ColumnPtr & column = block.safeGetByPosition(i).column;
            column = column->replicate(offsets);
        }

        block.insert(ColumnWithTypeAndName(table_column, std::make_shared<DataTypeString>(), "table"));
    }

    /// Filter block with `database` and `table` columns.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);

    if (!block.rows())
        return BlockInputStreams();

    ColumnPtr filtered_database_column = block.getByName("database").column;
    ColumnPtr filtered_table_column = block.getByName("table").column;

    /// We compose the result.
    ColumnPtr database_column = std::make_shared<ColumnString>();
    ColumnPtr table_column = std::make_shared<ColumnString>();
    ColumnPtr name_column = std::make_shared<ColumnString>();
    ColumnPtr type_column = std::make_shared<ColumnString>();
    ColumnPtr default_kind_column = std::make_shared<ColumnString>();
    ColumnPtr default_expression_column = std::make_shared<ColumnString>();
    ColumnPtr data_compressed_bytes_column = std::make_shared<ColumnUInt64>();
    ColumnPtr data_uncompressed_bytes_column = std::make_shared<ColumnUInt64>();
    ColumnPtr marks_bytes_column = std::make_shared<ColumnUInt64>();

    size_t rows = filtered_database_column->size();
    for (size_t i = 0; i < rows; ++i)
    {
        const std::string database_name = (*filtered_database_column)[i].get<std::string>();
        const std::string table_name = (*filtered_table_column)[i].get<std::string>();

        NamesAndTypesList columns;
        ColumnDefaults column_defaults;
        MergeTreeData::ColumnSizes column_sizes;

        {
            StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
            TableStructureReadLockPtr table_lock;

            try
            {
                table_lock = storage->lockStructure(false);
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

            columns = storage->getColumnsList();
            columns.insert(std::end(columns), std::begin(storage->alias_columns), std::end(storage->alias_columns));
            column_defaults = storage->column_defaults;

            /** Info about sizes of columns for tables of MergeTree family.
              * NOTE: It is possible to add getter for this info to IStorage interface.
              */
            if (auto storage_concrete = dynamic_cast<StorageMergeTree *>(storage.get()))
            {
                column_sizes = storage_concrete->getData().getColumnSizes();
            }
            else if (auto storage_concrete = dynamic_cast<StorageReplicatedMergeTree *>(storage.get()))
            {
                column_sizes = storage_concrete->getData().getColumnSizes();
            }
        }

        for (const auto & column : columns)
        {
            database_column->insert(database_name);
            table_column->insert(table_name);
            name_column->insert(column.name);
            type_column->insert(column.type->getName());

            {
                const auto it = column_defaults.find(column.name);
                if (it == std::end(column_defaults))
                {
                    default_kind_column->insertDefault();
                    default_expression_column->insertDefault();
                }
                else
                {
                    default_kind_column->insert(toString(it->second.type));
                    default_expression_column->insert(queryToString(it->second.expression));
                }
            }

            {
                const auto it = column_sizes.find(column.name);
                if (it == std::end(column_sizes))
                {
                    data_compressed_bytes_column->insertDefault();
                    data_uncompressed_bytes_column->insertDefault();
                    marks_bytes_column->insertDefault();
                }
                else
                {
                    data_compressed_bytes_column->insert(it->second.data_compressed);
                    data_uncompressed_bytes_column->insert(it->second.data_uncompressed);
                    marks_bytes_column->insert(it->second.marks);
                }
            }
        }
    }

    block.clear();

    block.insert(ColumnWithTypeAndName(database_column, std::make_shared<DataTypeString>(), "database"));
    block.insert(ColumnWithTypeAndName(table_column, std::make_shared<DataTypeString>(), "table"));
    block.insert(ColumnWithTypeAndName(name_column, std::make_shared<DataTypeString>(), "name"));
    block.insert(ColumnWithTypeAndName(type_column, std::make_shared<DataTypeString>(), "type"));
    block.insert(ColumnWithTypeAndName(default_kind_column, std::make_shared<DataTypeString>(), "default_kind"));
    block.insert(ColumnWithTypeAndName(default_expression_column, std::make_shared<DataTypeString>(), "default_expression"));
    block.insert(ColumnWithTypeAndName(data_compressed_bytes_column, std::make_shared<DataTypeUInt64>(), "data_compressed_bytes"));
    block.insert(ColumnWithTypeAndName(data_uncompressed_bytes_column, std::make_shared<DataTypeUInt64>(), "data_uncompressed_bytes"));
    block.insert(ColumnWithTypeAndName(marks_bytes_column, std::make_shared<DataTypeUInt64>(), "marks_bytes"));

    return BlockInputStreams{ 1, std::make_shared<OneBlockInputStream>(block) };
}

}
