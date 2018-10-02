#include <optional>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageSystemColumns::StorageSystemColumns(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription(
    {
        { "database",           std::make_shared<DataTypeString>() },
        { "table",              std::make_shared<DataTypeString>() },
        { "name",               std::make_shared<DataTypeString>() },
        { "type",               std::make_shared<DataTypeString>() },
        { "default_kind",       std::make_shared<DataTypeString>() },
        { "default_expression", std::make_shared<DataTypeString>() },
        { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>() },
        { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>() },
        { "marks_bytes",                std::make_shared<DataTypeUInt64>() },
    }));
}


namespace
{
    using Storages = std::map<std::pair<std::string, std::string>, StoragePtr>;
}


class ColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    ColumnsBlockInputStream(
        const std::vector<UInt8> & columns_mask,
        const Block & header,
        size_t max_block_size,
        ColumnPtr databases,
        ColumnPtr tables,
        Storages storages)
        : columns_mask(columns_mask), header(header), max_block_size(max_block_size),
        databases(databases), tables(tables), storages(std::move(storages)), total_tables(tables->size()) {}

    String getName() const override { return "Columns"; }
    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (db_table_num >= total_tables)
            return {};

        Block res = header;
        MutableColumns res_columns = header.cloneEmptyColumns();
        size_t rows_count = 0;

        while (rows_count < max_block_size && db_table_num < total_tables)
        {
            const std::string database_name = (*databases)[db_table_num].get<std::string>();
            const std::string table_name = (*tables)[db_table_num].get<std::string>();
            ++db_table_num;

            NamesAndTypesList columns;
            ColumnDefaults column_defaults;
            MergeTreeData::ColumnSizeByName column_sizes;

            {
                StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
                TableStructureReadLockPtr table_lock;

                try
                {
                    table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
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

                columns = storage->getColumns().getAll();
                column_defaults = storage->getColumns().defaults;

                /** Info about sizes of columns for tables of MergeTree family.
                * NOTE: It is possible to add getter for this info to IStorage interface.
                */
                if (auto storage_concrete_plain = dynamic_cast<StorageMergeTree *>(storage.get()))
                {
                    column_sizes = storage_concrete_plain->getData().getColumnSizes();
                }
                else if (auto storage_concrete_replicated = dynamic_cast<StorageReplicatedMergeTree *>(storage.get()))
                {
                    column_sizes = storage_concrete_replicated->getData().getColumnSizes();
                }
            }

            for (const auto & column : columns)
            {
                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.type->getName());

                {
                    const auto it = column_defaults.find(column.name);
                    if (it == std::end(column_defaults))
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }
                    else
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(toString(it->second.kind));
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(queryToString(it->second.expression));
                    }
                }

                {
                    const auto it = column_sizes.find(column.name);
                    if (it == std::end(column_sizes))
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }
                    else
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(static_cast<UInt64>(it->second.data_compressed));
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(static_cast<UInt64>(it->second.data_uncompressed));
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(static_cast<UInt64>(it->second.marks));
                    }
                }

                ++rows_count;
            }
        }

        res.setColumns(std::move(res_columns));
        return res;
    }

private:
    std::vector<UInt8> columns_mask;
    Block header;
    size_t max_block_size;
    ColumnPtr databases;
    ColumnPtr tables;
    Storages storages;
    size_t db_table_num = 0;
    size_t total_tables;
};


BlockInputStreams StorageSystemColumns::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    check(column_names);

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = getSampleBlock();
    Block res_block;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            res_block.insert(sample_block.getByPosition(i));
        }
    }

    Block block_to_filter;
    Storages storages;

    {
        Databases databases = context.getDatabases();

        /// Add `database` column.
        MutableColumnPtr database_column_mut = ColumnString::create();
        for (const auto & database : databases)
        {
            if (context.hasDatabaseAccessRights(database.first))
                database_column_mut->insert(database.first);
        }

        block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

        /// Filter block with `database` column.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

        if (!block_to_filter.rows())
            return {std::make_shared<NullBlockInputStream>(res_block)};

        ColumnPtr & database_column = block_to_filter.getByName("database").column;
        size_t rows = database_column->size();

        /// Add `table` column.
        MutableColumnPtr table_column_mut = ColumnString::create();
        IColumn::Offsets offsets(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string database_name = (*database_column)[i].get<std::string>();
            const DatabasePtr database = databases.at(database_name);
            offsets[i] = i ? offsets[i - 1] : 0;

            for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            {
                const String & table_name = iterator->name();
                storages.emplace(std::piecewise_construct,
                    std::forward_as_tuple(database_name, table_name),
                    std::forward_as_tuple(iterator->table()));
                table_column_mut->insert(table_name);
                ++offsets[i];
            }
        }

        database_column = database_column->replicate(offsets);
        block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    }

    /// Filter block with `database` and `table` columns.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return {std::make_shared<NullBlockInputStream>(res_block)};

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;

    return {std::make_shared<ColumnsBlockInputStream>(
        std::move(columns_mask), std::move(res_block), max_block_size,
        std::move(filtered_database_column), std::move(filtered_table_column), std::move(storages))};
}

}
