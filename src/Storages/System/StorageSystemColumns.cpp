#include <optional>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/queryToString.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/NullSource.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
}

StorageSystemColumns::StorageSystemColumns(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        { "database",           std::make_shared<DataTypeString>() },
        { "table",              std::make_shared<DataTypeString>() },
        { "name",               std::make_shared<DataTypeString>() },
        { "type",               std::make_shared<DataTypeString>() },
        { "position",           std::make_shared<DataTypeUInt64>() },
        { "default_kind",       std::make_shared<DataTypeString>() },
        { "default_expression", std::make_shared<DataTypeString>() },
        { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>() },
        { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>() },
        { "marks_bytes",                std::make_shared<DataTypeUInt64>() },
        { "comment",                    std::make_shared<DataTypeString>() },
        { "is_in_partition_key", std::make_shared<DataTypeUInt8>() },
        { "is_in_sorting_key",   std::make_shared<DataTypeUInt8>() },
        { "is_in_primary_key",   std::make_shared<DataTypeUInt8>() },
        { "is_in_sampling_key",  std::make_shared<DataTypeUInt8>() },
        { "compression_codec",   std::make_shared<DataTypeString>() },
        { "character_octet_length",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },
        { "numeric_precision",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },
        { "numeric_precision_radix",    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },
        { "numeric_scale",              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },
        { "datetime_precision",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },

    }));
    setInMemoryMetadata(storage_metadata);
}


namespace
{
    using Storages = std::map<std::pair<std::string, std::string>, StoragePtr>;
}


class ColumnsSource : public ISource
{
public:
    ColumnsSource(
        std::vector<UInt8> columns_mask_,
        Block header_,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        Storages storages_,
        ContextPtr context)
        : ISource(header_)
        , columns_mask(std::move(columns_mask_)), max_block_size(max_block_size_)
        , databases(std::move(databases_)), tables(std::move(tables_)), storages(std::move(storages_))
        , total_tables(tables->size()), access(context->getAccess())
        , query_id(context->getCurrentQueryId()), lock_acquire_timeout(context->getSettingsRef().lock_acquire_timeout)
    {
    }

    String getName() const override { return "Columns"; }

protected:
    Chunk generate() override
    {
        if (db_table_num >= total_tables)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        size_t rows_count = 0;

        const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_COLUMNS);

        while (rows_count < max_block_size && db_table_num < total_tables)
        {
            const std::string database_name = (*databases)[db_table_num].get<std::string>();
            const std::string table_name = (*tables)[db_table_num].get<std::string>();
            ++db_table_num;

            ColumnsDescription columns;
            Names cols_required_for_partition_key;
            Names cols_required_for_sorting_key;
            Names cols_required_for_primary_key;
            Names cols_required_for_sampling;
            IStorage::ColumnSizeByName column_sizes;

            {
                StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
                TableLockHolder table_lock;

                try
                {
                    table_lock = storage->lockForShare(query_id, lock_acquire_timeout);
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

                auto metadata_snapshot = storage->getInMemoryMetadataPtr();
                columns = metadata_snapshot->getColumns();

                cols_required_for_partition_key = metadata_snapshot->getColumnsRequiredForPartitionKey();
                cols_required_for_sorting_key = metadata_snapshot->getColumnsRequiredForSortingKey();
                cols_required_for_primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
                cols_required_for_sampling = metadata_snapshot->getColumnsRequiredForSampling();
                column_sizes = storage->getColumnSizes();
            }

            bool check_access_for_columns = check_access_for_tables && !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name);

            size_t position = 0;
            for (const auto & column : columns)
            {
                ++position;
                if (check_access_for_columns && !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name, column.name))
                    continue;

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
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(position);

                if (column.default_desc.expression)
                {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(toString(column.default_desc.kind));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(queryToString(column.default_desc.expression));
                }
                else
                {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
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
                            res_columns[res_index++]->insert(it->second.data_compressed);
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(it->second.data_uncompressed);
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(it->second.marks);
                    }
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.comment);

                {
                    auto find_in_vector = [&key = column.name](const Names& names)
                    {
                        return std::find(names.cbegin(), names.cend(), key) != names.end();
                    };

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_partition_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sorting_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_primary_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sampling));
                }

                if (columns_mask[src_index++])
                {
                    if (column.codec)
                        res_columns[res_index++]->insert(queryToString(column.codec));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// character_octet_length makes sense for FixedString only
                DataTypePtr not_nullable_type = removeNullable(column.type);
                if (columns_mask[src_index++])
                {
                    if (isFixedString(not_nullable_type))
                        res_columns[res_index++]->insert(not_nullable_type->getSizeOfValueInMemory());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_precision
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(not_nullable_type->getSizeOfValueInMemory() * 8);  /// radix is 2
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(getDecimalPrecision(*not_nullable_type));  /// radix is 10
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_precision_radix
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(2);
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(10);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_scale
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(0);
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(getDecimalScale(*not_nullable_type));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// datetime_precision
                if (columns_mask[src_index++])
                {
                    if (isDateTime64(not_nullable_type))
                        res_columns[res_index++]->insert(assert_cast<const DataTypeDateTime64 &>(*not_nullable_type).getScale());
                    else if (isDateOrDate32(not_nullable_type) || isDateTime(not_nullable_type) || isDateTime64(not_nullable_type))
                        res_columns[res_index++]->insert(0);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                ++rows_count;
            }
        }

        return Chunk(std::move(res_columns), rows_count);
    }

private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    ColumnPtr tables;
    Storages storages;
    size_t db_table_num = 0;
    size_t total_tables;
    std::shared_ptr<const ContextAccess> access;
    String query_id;
    std::chrono::milliseconds lock_acquire_timeout;
};


Pipe StorageSystemColumns::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    Block block_to_filter;
    Storages storages;
    Pipes pipes;

    {
        /// Add `database` column.
        MutableColumnPtr database_column_mut = ColumnString::create();

        const auto databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & [database_name, database] : databases)
        {
            if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
                continue; /// We don't want to show the internal database for temporary tables in system.columns

            /// We are skipping "Lazy" database because we cannot afford initialization of all its tables.
            /// This should be documented.

            if (database->getEngineName() != "Lazy")
                database_column_mut->insert(database_name);
        }

        Tables external_tables;
        if (context->hasSessionContext())
        {
            external_tables = context->getSessionContext()->getExternalTables();
            if (!external_tables.empty())
                database_column_mut->insertDefault(); /// Empty database for external tables.
        }

        block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

        /// Filter block with `database` column.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

        if (!block_to_filter.rows())
        {
            pipes.emplace_back(std::make_shared<NullSource>(header));
            return Pipe::unitePipes(std::move(pipes));
        }

        ColumnPtr & database_column = block_to_filter.getByName("database").column;

        /// Add `table` column.
        MutableColumnPtr table_column_mut = ColumnString::create();
        IColumn::Offsets offsets(database_column->size());

        for (size_t i = 0; i < database_column->size(); ++i)
        {
            const std::string database_name = (*database_column)[i].get<std::string>();
            if (database_name.empty())
            {
                for (auto & [table_name, table] : external_tables)
                {
                    storages[{"", table_name}] = table;
                    table_column_mut->insert(table_name);
                }
            }
            else
            {
                const DatabasePtr & database = databases.at(database_name);
                for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
                {
                    if (const auto & table = iterator->table())
                    {
                        const String & table_name = iterator->name();
                        storages[{database_name, table_name}] = table;
                        table_column_mut->insert(table_name);
                    }
                }
            }
            offsets[i] = table_column_mut->size();
        }

        database_column = database_column->replicate(offsets);
        block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    }

    /// Filter block with `database` and `table` columns.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
    {
        pipes.emplace_back(std::make_shared<NullSource>(header));
        return Pipe::unitePipes(std::move(pipes));
    }

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;

    pipes.emplace_back(std::make_shared<ColumnsSource>(
            std::move(columns_mask), std::move(header), max_block_size,
            std::move(filtered_database_column), std::move(filtered_table_column),
            std::move(storages), context));

    return Pipe::unitePipes(std::move(pipes));
}

}
