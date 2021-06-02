#include <optional>
#include <Storages/System/StorageSystemColumnsIS.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSelectQuery.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/NullSource.h>
#include <Interpreters/Context.h>
#include <type_traits>
#include <cstdint>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
}

StorageSystemColumnsIS::StorageSystemColumnsIS(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        { "table_catalog",                              std::make_shared<DataTypeString>() },
//      { "table_schema",                               std::make_shared<DataTypeString>() },
        { "table_name",                                 std::make_shared<DataTypeString>() },
        { "column_name",                                std::make_shared<DataTypeString>() },
        { "ordinal_position",                           std::make_shared<DataTypeUInt64>() },
        { "column_default",                             std::make_shared<DataTypeString>() },
//      { "is_nullable",                                std::make_shared<DataTypeUInt8>() }, TODO
        { "data_type",                                  std::make_shared<DataTypeString>() },
        { "character_maximum_length",                   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) }, // always null?
        { "character_octet_maximum_length",             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) }, // always null?
        { "numeric_precision",                          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) }, // always null?
        { "numeric_scale",                              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) }, // always null?
        { "datetime_precision",                         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) }, // always null?
        { "character_set_name",                         std::make_shared<DataTypeString>() },
        { "collation_name",                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()) },
        { "default_kind",                               std::make_shared<DataTypeString>() },
        { "default_expression",                         std::make_shared<DataTypeString>() },
        { "data_compressed_bytes",                      std::make_shared<DataTypeUInt64>() },
        { "data_uncompressed_bytes",                    std::make_shared<DataTypeUInt64>() },
        { "marks_bytes",                                std::make_shared<DataTypeUInt64>() },
        { "comment",                                    std::make_shared<DataTypeString>() },
        { "is_in_partition_key",                        std::make_shared<DataTypeUInt8>() },
        { "is_in_sorting_key",                          std::make_shared<DataTypeUInt8>() },
        { "is_in_primary_key",                          std::make_shared<DataTypeUInt8>() },
        { "is_in_sampling_key",                         std::make_shared<DataTypeUInt8>() },
        { "compression_codec",                          std::make_shared<DataTypeString>() },
    }));
    setInMemoryMetadata(storage_metadata);
}


namespace
{
    using Storages = std::map<std::pair<std::string, std::string>, StoragePtr>;
}


class ColumnsSourceIS : public SourceWithProgress
{
public:
    ColumnsSourceIS(
        std::vector<UInt8> columns_mask_,
        Block header_,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        Storages storages_,
        const Context & context)
        : SourceWithProgress(header_)
        , columns_mask(std::move(columns_mask_)), max_block_size(max_block_size_)
        , databases(std::move(databases_)), tables(std::move(tables_)), storages(std::move(storages_))
        , total_tables(tables->size()), access(context.getAccess())
        , query_id(context.getCurrentQueryId()), lock_acquire_timeout(context.getSettingsRef().lock_acquire_timeout)
    {
    }

    String getName() const override { return "ColumnsIS"; }

protected:

    using NumericParams = std::pair<UInt64, UInt64>;
    std::unordered_map<TypeIndex, NumericParams> typeToNumberParams = 
    {
        {TypeIndex::Int8, {8, 0}},
        {TypeIndex::Int16, {16, 0}},
        {TypeIndex::Int32, {32, 0}},
        {TypeIndex::Int64, {64, 0}},
        {TypeIndex::Int128, {128, 0}},
        {TypeIndex::Int256, {256, 0}},
        {TypeIndex::UInt8, {8, 0}},
        {TypeIndex::UInt16, {16, 0}},
        {TypeIndex::UInt32, {32, 0}},
        {TypeIndex::UInt64, {64, 0}},
        {TypeIndex::UInt128, {128, 0}},
        {TypeIndex::UInt256, {256, 0}},
        {TypeIndex::Float32, {23, 23}},
        {TypeIndex::Float64, {52, 52}},
    };

    Chunk generate() override
    {
        if (db_table_num >= total_tables)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        size_t rows_count = 0;

        const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_COLUMNS);

        while (rows_count < max_block_size && db_table_num < total_tables)
        {
            std::cerr << "Hi!" << std::endl;

            const std::string database_name = (*databases)[db_table_num].get<std::string>();
            const std::string table_name = (*tables)[db_table_num].get<std::string>();
            ++db_table_num;

            ColumnsDescription columns;
            Names cols_required_for_partition_key;
            Names cols_required_for_sorting_key;
            Names cols_required_for_primary_key;
            Names cols_required_for_sampling;
            MergeTreeData::ColumnSizeByName column_sizes;

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

            std::cerr << "Hello!" << std::endl;

            for (auto i : columns_mask) {
                std::cerr << i;
            }
            std::cerr << std::endl;

            size_t position = 0;
            for (const auto & column : columns)
            {
                ++position;
                if (check_access_for_columns && !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name, column.name))
                    continue;

                std::cerr << columns_mask.size() << std::endl;

                size_t src_index = 0;
                size_t res_index = 0;

                // table_catolg
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);
                std::cerr << src_index << std::endl;

                // table_name
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);
                std::cerr << src_index << std::endl;

                // column_name
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.name);
                std::cerr << src_index << std::endl;

                // ordinal position
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(position);
                std::cerr << src_index << std::endl;

                // column_default
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insertDefault();
                std::cerr << src_index << std::endl;

                // data_type
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.type->getName());
                std::cerr << src_index << std::endl;

                // character_maximum_length
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insertDefault();
                std::cerr << src_index << std::endl;

                // character_octet_maximum_length
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insertDefault();
                std::cerr << src_index << std::endl;

                // numeric_precision & numeric_scale
                if (typeToNumberParams.count(column.type->getTypeId())) {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(typeToNumberParams[column.type->getTypeId()].first);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(typeToNumberParams[column.type->getTypeId()].second);
                } else {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
                }

                std::cerr << columns_mask.size() << " " << src_index << "\n";
                std::cerr.flush();
                // datetime_precision
                if (WhichDataType(column.type).isDate())
                {
                    if (columns_mask[src_index++]) 
                        res_columns[res_index++]->insert(86400u);
                } 
                else if (WhichDataType(column.type).isDateTime())
                {
                    if (columns_mask[src_index++]) 
                        res_columns[res_index++]->insert(1u);
                }
                else if (WhichDataType(column.type).isDateTime64()) {
                    if (columns_mask[src_index++]) {
                        //auto dateTime64 = std::dynamic_pointer_cast<
                    }
                }

                // character_set_name
                if (columns_mask[src_index++]) {
                    res_columns[res_index++]->insert("UTF-8");
                }

                // collation_name
                if (columns_mask[src_index++]) {
                    res_columns[res_index++]->insertDefault();
                }

                // default_kind & default_expression
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
                    // data_compressed_bytes & data_uncompressed_bytes & marks_bytes
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

                // comment
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.comment);

                {
                    auto find_in_vector = [&key = column.name](const Names& names)
                    {
                        return std::find(names.cbegin(), names.cend(), key) != names.end();
                    };

                    // is_in_partition_key
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_partition_key));

                    // is_in_sorting_key
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sorting_key));

                    // is_in_primary_key
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_primary_key));

                    // is_in_sampling_key
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sampling));
                }

                // compression_codec
                if (columns_mask[src_index++])
                {
                    if (column.codec)
                        res_columns[res_index++]->insert(queryToString(column.codec));
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


Pipe StorageSystemColumnsIS::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = metadata_snapshot->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    std::cerr << sample_block.columns() << "\n";
    std::cerr.flush();

    for (auto i : names_set) {
        std::cerr << i << std::endl;
    }
    std::cerr << std::endl;
    std::cerr << std::endl;

    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        std::cerr << sample_block.getByPosition(i).name << "\n";
        std::cerr.flush();
        if (names_set.count(sample_block.getByPosition(i).name))
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
        if (context.hasSessionContext())
        {
            external_tables = context.getSessionContext().getExternalTables();
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

    pipes.emplace_back(std::make_shared<ColumnsSourceIS>(
            std::move(columns_mask), std::move(header), max_block_size,
            std::move(filtered_database_column), std::move(filtered_table_column),
            std::move(storages), context));

    return Pipe::unitePipes(std::move(pipes));
}

}
