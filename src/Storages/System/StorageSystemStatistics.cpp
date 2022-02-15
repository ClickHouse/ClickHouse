#include <Storages/System/StorageSystemStatistics.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/SourceWithProgress.h>
#include "DataTypes/DataTypeArray.h"


namespace DB
{
StorageSystemStatistics::StorageSystemStatistics(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            { "database", std::make_shared<DataTypeString>() },
            { "table", std::make_shared<DataTypeString>() },
            { "name", std::make_shared<DataTypeString>() },
            { "columns", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
            { "type", std::make_shared<DataTypeString>() },
            { "data_in_ram_bytes", std::make_shared<DataTypeUInt64>() },
            { "data_compressed_bytes", std::make_shared<DataTypeUInt64>() },
            { "data_uncompressed_bytes", std::make_shared<DataTypeUInt64>() }
        }));
    setInMemoryMetadata(storage_metadata);
}

class StatisticsSource : public SourceWithProgress
{
public:
    StatisticsSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ContextPtr context_)
        : SourceWithProgress(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
        , database_idx(0)
    {}

    String getName() const override { return "DataSkippingIndices"; }

protected:
    Chunk generate() override
    {
        if (database_idx >= databases->size())
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (database)
                    break;
                ++database_idx;
            }

            if (database_idx >= databases->size())
                break;

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context);

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                const auto table = tables_it->table();
                if (!table)
                    continue;
                StorageMetadataPtr metadata_snapshot = table->getInMemoryMetadataPtr();
                if (!metadata_snapshot)
                    continue;
                const auto statistics = metadata_snapshot->getStatistics();

                //auto statistics_sizes = table->getStatisticsSizes();
                for (const auto & statistic : statistics)
                {
                    ++rows_count;

                    size_t src_stat = 0;
                    size_t res_stat = 0;

                    // 'database' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(database_name);
                    // 'table' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(table_name);
                    // 'name' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(statistic.name);
                    // 'columns' column
                    if (column_mask[src_stat++])
                    {
                        Array array;
                        for (const auto & column : statistic.column_names) {
                            array.push_back(column);
                        }
                        res_columns[res_stat++]->insert(array);
                    }
                    // 'type' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(statistic.type);

                    //auto & secondary_index_size = secondary_index_sizes[index.name];

                    /// 'in ram bytes' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(0);

                    // 'compressed bytes' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(0);

                    // 'uncompressed bytes' column
                    if (column_mask[src_stat++])
                        res_columns[res_stat++]->insert(0);
                }
            }
        }
        return Chunk(std::move(res_columns), rows_count);
    }

private:
    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    ContextPtr context;
    size_t database_idx;
    DatabasePtr database;
    std::string database_name;
    DatabaseTablesIteratorPtr tables_it;
};

Pipe StorageSystemStatistics::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    unsigned int /* num_streams */)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = metadata_snapshot->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        /// Lazy database can contain only very primitive tables,
        /// it cannot contain tables with statistics.
        /// Skip it to avoid unnecessary tables loading in the Lazy database.
        if (database->getEngineName() != "Lazy")
            column->insert(database_name);
    }

    /// Condition on "database" in a query acts like an index.
    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    return Pipe(std::make_shared<StatisticsSource>(
        std::move(columns_mask), std::move(header), max_block_size, std::move(filtered_databases), context));
}

}
