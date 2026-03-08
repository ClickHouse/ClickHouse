#include <Storages/System/StorageSystemColumnsCache.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/MergeTree/ColumnsCache.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

namespace
{

class SystemColumnsCacheSource;

class ReadFromSystemColumnsCache final : public SourceStepWithFilter
{
public:
    explicit ReadFromSystemColumnsCache(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        const Block & header_,
        UInt64 max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(header_),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , max_block_size(max_block_size_)
    {
    }

    std::string getName() const override { return "ReadFromSystemColumnsCache"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto source = std::make_shared<SystemColumnsCacheSource>(
            getOutputHeader(),
            max_block_size,
            context);

        processors.emplace_back(std::static_pointer_cast<IProcessor>(source));
        pipeline.init(Pipe(std::static_pointer_cast<IProcessor>(std::move(source))));
    }

private:
    const UInt64 max_block_size;
};

class SystemColumnsCacheSource : public ISource, private WithContext
{
public:
    SystemColumnsCacheSource(
        SharedHeader header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "SystemColumnsCacheSource"; }

protected:
    Chunk generate() override
    {
        if (done)
            return {};

        done = true;

        auto columns_cache = getContext()->getColumnsCache();
        if (!columns_cache)
            return {};

        auto entries = columns_cache->getAllEntries();
        if (entries.empty())
            return {};

        MutableColumnPtr col_database = ColumnString::create();
        MutableColumnPtr col_table = ColumnString::create();
        MutableColumnPtr col_table_uuid = ColumnUUID::create();
        MutableColumnPtr col_part = ColumnString::create();
        MutableColumnPtr col_column = ColumnString::create();
        MutableColumnPtr col_row_begin = ColumnUInt64::create();
        MutableColumnPtr col_row_end = ColumnUInt64::create();
        MutableColumnPtr col_rows = ColumnUInt64::create();
        MutableColumnPtr col_bytes = ColumnUInt64::create();

        size_t num_rows = 0;
        for (const auto & [key, entry] : entries)
        {
            if (max_block_size && num_rows >= max_block_size)
                break;

            /// Look up database and table names from UUID
            String database_name;
            String table_name;
            auto [database, table] = DatabaseCatalog::instance().tryGetByUUID(key.table_uuid);
            if (database && table)
            {
                database_name = database->getDatabaseName();
                table_name = table->getStorageID().table_name;
            }

            col_database->insert(database_name);
            col_table->insert(table_name);
            col_table_uuid->insert(key.table_uuid);
            col_part->insert(key.part_name);
            col_column->insert(key.column_name);
            col_row_begin->insert(key.row_begin);
            col_row_end->insert(key.row_end);
            col_rows->insert(entry->rows);
            col_bytes->insert(entry->column->byteSize());

            ++num_rows;
        }

        Columns columns;
        columns.emplace_back(std::move(col_database));
        columns.emplace_back(std::move(col_table));
        columns.emplace_back(std::move(col_table_uuid));
        columns.emplace_back(std::move(col_part));
        columns.emplace_back(std::move(col_column));
        columns.emplace_back(std::move(col_row_begin));
        columns.emplace_back(std::move(col_row_end));
        columns.emplace_back(std::move(col_rows));
        columns.emplace_back(std::move(col_bytes));

        return Chunk(std::move(columns), num_rows);
    }

private:
    const UInt64 max_block_size;
    bool done = false;
};

}

StorageSystemColumnsCache::StorageSystemColumnsCache(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    ColumnsDescription columns{
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table", std::make_shared<DataTypeString>(), "Table name"},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "Table UUID"},
        {"part", std::make_shared<DataTypeString>(), "Data part name"},
        {"column", std::make_shared<DataTypeString>(), "Column name"},
        {"row_begin", std::make_shared<DataTypeUInt64>(), "Starting row index (inclusive)"},
        {"row_end", std::make_shared<DataTypeUInt64>(), "Ending row index (exclusive)"},
        {"rows", std::make_shared<DataTypeUInt64>(), "Number of rows in cached block"},
        {"bytes", std::make_shared<DataTypeUInt64>(), "Size of cached column data in bytes"}
    };

    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemColumnsCache::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */)
{
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->metadata->getSampleBlock();
    auto read_step = std::make_unique<ReadFromSystemColumnsCache>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);

    query_plan.addStep(std::move(read_step));
}

}
