#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDeltaLakeHistory.h>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#endif

/// Delta Lake timestamps are stored in milliseconds
static constexpr auto TIME_SCALE = 3;

namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

namespace
{

/// Streaming source that iterates Delta Lake tables and produces history records in chunks.
class SystemDeltaLakeHistorySource : public ISource, private WithContext
{
public:
    SystemDeltaLakeHistorySource(
        SharedHeader header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , max_block_size(max_block_size_)
    {
#if USE_PARQUET
        context_copy = Context::createCopy(context_);
        access = context_copy->getAccess();
        if (access->isGranted(AccessType::SHOW_TABLES))
            databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        db_it = databases.begin();
#endif
    }

    String getName() const override { return "SystemDeltaLakeHistorySource"; }

protected:
    Chunk generate() override
    {
#if USE_PARQUET
        auto header = getPort().getHeader();
        MutableColumns res_columns = header.cloneEmptyColumns();

        size_t num_rows = 0;

        while (true)
        {
            /// If we have buffered history records from the current table, emit them
            while (current_history_idx < current_history.size())
            {
                if (num_rows && max_block_size && num_rows >= max_block_size)
                    goto done;

                auto & record = current_history[current_history_idx++];

                size_t col = 0;
                res_columns[col++]->insert(current_database);
                res_columns[col++]->insert(current_table);
                res_columns[col++]->insert(record.version);

                if (record.timestamp.has_value())
                    res_columns[col++]->insert(record.timestamp.value());
                else
                    res_columns[col++]->insertDefault();

                res_columns[col++]->insert(record.operation);

                Map params_map;
                for (const auto & [key, value] : record.operation_parameters)
                {
                    Tuple tuple;
                    tuple.push_back(key);
                    tuple.push_back(value);
                    params_map.push_back(std::move(tuple));
                }
                res_columns[col++]->insert(params_map);

                res_columns[col++]->insert(record.is_latest_version ? 1 : 0);
                ++num_rows;
            }

            /// Need to fetch history for the next table
            if (!advanceToNextTable())
                break;
        }

done:
        if (!num_rows)
            return {};

        return Chunk(std::move(res_columns), num_rows);
#else
        return {};
#endif
    }

private:
#if USE_PARQUET
    /// Advance the database/table iterator to the next Delta Lake table and load its history.
    /// Returns true if a table with history was found, false if iteration is exhausted.
    bool advanceToNextTable()
    {
        while (db_it != databases.end())
        {
            if (!table_it)
                table_it = db_it->second->getTablesIterator(context_copy, {}, true);

            while (table_it->isValid())
            {
                StoragePtr storage = table_it->table();
                String db_name = table_it->databaseName();
                String tbl_name = table_it->name();
                table_it->next();

                if (!access->isGranted(AccessType::SHOW_TABLES, db_name, tbl_name))
                    continue;

                TableLockHolder lock = storage->tryLockForShare(
                    context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    continue;

                auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get());
                if (!object_storage_table)
                    continue;

                try
                {
                    IDataLakeMetadata * data_lake_metadata = object_storage_table->getExternalMetadata(context_copy);
                    DeltaLakeHistory history;

#if USE_DELTA_KERNEL_RS
                    if (auto * delta_kernel_metadata = dynamic_cast<DeltaLakeMetadataDeltaKernel *>(data_lake_metadata))
                        history = delta_kernel_metadata->getHistory(context_copy);
                    else if (auto * delta_metadata = dynamic_cast<DeltaLakeMetadata *>(data_lake_metadata))
                        history = delta_metadata->getHistory(context_copy);
#else
                    if (auto * delta_metadata = dynamic_cast<DeltaLakeMetadata *>(data_lake_metadata))
                        history = delta_metadata->getHistory(context_copy);
#endif

                    if (history.empty())
                        continue;

                    current_database = std::move(db_name);
                    current_table = std::move(tbl_name);
                    current_history = std::move(history);
                    current_history_idx = 0;
                    return true;
                }
                catch (...)
                {
                    /// Broken external Delta tables are expected during broad system-table scans.
                    /// Keep this at debug level to avoid polluting stderr in stateless tests.
                    LOG_DEBUG(
                        getLogger("SystemDeltaLakeHistory"),
                        "Ignoring broken table {}: {}",
                        object_storage_table->getStorageID().getFullTableName(),
                        getCurrentExceptionMessage(false));
                }
            }

            table_it = nullptr;
            ++db_it;
        }

        return false;
    }
#endif

    [[maybe_unused]] const UInt64 max_block_size;

#if USE_PARQUET
    ContextMutablePtr context_copy;
    std::shared_ptr<const ContextAccessWrapper> access;
    Databases databases;
    Databases::iterator db_it;
    DatabaseTablesIteratorPtr table_it;

    String current_database;
    String current_table;
    DeltaLakeHistory current_history;
    size_t current_history_idx = 0;
#endif
};

class ReadFromSystemDeltaLakeHistory final : public SourceStepWithFilter
{
public:
    ReadFromSystemDeltaLakeHistory(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        UInt64 max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(header),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage_limits(query_info.storage_limits)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "ReadFromSystemDeltaLakeHistory"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto source = std::make_shared<SystemDeltaLakeHistorySource>(getOutputHeader(), max_block_size, context);
        source->setStorageLimits(storage_limits);
        processors.push_back(source);
        pipeline.init(Pipe(std::move(source)));
    }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
};

}

StorageSystemDeltaLakeHistory::StorageSystemDeltaLakeHistory(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"version", std::make_shared<DataTypeUInt64>(), "Version number of the Delta Lake table."},
        {"timestamp",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)),
         "Timestamp when this version was committed."},
        {"operation", std::make_shared<DataTypeString>(), "Operation type (e.g., WRITE, DELETE, MERGE)."},
        {"operation_parameters",
         std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
         "Parameters of the operation."},
        {"is_latest_version", std::make_shared<DataTypeUInt8>(), "Flag indicating if this is the latest version."}});
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemDeltaLakeHistory::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto read_step = std::make_unique<ReadFromSystemDeltaLakeHistory>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
}

}
