#include <Storages/System/StorageSystemDatabaseReplicas.h>

#include <future>
#include <memory>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StatusRequestsPool.h>
#include <Storages/VirtualColumnUtils.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
}

using TFuture = typename StorageSystemDatabaseReplicas::TPools::StatusPool::TFuture;
using TStatus = typename StorageSystemDatabaseReplicas::TPools::StatusPool::TStatus;

namespace
{

class SystemDatabaseReplicasSource : public ISource
{
public:
    SystemDatabaseReplicasSource(
        SharedHeader header_,
        size_t max_block_size_,
        ColumnPtr col_database_,
        ContextPtr context_,
        std::vector<TFuture> && futures_)
        : ISource(header_)
        , max_databases(col_database_->size())
        , max_block_size(max_block_size_)
        , col_database(std::move(col_database_))
        , context(std::move(context_))
        , futures(std::move(futures_))
    {
    }

    String getName() const override { return "SystemDatabaseReplicas"; }

protected:
    Chunk generate() override;

private:
    const size_t max_databases;
    const size_t max_block_size;
    ColumnPtr col_database;
    ContextPtr context;
    std::vector<TFuture> futures;
    size_t index = 0;
};

Chunk SystemDatabaseReplicasSource::generate()
{
    if (index == futures.size())
        return {};

    QueryStatusPtr query_status = context ? context->getProcessListElement() : nullptr;

    MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

    bool rows_added = false;
    for (; index < futures.size(); ++index)
    {
        if (query_status)
            query_status->checkTimeLimit();

        if (rows_added)
        {
            if (futures[index].wait_for(std::chrono::seconds(0)) != std::future_status::ready)
                break;

            if (max_block_size != 0)
            {
                size_t total_size = 0;
                for (const auto & column : res_columns)
                    total_size += column->byteSize();
                if (total_size >= max_block_size)
                    break;
            }
        }

        const TStatus * status;
        try
        {
            status = &futures[index].get();
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::ABORTED)
            {
                tryLogCurrentException(
                    getLogger("table logger"),
                    "Received the ABORTED error while trying to get the status of a database, this is likely because it has been shut "
                    "down");
                continue;
            }
            throw;
        }

        size_t col_num = 0;

        res_columns[col_num++]->insert((*col_database)[index]);

        res_columns[col_num++]->insert(status->is_readonly);
        res_columns[col_num++]->insert(status->max_log_ptr);
        res_columns[col_num++]->insert(status->replica_name);
        res_columns[col_num++]->insert(status->replica_path);
        res_columns[col_num++]->insert(status->zookeeper_path);
        res_columns[col_num++]->insert(status->shard_name);
        res_columns[col_num++]->insert(status->log_ptr);
        res_columns[col_num++]->insert(status->total_replicas);
        res_columns[col_num++]->insert(status->zookeeper_exception);
        res_columns[col_num++]->insert(status->is_session_expired);

        rows_added = true;
    }

    UInt64 num_rows = res_columns.at(0)->size();
    return Chunk(std::move(res_columns), num_rows);
}

class ReadFromSystemDatabaseReplicas : public SourceStepWithFilter
{
public:
    ReadFromSystemDatabaseReplicas(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::map<String, DatabasePtr> replicated_databases_,
        size_t max_block_size_,
        std::shared_ptr<StorageSystemDatabaseReplicas::TPools> pools_,
        bool with_zk_fields_)
        : SourceStepWithFilter(
              std::make_shared<const Block>(std::move(sample_block)), column_names_, query_info_, storage_snapshot_, context_)
        , replicated_databases(std::move(replicated_databases_))
        , max_block_size(max_block_size_)
        , pools(pools_)
        , with_zk_fields(with_zk_fields_)
    {
    }

    std::string getName() const override { return "ReadFromSystemDatabaseReplicas"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::map<String, DatabasePtr> replicated_databases;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
    std::shared_ptr<StorageSystemDatabaseReplicas::TPools> pools;
    const bool with_zk_fields;
};

void ReadFromSystemDatabaseReplicas::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "database"},
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
}

void ReadFromSystemDatabaseReplicas::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto header = getOutputHeader();

    MutableColumnPtr col_database_mut = ColumnString::create();

    for (auto & [db_name, database] : replicated_databases)
    {
        col_database_mut->insert(db_name);
    }

    ColumnPtr col_database = std::move(col_database_mut);

    {
        Block filtered_block{
            {col_database, std::make_shared<DataTypeString>(), "database"},
        };

        if (virtual_columns_filter)
            VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, filtered_block);

        if (!filtered_block.rows())
        {
            auto source = std::make_shared<NullSource>(std::move(header));
            pipeline.init(Pipe(std::move(source)));
            return;
        }

        col_database = filtered_block.getByName("database").column;
    }

    std::vector<TFuture> futures;

    const size_t tables_size = col_database->size();
    futures.reserve(tables_size);

    auto & get_status_requests = with_zk_fields ? pools->requests_with_zk_fields : pools->requests_without_zk_fields;
    QueryStatusPtr query_status = context ? context->getProcessListElement() : nullptr;
    UInt64 max_request_id = 0;

    for (size_t i = 0; i < tables_size; ++i)
    {
        if (query_status)
            query_status->checkTimeLimit();

        auto & database = replicated_databases[(*col_database)[i].safeGet<String>()];

        auto [request_id, future] = get_status_requests.addRequest(database, with_zk_fields);
        futures.emplace_back(future);
        max_request_id = std::max(max_request_id, request_id);
    }

    get_status_requests.scheduleRequests(max_request_id, query_status);

    pipeline.init(Pipe(std::make_shared<SystemDatabaseReplicasSource>(header, max_block_size, col_database, context, std::move(futures))));
}

} // anonymous namespace

StorageSystemDatabaseReplicas::StorageSystemDatabaseReplicas(const StorageID & table_id_)
    : IStorage(table_id_)
    , pools(std::make_shared<TPools>(DEFAULT_THREAD_COUNT))
{
    ColumnsDescription description
        = {{"database", std::make_shared<DataTypeString>(), "The name of the Replicated database is in."},
           {"is_readonly", std::make_shared<DataTypeUInt8>(), "Whether the database replica is in read-only mode."},
           {"max_log_ptr", std::make_shared<DataTypeInt32>(), "Maximum entry number in the log of general activity."},
           {"replica_name", std::make_shared<DataTypeString>(), "Replica name in ClickHouse Keeper."},
           {"replica_path", std::make_shared<DataTypeString>(), "Path to replica data in ClickHouse Keeper."},
           {"zookeeper_path", std::make_shared<DataTypeString>(), "Path to database data in ClickHouse Keeper."},
           {"shard_name", std::make_shared<DataTypeString>(), "The name of the shard in the cluster."},
           {"log_ptr", std::make_shared<DataTypeInt32>(), "Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one."},
           {"total_replicas", std::make_shared<DataTypeUInt32>(), "The total number of known replicas of this database."},
           {"zookeeper_exception", std::make_shared<DataTypeString>(), "The last exception message, got if the error happened when fetching the info from ClickHouse Keeper."},
           {"is_session_expired", std::make_shared<DataTypeUInt8>(), "The session with ClickHouse Keeper has expired. Basically the same as `is_readonly`."}};

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(description);
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemDatabaseReplicas::read(
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

    const auto access = context->getAccess();
    const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    std::map<String, DatabasePtr> replicated_databases;
    for (const auto & [db_name, db_data] : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (!dynamic_cast<const DatabaseReplicated *>(db_data.get()))
            continue;

        if (need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, db_name))
            continue;

        replicated_databases[db_name] = db_data;
    }

    bool with_zk_fields = false;
    for (const auto & column_name : column_names)
    {
        if (column_name == "max_log_ptr" || column_name == "log_ptr" || column_name == "total_replicas")
        {
            with_zk_fields = true;
            break;
        }
    }

    auto header = storage_snapshot->metadata->getSampleBlock();
    auto reading = std::make_unique<ReadFromSystemDatabaseReplicas>(
        column_names,
        query_info,
        storage_snapshot,
        std::move(context),
        std::move(header),
        std::move(replicated_databases),
        max_block_size,
        pools,
        with_zk_fields);

    query_plan.addStep(std::move(reading));
}

}
