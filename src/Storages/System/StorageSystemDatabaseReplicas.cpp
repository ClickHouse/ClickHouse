#include <future>
#include <memory>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Databases/ReplicatedDatabaseStatus.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDatabaseReplicas.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


namespace CurrentMetrics
{
    extern const Metric SystemDatabaseReplicasThreads;
    extern const Metric SystemDatabaseReplicasThreadsActive;
    extern const Metric SystemDatabaseReplicasThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

class StatusRequestsPool
{
public:
    struct RequestInfo
    {
        UInt64 request_id = 0;
        std::shared_future<ReplicatedDatabaseStatus> future;
    };

private:
    ThreadPool thread_pool;

    std::mutex mutex;
    std::unordered_map<DatabasePtr, RequestInfo> current_requests TSA_GUARDED_BY(mutex);
    std::deque<std::tuple<UInt64, DatabasePtr, std::shared_ptr<std::promise<ReplicatedDatabaseStatus>>, bool>> requests_to_schedule TSA_GUARDED_BY(mutex);
    UInt64 request_id TSA_GUARDED_BY(mutex) = 0;

    LoggerPtr log;

public:
    explicit StatusRequestsPool(size_t max_threads)
        : thread_pool(CurrentMetrics::SystemDatabaseReplicasThreads, CurrentMetrics::SystemDatabaseReplicasThreadsActive, CurrentMetrics::SystemDatabaseReplicasThreadsScheduled, max_threads)
        , log(getLogger("StatusRequestsPool"))
    {}

    ~StatusRequestsPool()
    {
        thread_pool.wait();
        for (auto & request : requests_to_schedule)
            std::get<2>(request)->set_exception(std::make_exception_ptr(
                DB::Exception(ErrorCodes::QUERY_WAS_CANCELLED, "StatusRequestsPool is destroyed")));
    }

    RequestInfo addRequest(DatabasePtr database, const bool with_zk_fields)
    {
        std::shared_ptr<std::promise<ReplicatedDatabaseStatus>> promise;
        std::shared_future<ReplicatedDatabaseStatus> future;
        UInt64 this_request_id = 0;

        {
            std::lock_guard lock(mutex);

            auto existing_request = current_requests.find(database);
            if (existing_request != current_requests.end())
            {
                LOG_DEBUG(log, "Attaching to existing request for database {}", database->getDatabaseName());
                return existing_request->second;
            }

            this_request_id = request_id;
            ++request_id;

            promise = std::make_shared<std::promise<ReplicatedDatabaseStatus>>();
            future = promise->get_future().share();

            current_requests[database] = { .request_id = this_request_id, .future = future };

            LOG_DEBUG(log, "Making new request for database {}", database->getDatabaseName());

            requests_to_schedule.emplace_back(this_request_id, database, promise, with_zk_fields);
        }

        return {this_request_id, future};
    }

    void scheduleRequests(UInt64 max_request_id, QueryStatusPtr query_status)
    {
        while (true)
        {
            if (query_status)
                query_status->checkTimeLimit();

            std::tuple<UInt64, DatabasePtr, std::shared_ptr<std::promise<ReplicatedDatabaseStatus>>, bool> req;
            {
                std::lock_guard lock(mutex);
                if (requests_to_schedule.empty())
                    break;

                req = requests_to_schedule.front();

                if (std::get<0>(req) > max_request_id)
                    break;

                requests_to_schedule.pop_front();
            }

            auto get_status_task = [this, req, thread_group = CurrentThread::getGroup()]() mutable
            {
                ThreadGroupSwitcher switcher(thread_group, "SysDbReplicas");

                auto & [_, database, promise, with_zk_fields] = req;
                try
                {
                    ReplicatedDatabaseStatus status;

                    DatabaseReplicated * replicated_database = dynamic_cast<DatabaseReplicated *>(database.get());
                    replicated_database->getStatus(status, with_zk_fields);

                    promise->set_value(std::move(status));
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error getting status for database " + database->getDatabaseName());
                    promise->set_exception(std::current_exception());
                }

                completeRequest(database);
            };

            auto & [_, database, promise, with_zk_fields] = req;

            try
            {
                thread_pool.scheduleOrThrowOnError(std::move(get_status_task));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Error scheduling get status task for database " + database->getDatabaseName());
                promise->set_exception(std::current_exception());
                completeRequest(database);
            }
        }
    }

private:
    void completeRequest(DatabasePtr database)
    {
        std::lock_guard lock(mutex);
        current_requests.erase(database);
    }
};

} // anonymous namespace


class StorageSystemDatabaseReplicasImpl
{
public:
    explicit StorageSystemDatabaseReplicasImpl(size_t max_threads)
        : requests_without_zk_fields(max_threads)
        , requests_with_zk_fields(max_threads)
    {}

    StatusRequestsPool requests_without_zk_fields;
    StatusRequestsPool requests_with_zk_fields;
};


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
        std::vector<std::shared_future<ReplicatedDatabaseStatus>>&& futures_
        )
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
    std::vector<std::shared_future<ReplicatedDatabaseStatus>> futures;
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

        const ReplicatedDatabaseStatus * status;
        try
        {
            status = &futures[index].get();
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::ABORTED)
            {
                tryLogCurrentException(getLogger("table logger"), "Received the ABORTED error while trying to get the status of a database, this is likely because it has been shut down");
                continue;
            }
            throw;
        }

        size_t col_num = 0;

        res_columns[col_num++]->insert((*col_database)[index]);

        res_columns[col_num++]->insert(status->is_readonly);
        res_columns[col_num++]->insert(status->max_log_ptr);

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
        std::shared_ptr<StorageSystemDatabaseReplicasImpl> impl_,
        bool with_zk_fields_
    )
        : SourceStepWithFilter(
            std::make_shared<const Block>(std::move(sample_block)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , replicated_databases(std::move(replicated_databases_))
        , max_block_size(max_block_size_)
        , impl(impl_)
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
    std::shared_ptr<StorageSystemDatabaseReplicasImpl> impl;
    const bool with_zk_fields;
};

void ReadFromSystemDatabaseReplicas::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter);
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
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
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

    std::vector<std::shared_future<ReplicatedDatabaseStatus>> futures;

    const size_t tables_size = col_database->size();
    futures.reserve(tables_size);

    StatusRequestsPool & get_status_requests = with_zk_fields ? impl->requests_with_zk_fields : impl->requests_without_zk_fields;
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
    , impl(std::make_shared<StorageSystemDatabaseReplicasImpl>(8))
{

    ColumnsDescription description = {
        { "database", std::make_shared<DataTypeString>(),   "Database name."},
        { "is_readonly", std::make_shared<DataTypeUInt8>(),   "is_readonly"},
        { "max_log_ptr", std::make_shared<DataTypeInt32>(),   "max_log_ptr"}
    };

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
    for (const auto & [db_name, db_data] : DatabaseCatalog::instance().getDatabases())
    {
        if (!dynamic_cast<const DatabaseReplicated *>(db_data.get()))
            continue;

        const bool check_access_for_db = !access->isGranted(AccessType::SHOW_DATABASES, db_name);
        if (need_to_check_access_for_databases && !check_access_for_db)
            continue;

        replicated_databases[db_name] = db_data;
    }

    bool with_zk_fields = false;
    for (const auto & column_name : column_names)
    {
        if (column_name == "max_log_ptr")
        {
            with_zk_fields = true;
            break;
        }
    }

    auto header = storage_snapshot->metadata->getSampleBlock();
    auto reading = std::make_unique<ReadFromSystemDatabaseReplicas>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(replicated_databases), max_block_size, impl, with_zk_fields);

    query_plan.addStep(std::move(reading));
}

}
