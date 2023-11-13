#include <future>
#include <memory>
#include <mutex>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/MergeTree/ReplicatedTableStatus.h>
#include <Interpreters/ProcessList.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/typeid_cast.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>


namespace CurrentMetrics
{
    extern const Metric SystemReplicasThreads;
    extern const Metric SystemReplicasThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

/// Allows to "deduplicate" getStatus() requests for the same table: if a request for a table is already in progress
/// then the new request will return the same future as the previous one.
class StatusRequestsPool
{
public:
    struct RequestInfo
    {
        UInt64 request_id = 0;
        std::shared_future<ReplicatedTableStatus> future;
    };

private:
    ThreadPool thread_pool;

    std::mutex mutex;
    /// All requests from the queries that are currently being executed.
    std::unordered_map<StoragePtr, RequestInfo> current_requests TSA_GUARDED_BY(mutex);
    /// Requests that were added by currently executing queries but have not been scheduled yet.
    std::deque<std::tuple<UInt64, StoragePtr, std::shared_ptr<std::promise<ReplicatedTableStatus>>, bool>> requests_to_schedule TSA_GUARDED_BY(mutex);
    /// Used to assign unique incremental ids to requests.
    UInt64 request_id TSA_GUARDED_BY(mutex) = 0;

    Poco::Logger * log;

public:
    explicit StatusRequestsPool(size_t max_threads)
        : thread_pool(CurrentMetrics::SystemReplicasThreads, CurrentMetrics::SystemReplicasThreadsActive, max_threads)
        , log(&Poco::Logger::get("StatusRequestsPool"))
    {}

    ~StatusRequestsPool()
    {
        thread_pool.wait();
        /// Cancel unscheduled requests
        for (auto & request : requests_to_schedule)
            std::get<2>(request)->set_exception(std::make_exception_ptr(
                DB::Exception(ErrorCodes::QUERY_WAS_CANCELLED, "StatusRequestsPool is destroyed")));
    }

    /// Make a new request or "attach" to an existing one.
    RequestInfo addRequest(StoragePtr storage, bool with_zk_fields)
    {
        std::shared_ptr<std::promise<ReplicatedTableStatus>> promise;
        std::shared_future<ReplicatedTableStatus> future;
        UInt64 this_request_id = 0;

        {
            std::lock_guard lock(mutex);
            auto existing_request = current_requests.find(storage);
            if (existing_request != current_requests.end())
            {
                LOG_TEST(log, "Attaching to existing request for table {}", storage->getStorageID().getNameForLogs());
                return existing_request->second;
            }

            this_request_id = request_id;
            ++request_id;

            promise = std::make_shared<std::promise<ReplicatedTableStatus>>();
            future = promise->get_future().share();

            current_requests[storage] = { .request_id = this_request_id, .future = future };

            LOG_TEST(log, "Making new request for table {}", storage->getStorageID().getNameForLogs());

            requests_to_schedule.emplace_back(this_request_id, storage, promise, with_zk_fields);
        }

        return {this_request_id, future};
    }

    /// Schedule requests (if any) that are needed for the current query. This is determined by the maximum request id
    /// returned by addRequest.
    void scheduleRequests(UInt64 max_request_id, QueryStatusPtr query_status)
    {
        while (true)
        {
            if (query_status)
                query_status->checkTimeLimit();

            /// Try to pick up a request to schedule
            std::tuple<UInt64, StoragePtr, std::shared_ptr<std::promise<ReplicatedTableStatus>>, bool> req;
            {
                std::lock_guard lock(mutex);
                if (requests_to_schedule.empty())
                    break;

                req = requests_to_schedule.front();

                /// Check if all requests for the current query have been scheduled
                if (std::get<0>(req) > max_request_id)
                    break;

                requests_to_schedule.pop_front();
            }

            auto & [_, storage, promise, with_zk_fields] = req;

            auto get_status_task = [this, storage, with_zk_fields, promise] () mutable
            {
                try
                {
                    ReplicatedTableStatus status;
                    if (auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(storage.get()))
                    {
                        replicated_table->getStatus(status, with_zk_fields);
                    }
                    promise->set_value(std::move(status));
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error getting status for table " + storage->getStorageID().getNameForLogs());
                    promise->set_exception(std::current_exception());
                }

                completeRequest(storage);
            };

            try
            {
                thread_pool.scheduleOrThrowOnError(std::move(get_status_task));
            }
            catch (...)
            {
                tryLogCurrentException(log, "Error scheduling get status task for table " + storage->getStorageID().getNameForLogs());
                promise->set_exception(std::current_exception());
                completeRequest(storage);
            }
        }
    }

private:
    void completeRequest(StoragePtr storage)
    {
        std::lock_guard lock(mutex);
        current_requests.erase(storage);
    }
};


class StorageSystemReplicasImpl
{
public:
    explicit StorageSystemReplicasImpl(size_t max_threads)
        : requests_without_zk_fields(max_threads)
        , requests_with_zk_fields(max_threads)
    {}

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context);

private:
    StatusRequestsPool requests_without_zk_fields;
    StatusRequestsPool requests_with_zk_fields;
};


StorageSystemReplicas::StorageSystemReplicas(const StorageID & table_id_)
    : IStorage(table_id_)
    , impl(std::make_unique<StorageSystemReplicasImpl>(128))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "database",                             std::make_shared<DataTypeString>()   },
        { "table",                                std::make_shared<DataTypeString>()   },
        { "engine",                               std::make_shared<DataTypeString>()   },
        { "is_leader",                            std::make_shared<DataTypeUInt8>()    },
        { "can_become_leader",                    std::make_shared<DataTypeUInt8>()    },
        { "is_readonly",                          std::make_shared<DataTypeUInt8>()    },
        { "is_session_expired",                   std::make_shared<DataTypeUInt8>()    },
        { "future_parts",                         std::make_shared<DataTypeUInt32>()   },
        { "parts_to_check",                       std::make_shared<DataTypeUInt32>()   },
        { "zookeeper_name",                       std::make_shared<DataTypeString>()   },
        { "zookeeper_path",                       std::make_shared<DataTypeString>()   },
        { "replica_name",                         std::make_shared<DataTypeString>()   },
        { "replica_path",                         std::make_shared<DataTypeString>()   },
        { "columns_version",                      std::make_shared<DataTypeInt32>()    },
        { "queue_size",                           std::make_shared<DataTypeUInt32>()   },
        { "inserts_in_queue",                     std::make_shared<DataTypeUInt32>()   },
        { "merges_in_queue",                      std::make_shared<DataTypeUInt32>()   },
        { "part_mutations_in_queue",              std::make_shared<DataTypeUInt32>()   },
        { "queue_oldest_time",                    std::make_shared<DataTypeDateTime>() },
        { "inserts_oldest_time",                  std::make_shared<DataTypeDateTime>() },
        { "merges_oldest_time",                   std::make_shared<DataTypeDateTime>() },
        { "part_mutations_oldest_time",           std::make_shared<DataTypeDateTime>() },
        { "oldest_part_to_get",                   std::make_shared<DataTypeString>()   },
        { "oldest_part_to_merge_to",              std::make_shared<DataTypeString>()   },
        { "oldest_part_to_mutate_to",             std::make_shared<DataTypeString>()   },
        { "log_max_index",                        std::make_shared<DataTypeUInt64>()   },
        { "log_pointer",                          std::make_shared<DataTypeUInt64>()   },
        { "last_queue_update",                    std::make_shared<DataTypeDateTime>() },
        { "absolute_delay",                       std::make_shared<DataTypeUInt64>()   },
        { "total_replicas",                       std::make_shared<DataTypeUInt8>()    },
        { "active_replicas",                      std::make_shared<DataTypeUInt8>()    },
        { "lost_part_count",                      std::make_shared<DataTypeUInt64>()   },
        { "last_queue_update_exception",          std::make_shared<DataTypeString>()   },
        { "zookeeper_exception",                  std::make_shared<DataTypeString>()   },
        { "replica_is_active",                    std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt8>()) }
    }));
    setInMemoryMetadata(storage_metadata);
}

StorageSystemReplicas::~StorageSystemReplicas() = default;


Pipe StorageSystemReplicas::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    return impl->read(column_names, storage_snapshot, query_info, context);
}

Pipe StorageSystemReplicasImpl::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context)
{
    storage_snapshot->check(column_names);

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// We collect a set of replicated tables.
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain replicated tables
        if (!db.second->canContainMergeTreeTables())
            continue;
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);
        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            replicated_tables[db.first][iterator->name()] = table;
        }
    }


    /// Do you need columns that require a ZooKeeper request to compute.
    bool with_zk_fields = false;
    for (const auto & column_name : column_names)
    {
        if (   column_name == "log_max_index"
            || column_name == "log_pointer"
            || column_name == "total_replicas"
            || column_name == "active_replicas"
            || column_name == "lost_part_count"
            || column_name == "zookeeper_exception"
            || column_name == "replica_is_active")
        {
            with_zk_fields = true;
            break;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    MutableColumnPtr col_engine_mut = ColumnString::create();

    for (auto & db : replicated_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
            col_engine_mut->insert(table.second->getName());
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);
    ColumnPtr col_engine = std::move(col_engine_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
            { col_engine, std::make_shared<DataTypeString>(), "engine" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return {};

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
        col_engine = filtered_block.getByName("engine").column;
    }

    MutableColumns res_columns = storage_snapshot->metadata->getSampleBlock().cloneEmptyColumns();

    size_t tables_size = col_database->size();

    /// Use separate queues for requests with and without ZooKeeper fields.
    StatusRequestsPool & get_status_requests = with_zk_fields ? requests_with_zk_fields : requests_without_zk_fields;

    QueryStatusPtr query_status = context ? context->getProcessListElement() : nullptr;

    std::vector<std::shared_future<ReplicatedTableStatus>> futures;
    futures.reserve(tables_size);
    UInt64 max_request_id = 0;
    for (size_t i = 0; i < tables_size; ++i)
    {
        if (query_status)
            query_status->checkTimeLimit();

        auto & storage = replicated_tables[(*col_database)[i].safeGet<const String &>()]
            [(*col_table)[i].safeGet<const String &>()];

        auto [request_id, future] = get_status_requests.addRequest(storage, with_zk_fields);
        futures.emplace_back(future);
        max_request_id = std::max(max_request_id, request_id);
    }
    /// Schedule requests up to the maximum request needed for the current query.
    /// If there are more requests, they will be scheduled by the query that needs them.
    get_status_requests.scheduleRequests(max_request_id, query_status);

    for (size_t i = 0; i < tables_size; ++i)
    {
        if (query_status)
            query_status->checkTimeLimit();

        const auto & status = futures[i].get();
        size_t col_num = 3;
        res_columns[col_num++]->insert(status.is_leader);
        res_columns[col_num++]->insert(status.can_become_leader);
        res_columns[col_num++]->insert(status.is_readonly);
        res_columns[col_num++]->insert(status.is_session_expired);
        res_columns[col_num++]->insert(status.queue.future_parts);
        res_columns[col_num++]->insert(status.parts_to_check);
        res_columns[col_num++]->insert(status.zookeeper_name);
        res_columns[col_num++]->insert(status.zookeeper_path);
        res_columns[col_num++]->insert(status.replica_name);
        res_columns[col_num++]->insert(status.replica_path);
        res_columns[col_num++]->insert(status.columns_version);
        res_columns[col_num++]->insert(status.queue.queue_size);
        res_columns[col_num++]->insert(status.queue.inserts_in_queue);
        res_columns[col_num++]->insert(status.queue.merges_in_queue);
        res_columns[col_num++]->insert(status.queue.part_mutations_in_queue);
        res_columns[col_num++]->insert(status.queue.queue_oldest_time);
        res_columns[col_num++]->insert(status.queue.inserts_oldest_time);
        res_columns[col_num++]->insert(status.queue.merges_oldest_time);
        res_columns[col_num++]->insert(status.queue.part_mutations_oldest_time);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_get);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_merge_to);
        res_columns[col_num++]->insert(status.queue.oldest_part_to_mutate_to);
        res_columns[col_num++]->insert(status.log_max_index);
        res_columns[col_num++]->insert(status.log_pointer);
        res_columns[col_num++]->insert(status.queue.last_queue_update);
        res_columns[col_num++]->insert(status.absolute_delay);
        res_columns[col_num++]->insert(status.total_replicas);
        res_columns[col_num++]->insert(status.active_replicas);
        res_columns[col_num++]->insert(status.lost_part_count);
        res_columns[col_num++]->insert(status.last_queue_update_exception);
        res_columns[col_num++]->insert(status.zookeeper_exception);

        Map replica_is_active_values;
        for (const auto & [name, is_active] : status.replica_is_active)
        {
            Tuple is_replica_active_value;
            is_replica_active_value.emplace_back(name);
            is_replica_active_value.emplace_back(is_active);

            replica_is_active_values.emplace_back(std::move(is_replica_active_value));
        }

        res_columns[col_num++]->insert(std::move(replica_is_active_values));
    }

    Columns fin_columns;
    fin_columns.reserve(res_columns.size());

    for (auto & col : res_columns)
        fin_columns.emplace_back(std::move(col));

    fin_columns[0] = std::move(col_database);
    fin_columns[1] = std::move(col_table);
    fin_columns[2] = std::move(col_engine);

    UInt64 num_rows = fin_columns.at(0)->size();
    Chunk chunk(std::move(fin_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}


}
