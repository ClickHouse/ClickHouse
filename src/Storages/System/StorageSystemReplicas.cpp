#include <future>
#include <memory>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/StatusRequestsPool.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadPool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

using TFuture = typename StorageSystemReplicas::TPools::StatusPool::TFuture;
using TStatus = typename StorageSystemReplicas::TPools::StatusPool::TStatus;

StorageSystemReplicas::StorageSystemReplicas(const StorageID & table_id_)
    : IStorage(table_id_)
    , pools(std::make_shared<TPools>(128))
{

    ColumnsDescription description = {
        { "database",                             std::make_shared<DataTypeString>(),   "Database name."},
        { "table",                                std::make_shared<DataTypeString>(),   "Table name."},
        { "uuid",                                 std::make_shared<DataTypeUUID>(),     "Table UUID."},
        { "engine",                               std::make_shared<DataTypeString>(),   "Table engine name."},
        { "is_leader",                            std::make_shared<DataTypeUInt8>(),    "Whether the replica is the leader. Multiple replicas can be leaders at the same time. "
                                                                                          "A replica can be prevented from becoming a leader using the merge_tree setting replicated_can_become_leader. "
                                                                                          "The leaders are responsible for scheduling background merges. "
                                                                                          "Note that writes can be performed to any replica that is available and has a session in ZK, regardless of whether it is a leader."},
        { "can_become_leader",                    std::make_shared<DataTypeUInt8>(),    "Whether the replica can be a leader."},
        { "is_readonly",                          std::make_shared<DataTypeUInt8>(),    "Whether the replica is in read-only mode. This mode is turned on if the config does not have sections with ClickHouse Keeper, "
                                                                                          "if an unknown error occurred when reinitializing sessions in ClickHouse Keeper, and during session reinitialization in ClickHouse Keeper."},
        { "readonly_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "The timestamp when the replica transitioned into readonly mode. Null if the replica is not in readonly mode." },
        { "is_session_expired",                   std::make_shared<DataTypeUInt8>(),    "Whether the session with ClickHouse Keeper has expired. Basically the same as `is_readonly`."},
        { "future_parts",                         std::make_shared<DataTypeUInt32>(),   "The number of data parts that will appear as the result of INSERTs or merges that haven't been done yet."},
        { "parts_to_check",                       std::make_shared<DataTypeUInt32>(),   "The number of data parts in the queue for verification. A part is put in the verification queue if there is suspicion that it might be damaged."},
        { "zookeeper_name",                       std::make_shared<DataTypeString>(),   "The name of the the [Zoo]Keeper cluster (possibly auxiliary one) where the table's metadata is stored"},
        { "zookeeper_path",                       std::make_shared<DataTypeString>(),   "Path to table data in ClickHouse Keeper."},
        { "replica_name",                         std::make_shared<DataTypeString>(),   "Replica name in ClickHouse Keeper. Different replicas of the same table have different names."},
        { "replica_path",                         std::make_shared<DataTypeString>(),   "Path to replica data in ClickHouse Keeper. The same as concatenating 'zookeeper_path/replicas/replica_path'."},
        { "columns_version",                      std::make_shared<DataTypeInt32>(),    "Version number of the table structure. Indicates how many times ALTER was performed. "
                                                                                            "If replicas have different versions, it means some replicas haven't made all of the ALTERs yet."},
        { "queue_size",                           std::make_shared<DataTypeUInt32>(),   "Size of the queue for operations waiting to be performed. Operations include inserting blocks of data, merges, and certain other actions. It usually coincides with future_parts."},
        { "inserts_in_queue",                     std::make_shared<DataTypeUInt32>(),   "Number of inserts of blocks of data that need to be made. Insertions are usually replicated fairly quickly. If this number is large, it means something is wrong."},
        { "merges_in_queue",                      std::make_shared<DataTypeUInt32>(),   "The number of merges waiting to be made. Sometimes merges are lengthy, so this value may be greater than zero for a long time."},
        { "part_mutations_in_queue",              std::make_shared<DataTypeUInt32>(),   "The number of mutations waiting to be made."},
        { "queue_oldest_time",                    std::make_shared<DataTypeDateTime>(), "If `queue_size` greater than 0, shows when the oldest operation was added to the queue."},
        { "inserts_oldest_time",                  std::make_shared<DataTypeDateTime>(), "See `queue_oldest_time`."},
        { "merges_oldest_time",                   std::make_shared<DataTypeDateTime>(), "See `queue_oldest_time`."},
        { "part_mutations_oldest_time",           std::make_shared<DataTypeDateTime>(), "See `queue_oldest_time`."},
        { "oldest_part_to_get",                   std::make_shared<DataTypeString>(),   "The name of the part to fetch from other replicas obtained from the oldest GET_PARTS entry in the replication queue."},
        { "oldest_part_to_merge_to",              std::make_shared<DataTypeString>(),   "The result part name to merge to obtained from the oldest MERGE_PARTS entry in the replication queue."},
        { "oldest_part_to_mutate_to",             std::make_shared<DataTypeString>(),   "The result part name to mutate to obtained from the oldest MUTATE_PARTS entry in the replication queue."},
        { "log_max_index",                        std::make_shared<DataTypeUInt64>(),   "Maximum entry number in the log of general activity."},
        { "log_pointer",                          std::make_shared<DataTypeUInt64>(),   "Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one. "
                                                                                             "If log_pointer is much smaller than log_max_index, something is wrong."},
        { "last_queue_update",                    std::make_shared<DataTypeDateTime>(), "When the queue was updated last time."},
        { "absolute_delay",                       std::make_shared<DataTypeUInt64>(),   "How big lag in seconds the current replica has."},
        { "total_replicas",                       std::make_shared<DataTypeUInt32>(),    "The total number of known replicas of this table."},
        { "active_replicas",                      std::make_shared<DataTypeUInt32>(),    "The number of replicas of this table that have a session in ClickHouse Keeper (i.e., the number of functioning replicas)."},
        { "lost_part_count",                      std::make_shared<DataTypeUInt64>(),   "The number of data parts lost in the table by all replicas in total since table creation. Value is persisted in ClickHouse Keeper and can only increase."},
        { "last_queue_update_exception",          std::make_shared<DataTypeString>(),   "When the queue contains broken entries. Especially important when ClickHouse breaks backward compatibility between versions and log entries written by newer versions aren't parseable by old versions."},
        { "zookeeper_exception",                  std::make_shared<DataTypeString>(),   "The last exception message, got if the error happened when fetching the info from ClickHouse Keeper."},
        { "replica_is_active",                    std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt8>()), "Map between replica name and is replica active."}
    };

    description.setAliases({
        {"readonly_duration", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "if(isNull(readonly_start_time), NULL, now() - readonly_start_time)"},
    });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(description);
    setInMemoryMetadata(storage_metadata);
}

StorageSystemReplicas::~StorageSystemReplicas() = default;

class ReadFromSystemReplicas : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemReplicas"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemReplicas(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::map<String, std::map<String, StoragePtr>> replicated_tables_,
        bool with_zk_fields_,
        size_t max_block_size_,
        std::shared_ptr<StorageSystemReplicas::TPools> pools_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(std::move(sample_block)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , replicated_tables(std::move(replicated_tables_))
        , with_zk_fields(with_zk_fields_)
        , max_block_size(max_block_size_)
        , pools(std::move(pools_))
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    const bool with_zk_fields;
    const size_t max_block_size;
    std::shared_ptr<StorageSystemReplicas::TPools> pools;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromSystemReplicas::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
            { ColumnString::create(), std::make_shared<DataTypeString>(), "table" },
            { ColumnString::create(), std::make_shared<DataTypeUUID>(), "uuid" },
            { ColumnString::create(), std::make_shared<DataTypeString>(), "engine" },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
}

void StorageSystemReplicas::read(
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
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// We collect a set of replicated tables.
    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        /// Check if database can contain replicated tables
        if (db.second->isExternal())
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

    auto header = storage_snapshot->metadata->getSampleBlock();
    auto reading = std::make_unique<ReadFromSystemReplicas>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(replicated_tables), with_zk_fields, max_block_size, pools);

    query_plan.addStep(std::move(reading));
}

class SystemReplicasSource : public ISource
{
public:
    SystemReplicasSource(
        SharedHeader header_,
        size_t max_block_size_,
        ColumnPtr col_database_,
        ColumnPtr col_table_,
        ColumnPtr col_uuid_,
        ColumnPtr col_engine_,
        std::vector<TFuture> futures_,
        ContextPtr context_)
        : ISource(header_)
        , max_block_size(max_block_size_)
        , col_database(std::move(col_database_))
        , col_table(std::move(col_table_))
        , col_uuid(std::move(col_uuid_))
        , col_engine(std::move(col_engine_))
        , futures(std::move(futures_))
        , context(std::move(context_))
    {
    }

    String getName() const override { return "SystemReplicas"; }

protected:
    Chunk generate() override;

private:
    const size_t max_block_size;
    /// Columns with table metadata.
    ColumnPtr col_database;
    ColumnPtr col_table;
    ColumnPtr col_uuid;
    ColumnPtr col_engine;
    /// Futures for the status of each table.
    std::vector<TFuture> futures;
    ContextPtr context;
    /// Index (row number) of the next table to process.
    size_t i = 0;
};


void ReadFromSystemReplicas::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto header = getOutputHeader();

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    MutableColumnPtr col_uuid_mut = ColumnUUID::create();
    MutableColumnPtr col_engine_mut = ColumnString::create();

    for (auto & db : replicated_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
            col_uuid_mut->insert(table.second->getStorageID().uuid);
            col_engine_mut->insert(table.second->getName());
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);
    ColumnPtr col_uuid = std::move(col_uuid_mut);
    ColumnPtr col_engine = std::move(col_engine_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
            { col_uuid, std::make_shared<DataTypeUUID>(), "uuid" },
            { col_engine, std::make_shared<DataTypeString>(), "engine" },
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
        col_table = filtered_block.getByName("table").column;
        col_uuid = filtered_block.getByName("uuid").column;
        col_engine = filtered_block.getByName("engine").column;
    }

    size_t tables_size = col_database->size();

    /// Use separate queues for requests with and without ZooKeeper fields.
    auto & get_status_requests = with_zk_fields ? pools->requests_with_zk_fields : pools->requests_without_zk_fields;

    QueryStatusPtr query_status = context ? context->getProcessListElement() : nullptr;

    std::vector<TFuture> futures;
    futures.reserve(tables_size);
    UInt64 max_request_id = 0;
    for (size_t i = 0; i < tables_size; ++i)
    {
        if (query_status)
            query_status->checkTimeLimit();

        auto & storage = replicated_tables[(*col_database)[i].safeGet<String>()]
            [(*col_table)[i].safeGet<String>()];

        auto [request_id, future] = get_status_requests.addRequest(storage, with_zk_fields);
        futures.emplace_back(future);
        max_request_id = std::max(max_request_id, request_id);
    }
    /// Schedule requests up to the maximum request needed for the current query.
    /// If there are more requests, they will be scheduled by the query that needs them.
    get_status_requests.scheduleRequests(max_request_id, query_status);

    pipeline.init(Pipe(std::make_shared<SystemReplicasSource>(header, max_block_size, col_database, col_table, col_uuid, col_engine, std::move(futures), context)));
}

Chunk SystemReplicasSource::generate()
{
    if (i == futures.size())
        return {};

    QueryStatusPtr query_status = context ? context->getProcessListElement() : nullptr;

    MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

    bool rows_added = false;

    LoggerPtr logger = getLogger("SystemReplicasSource::generate");

    for (; i < futures.size(); ++i)
    {
        if (query_status)
            query_status->checkTimeLimit();

        if (rows_added)
        {
            /// Return current chunk if the next future is not ready yet
            if (futures[i].wait_for(std::chrono::seconds(0)) != std::future_status::ready)
                break;

            if (max_block_size != 0)
            {
                size_t total_size = 0;
                for (const auto & column : res_columns)
                    total_size += column->byteSize();
                /// If the block size exceeds the maximum, return the current block
                if (total_size >= max_block_size)
                    break;
            }
        }

        const TStatus * status;
        try
        {
            status = &futures[i].get();
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::ABORTED)
            {
                tryLogCurrentException(logger, "Received the ABORTED error while trying to get the status of a storage, this is likely because it has been shut down");
                continue;
            }
            throw;
        }

        size_t col_num = 0;
        res_columns[col_num++]->insert((*col_database)[i]);
        res_columns[col_num++]->insert((*col_table)[i]);
        res_columns[col_num++]->insert((*col_uuid)[i]);
        res_columns[col_num++]->insert((*col_engine)[i]);

        res_columns[col_num++]->insert(status->is_leader);
        res_columns[col_num++]->insert(status->can_become_leader);
        res_columns[col_num++]->insert(status->is_readonly);
        if (status->readonly_start_time != 0)
            res_columns[col_num++]->insert(status->readonly_start_time);
        else
            res_columns[col_num++]->insertDefault();
        res_columns[col_num++]->insert(status->is_session_expired);
        res_columns[col_num++]->insert(status->queue.future_parts);
        res_columns[col_num++]->insert(status->parts_to_check);
        res_columns[col_num++]->insert(status->zookeeper_info.zookeeper_name);
        res_columns[col_num++]->insert(status->zookeeper_info.path);
        res_columns[col_num++]->insert(status->zookeeper_info.replica_name);
        res_columns[col_num++]->insert(status->replica_path);
        res_columns[col_num++]->insert(status->columns_version);
        res_columns[col_num++]->insert(status->queue.queue_size);
        res_columns[col_num++]->insert(status->queue.inserts_in_queue);
        res_columns[col_num++]->insert(status->queue.merges_in_queue);
        res_columns[col_num++]->insert(status->queue.part_mutations_in_queue);
        res_columns[col_num++]->insert(status->queue.queue_oldest_time);
        res_columns[col_num++]->insert(status->queue.inserts_oldest_time);
        res_columns[col_num++]->insert(status->queue.merges_oldest_time);
        res_columns[col_num++]->insert(status->queue.part_mutations_oldest_time);
        res_columns[col_num++]->insert(status->queue.oldest_part_to_get);
        res_columns[col_num++]->insert(status->queue.oldest_part_to_merge_to);
        res_columns[col_num++]->insert(status->queue.oldest_part_to_mutate_to);
        res_columns[col_num++]->insert(status->log_max_index);
        res_columns[col_num++]->insert(status->log_pointer);
        res_columns[col_num++]->insert(status->queue.last_queue_update);
        res_columns[col_num++]->insert(status->absolute_delay);
        res_columns[col_num++]->insert(status->total_replicas);
        res_columns[col_num++]->insert(status->active_replicas);
        res_columns[col_num++]->insert(status->lost_part_count);
        res_columns[col_num++]->insert(status->last_queue_update_exception);
        res_columns[col_num++]->insert(status->zookeeper_exception);

        Map replica_is_active_values;
        for (const auto & [name, is_active] : status->replica_is_active)
        {
            Tuple is_replica_active_value;
            is_replica_active_value.emplace_back(name);
            is_replica_active_value.emplace_back(is_active);

            replica_is_active_values.emplace_back(std::move(is_replica_active_value));
        }

        res_columns[col_num++]->insert(std::move(replica_is_active_values));

        rows_added = true;
    }

    UInt64 num_rows = res_columns.at(0)->size();
    return Chunk(std::move(res_columns), num_rows);
}

}
