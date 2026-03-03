#include <algorithm>
#include <unordered_map>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnTuple.h>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/distributedIndexAnalysis.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#if defined(OS_LINUX)
#include <Client/HedgedConnectionsFactory.h>
#include <Common/Epoll.h>
#endif
#include <Core/Settings.h>
#include <Common/CurrentMetrics.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/ConnectionTimeouts.h>
#include <Columns/ColumnString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/ranges.h>
#include <consistent_hashing.h>


namespace DB::ErrorCodes
{
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsUInt64 connections_with_failover_max_tries;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool use_hedged_requests;
}

namespace ProfileEvents
{
    extern const Event DistributedIndexAnalysisMicroseconds;
    extern const Event DistributedIndexAnalysisReplicaFallback;
    extern const Event DistributedIndexAnalysisReplicaUnavailable;
    extern const Event DistributedIndexAnalysisMissingParts;
    extern const Event DistributedIndexAnalysisScheduledReplicas;
    extern const Event DistributedConnectionFailAtAll;
}

namespace CurrentMetrics
{
    extern const Metric DistributedIndexAnalysisThreads;
    extern const Metric DistributedIndexAnalysisThreadsActive;
    extern const Metric DistributedIndexAnalysisThreadsScheduled;
}

namespace
{

using namespace DB;

size_t partReplica(const std::string & part_name, size_t replicas_count)
{
    auto hash = SipHash();
    hash.update(part_name);
    return ConsistentHashing(hash.get64(), replicas_count);
}

using ShuffledPool = ConnectionPoolWithFailover::Base::ShuffledPool;

size_t findLocalReplica(const std::vector<ShuffledPool> & pools, const Cluster::Addresses & local_addresses)
{
    std::optional<size_t> local_replica_index;
    for (size_t i = 0, s = pools.size(); i < s; ++i)
    {
        const auto & pool = dynamic_cast<ConnectionPool &>(*pools[i].pool);
        const auto & hostname = pool.getHost();
        const auto & port = pool.getPort();
        const auto found = std::find_if(begin(local_addresses), end(local_addresses), [&hostname, &port](const auto & local_addr)
        {
            return hostname == local_addr.host_name && port == local_addr.port;
        });
        if (found != local_addresses.end())
        {
            local_replica_index = i;
            break;
        }
    }
    if (!local_replica_index)
        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION, "Local replica in cluster_for_parallel_replicas");

    return local_replica_index.value();
}

/// Preserve replicas order as in cluster definition.
/// It's important for data locality during query execution independently of the query initiator.
GetPriorityForLoadBalancing::Func replicaIndexPriorityFunc()
{
    return [](size_t i) { return Priority{static_cast<Int64>(i)}; };
}

std::string buildAnalyzeIndexQuery(const StorageID & storage_id, const std::optional<std::string> & filter,
                                   const OptionalVectorSearchParameters & vector_search_parameters,
                                   const std::vector<std::string_view> & parts)
{
    std::string query = fmt::format("SELECT * FROM mergeTreeAnalyzeIndexesUUID('{}', {}, ['{}']",
        storage_id.uuid,
        filter.value_or("true"),
        fmt::join(parts, "','"));

    if (vector_search_parameters)
    {
        query += fmt::format(", 'vector_search_index_analysis', array('{}', '{}', {}, {}, {}, {})",
                        vector_search_parameters->column, vector_search_parameters->distance_function,
                        vector_search_parameters->limit, vector_search_parameters->reference_vector,
                        vector_search_parameters->additional_filters_present, vector_search_parameters->return_distances);
    }

    query += ")";
    return query;
}

SharedHeader indexAnalysisSampleBlock()
{
    return std::make_shared<const Block>(Block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "part_name" },
        { ColumnArray::create(ColumnTuple::create(Columns{
              ColumnUInt64::create(), // begin
              ColumnUInt64::create(), // end
          })),
          std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{
              std::make_shared<DataTypeUInt64>(), // begin
              std::make_shared<DataTypeUInt64>(), // end
          })),
          "ranges" },
    });
}

void parseIndexAnalysisBlock(Block block, IndexAnalysisPartsRanges & result)
{
    block = convertBLOBColumns(block);

    const auto & col_part_name = assert_cast<const ColumnString &>(*block.getByName("part_name").column);
    const auto & col_ranges_array = assert_cast<const ColumnArray &>(*block.getByName("ranges").column);
    const auto & col_ranges_array_offsets = col_ranges_array.getOffsets();
    const auto & col_ranges_tuple = assert_cast<const ColumnTuple &>(col_ranges_array.getData());
    const auto & col_range_start = assert_cast<const ColumnUInt64 &>(col_ranges_tuple.getColumn(0)).getData();
    const auto & col_range_end = assert_cast<const ColumnUInt64 &>(col_ranges_tuple.getColumn(1)).getData();

    for (size_t i = 0; i < col_part_name.size(); ++i)
    {
        auto & ranges_dst = result[std::string(col_part_name.getDataAt(i))];
        for (size_t range_i = col_ranges_array_offsets[i - 1]; range_i < col_ranges_array_offsets[i]; ++range_i)
            ranges_dst.push_back(MarkRange{col_range_start[range_i], col_range_end[range_i]});
    }
}

IndexAnalysisPartsRanges getIndexAnalysisFromReplicaSync(const LoggerPtr & logger, const StorageID & storage_id, const std::optional<std::string> & filter,
                                                     const OptionalVectorSearchParameters & vector_search_parameters, ContextPtr context, const Tables & external_tables,
                                                     const std::vector<std::string_view> & parts, Connection & connection)
{
    auto query = buildAnalyzeIndexQuery(storage_id, filter, vector_search_parameters, parts);
    auto sample_block = indexAnalysisSampleBlock();

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(connection, query, sample_block, context, ThrottlerPtr{}, Scalars{}, external_tables);
    remote_query_executor->setLogger(logger);
    auto remote_source = std::make_shared<RemoteSource>(std::move(remote_query_executor), false, false, false);
    QueryPipeline pipeline(std::move(remote_source));
    PullingPipelineExecutor executor(pipeline);

    IndexAnalysisPartsRanges res;
    Block block;
    while (executor.pull(block))
        parseIndexAnalysisBlock(std::move(block), res);

    return res;
}

ASTPtr getFilterAST(const ActionsDAG & filter_actions_dag, const NameSet & indexes_column_names, ContextMutablePtr & context, Tables * external_tables)
{
    ASTPtr predicate = tryBuildAdditionalFilterAST(filter_actions_dag,
        /*projection_names=*/ indexes_column_names,
        /*execution_name_to_projection_query_tree=*/ {},
        /*external_tables=*/ external_tables,
        context);
    if (!predicate)
        return nullptr;

    return predicate;
}


class DistributedIndexAnalyzer
{
public:
    DistributedIndexAnalyzer(
        const StorageID & storage_id_,
        const ActionsDAG * filter_actions_dag,
        const NameSet & indexes_column_names,
        const RangesInDataParts & parts_with_ranges_,
        const OptionalVectorSearchParameters & vector_search_parameters_,
        LocalIndexAnalysisCallback local_index_analysis_callback_,
        ContextPtr context_)
        : storage_id(storage_id_)
        , parts_with_ranges(parts_with_ranges_)
        , vector_search_parameters(vector_search_parameters_)
        , local_index_analysis_callback(std::move(local_index_analysis_callback_))
        , context(std::move(context_))
        , logger(getLogger("DistributedIndexAnalysis"))
        , settings(context->getSettingsRef())
#if defined(OS_LINUX)
        , use_hedged_requests(settings[Setting::use_hedged_requests])
        , use_async_reading(settings[Setting::async_socket_for_remote])
#endif
    {
        auto cluster = context->getClusterForParallelReplicas();
        const auto & shard = cluster->getShardsInfo().at(0);
        shuffled_pools = shard.pool->getShuffledPools(settings, replicaIndexPriorityFunc());
        local_replica_index = findLocalReplica(shuffled_pools, shard.local_addresses);
        total_replicas = shard.getAllNodeCount();
        max_active_replicas = std::min<size_t>(settings[Setting::max_parallel_replicas], total_replicas);

        /// Build a pool that excludes the local replica, for hedged connections.
        const auto * local_pool = shuffled_pools[local_replica_index].pool.get();
        ConnectionPoolPtrs remote_pools;
        remote_pools.reserve(total_replicas - 1);
        for (const auto & pool : shard.per_replica_pools)
        {
            if (pool && pool.get() != local_pool)
                remote_pools.push_back(pool);
        }
        remote_pool = std::make_shared<ConnectionPoolWithFailover>(std::move(remote_pools), LoadBalancing::NEAREST_HOSTNAME);

        replica_addresses.resize(total_replicas);
        for (size_t i = 0; i < shuffled_pools.size(); ++i)
            replica_addresses[i] = dynamic_cast<ConnectionPool &>(*shuffled_pools[i].pool).getAddress();

        execution_context = Context::createCopy(context);
        external_tables = execution_context->getExternalTables();

        if (filter_actions_dag)
        {
            auto filter_ast = getFilterAST(*filter_actions_dag, indexes_column_names, execution_context, &external_tables);
            if (filter_ast)
                filter_query = filter_ast->formatWithSecretsOneLine();
        }
    }

    DistributedIndexAnalysisPartsRanges run()
    {
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
        auto connections_by_replica = establishConnections(timeouts);

        auto active_replica_indexes = selectActiveReplicas(connections_by_replica);

        auto [replicas_parts, replicas_marks, replicas_rows] = distributePartsToReplicas(active_replica_indexes);

        DistributedIndexAnalysisPartsRanges res(total_replicas);
        for (const auto i : active_replica_indexes)
            res[i].first = replica_addresses[i];

        auto [local_thread, local_exception] = executeLocalAnalysis(replicas_parts, replicas_marks, replicas_rows, res);
        executeRemoteAnalysis(active_replica_indexes, connections_by_replica, replicas_parts, replicas_marks, replicas_rows, res);

        if (local_thread.joinable())
            local_thread.join();
        if (*local_exception)
            std::rethrow_exception(*local_exception);

        resolveMissingParts(res);

        return res;
    }

private:
    /// Returns connections_by_replica[total_replicas], nullptr for unavailable.
    std::vector<Connection *> establishConnections(const ConnectionTimeouts & timeouts)
    {
        if (use_hedged_requests)
            return establishConnectionsAsync(timeouts);
        return establishConnectionsSync(timeouts);
    }

#if defined(OS_LINUX)
    std::vector<Connection *> establishConnectionsAsync(const ConnectionTimeouts & timeouts)
    {
        std::vector<Connection *> connections_by_replica(total_replicas, nullptr);

        hedged_factory.emplace(
            remote_pool,
            settings,
            timeouts,
            settings[Setting::connections_with_failover_max_tries],
            settings[Setting::fallback_to_stale_replicas_for_distributed_queries],
            /// Exclude local replica
            max_active_replicas - 1,
            /*skip_unavailable_shards_=*/ true,
            /// FIXME: we can pass db.table, but, we use UUIDs internally, so we need first to add support of checking UUIDs
            /*table_to_check=*/ nullptr,
            replicaIndexPriorityFunc());

        auto ready_connections = hedged_factory->getManyConnections(PoolMode::GET_MANY);
        ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaUnavailable, hedged_factory->getFailedPoolsCount());

        /// Map connections back to replica indices via address.
        std::unordered_map<std::string, size_t> address_to_replica;
        for (size_t i = 0; i < shuffled_pools.size(); ++i)
        {
            if (i == local_replica_index)
                continue;
            auto & pool = dynamic_cast<ConnectionPool &>(*shuffled_pools[i].pool);
            address_to_replica[pool.getDescription()] = i;
        }
        for (auto * conn : ready_connections)
        {
            auto key = conn->getDescription(/*with_extra=*/ false);
            if (auto it = address_to_replica.find(key); it != address_to_replica.end())
                connections_by_replica[it->second] = conn;
        }

        return connections_by_replica;
    }
#else
    std::vector<Connection *> establishConnectionsAsync(const ConnectionTimeouts &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hedged connections are not supported on this platform");
    }
#endif

    ThreadPool & getThreadPool()
    {
        if (!sync_thread_pool)
            sync_thread_pool.emplace(
                CurrentMetrics::DistributedIndexAnalysisThreads,
                CurrentMetrics::DistributedIndexAnalysisThreadsActive,
                CurrentMetrics::DistributedIndexAnalysisThreadsScheduled,
                total_replicas);
        return *sync_thread_pool;
    }

    std::vector<Connection *> establishConnectionsSync(const ConnectionTimeouts & timeouts)
    {
        std::vector<Connection *> connections_by_replica(total_replicas, nullptr);
        connection_entries.resize(total_replicas);

        {
            ThreadPoolCallbackRunnerLocal<void> connect_runner(getThreadPool(), DB::ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
            for (size_t i = 0; i < total_replicas; ++i)
            {
                if (i == local_replica_index)
                    continue;
                connect_runner.enqueueAndKeepTrack([this, i, &timeouts]()
                {
                    try
                    {
                        /// NOTE: does not apply connections_with_failover_max_tries
                        connection_entries[i] = shuffled_pools[i].pool->get(timeouts, settings);
                    }
                    catch (...)
                    {
                        ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaUnavailable);
                        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
                        const auto & address = dynamic_cast<ConnectionPool &>(*shuffled_pools[i].pool).getAddress();
                        tryLogCurrentException(logger, fmt::format("Cannot connect to {} (index {}). It will not participate in distributed index analysis", address, i), LogsLevel::warning);
                    }
                }, Priority{});
            }
            connect_runner.waitForAllToFinishAndRethrowFirstError();
        }

        for (size_t i = 0; i < total_replicas; ++i)
        {
            if (!connection_entries[i].isNull())
                connections_by_replica[i] = &*connection_entries[i];
        }

        return connections_by_replica;
    }

    std::vector<size_t> selectActiveReplicas(const std::vector<Connection *> & connections_by_replica) const
    {
        std::vector<size_t> active_replica_indexes;
        active_replica_indexes.reserve(max_active_replicas);
        active_replica_indexes.push_back(local_replica_index);
        for (size_t i = 0; i < total_replicas && active_replica_indexes.size() < max_active_replicas; ++i)
        {
            if (i != local_replica_index && connections_by_replica[i] != nullptr)
                active_replica_indexes.push_back(i);
        }
        /// Sort because local_replica_index may not be the smallest.
        std::sort(active_replica_indexes.begin(), active_replica_indexes.end());

        LOG_DEBUG(logger, "Distributed index analysis for {} (total replicas: {}, local replica index: {}, active replicas: {} ({}))",
                  storage_id.getNameForLogs(),
                  total_replicas, local_replica_index, active_replica_indexes.size(), fmt::join(active_replica_indexes, ", "));

        return active_replica_indexes;
    }

    struct DistributedParts
    {
        std::vector<std::vector<std::string_view>> replicas_parts;
        std::vector<size_t> replicas_marks;
        std::vector<size_t> replicas_rows;
    };

    DistributedParts distributePartsToReplicas(const std::vector<size_t> & active_replica_indexes) const
    {
        DistributedParts result;
        result.replicas_parts.resize(total_replicas);
        result.replicas_marks.resize(total_replicas, 0);
        result.replicas_rows.resize(total_replicas, 0);

        for (const auto & part_ranges : parts_with_ranges)
        {
            chassert(part_ranges.ranges.size() == 1);
            chassert(part_ranges.exact_ranges.empty());

            const auto & part_name = part_ranges.data_part->name;
            const auto hash_index = partReplica(part_name, active_replica_indexes.size());
            const auto replica_index = active_replica_indexes[hash_index];
            result.replicas_parts[replica_index].push_back(part_name);

            result.replicas_marks[replica_index] += part_ranges.getMarksCount();
            result.replicas_rows[replica_index] += part_ranges.getRowsCount();
        }

        return result;
    }

    struct LocalAnalysisResult
    {
        ThreadFromGlobalPool thread;
        std::shared_ptr<std::exception_ptr> exception = std::make_shared<std::exception_ptr>();
    };

    LocalAnalysisResult executeLocalAnalysis(
        const std::vector<std::vector<std::string_view>> & replicas_parts,
        const std::vector<size_t> & replicas_marks,
        const std::vector<size_t> & replicas_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        LocalAnalysisResult result;
        const auto & local_parts = replicas_parts[local_replica_index];
        if (!local_parts.empty())
        {
            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            auto exception_ptr = result.exception;
            result.thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup(), &local_parts, &replicas_marks, &replicas_rows, &res, exception_ptr]()
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
                try
                {
                    LOG_TRACE(logger, "Resolving {} parts ({} marks, {} rows) from local replica {} (index {}): {}",
                        local_parts.size(), replicas_marks[local_replica_index], replicas_rows[local_replica_index],
                        replica_addresses[local_replica_index], local_replica_index, local_parts);
                    auto parts_ranges = local_index_analysis_callback(local_parts);
                    LOG_TRACE(logger, "Received {} parts from local replica {} (index {}): {}",
                        parts_ranges.size(), replica_addresses[local_replica_index], local_replica_index, parts_ranges);
                    res[local_replica_index].second = std::move(parts_ranges);
                }
                catch (...)
                {
                    *exception_ptr = std::current_exception();
                }
            });
        }
        return result;
    }

    void executeRemoteAnalysis(
        const std::vector<size_t> & active_replica_indexes,
        const std::vector<Connection *> & connections_by_replica,
        const std::vector<std::vector<std::string_view>> & replicas_parts,
        const std::vector<size_t> & replicas_marks,
        const std::vector<size_t> & replicas_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        if (use_async_reading)
            executeRemoteAnalysisAsync(active_replica_indexes, connections_by_replica, replicas_parts, replicas_marks, replicas_rows, res);
        else
            executeRemoteAnalysisSync(active_replica_indexes, connections_by_replica, replicas_parts, replicas_marks, replicas_rows, res);
    }

#if defined(OS_LINUX)
    void executeRemoteAnalysisAsync(
        const std::vector<size_t> & active_replica_indexes,
        const std::vector<Connection *> & connections_by_replica,
        const std::vector<std::vector<std::string_view>> & replicas_parts,
        const std::vector<size_t> & replicas_marks,
        const std::vector<size_t> & replicas_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        struct ReplicaReader
        {
            std::shared_ptr<RemoteQueryExecutor> executor;
            size_t replica_index;
            bool query_sent = false;
        };

        auto sample_block = indexAnalysisSampleBlock();
        Epoll readers_epoll;
        std::vector<ReplicaReader> readers;
        std::unordered_map<int, size_t> fd_to_reader;

        for (const auto i : active_replica_indexes)
        {
            if (i == local_replica_index)
                continue;
            auto * connection = connections_by_replica[i];
            if (!connection || replicas_parts[i].empty())
                continue;

            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            auto query = buildAnalyzeIndexQuery(storage_id, filter_query, vector_search_parameters, replicas_parts[i]);
            auto executor = std::make_shared<RemoteQueryExecutor>(*connection, query, sample_block, execution_context, ThrottlerPtr{}, Scalars{}, external_tables);
            executor->setLogger(logger);

            LOG_TRACE(logger, "Sending {} parts ({} marks, {} rows) to {} (index {}): {}",
                replicas_parts[i].size(), replicas_marks[i], replicas_rows[i], replica_addresses[i], i, replicas_parts[i]);

            size_t reader_index = readers.size();
            readers.push_back({std::move(executor), i});

            int fd = readers[reader_index].executor->sendQueryAsync();
            if (fd >= 0)
            {
                readers_epoll.add(fd);
                fd_to_reader[fd] = reader_index;
            }
            else
            {
                readers[reader_index].query_sent = true;
            }
        }

        auto process_reader = [&](size_t reader_index)
        {
            auto & reader = readers[reader_index];
            const auto i = reader.replica_index;
            try
            {
                while (!reader.query_sent)
                {
                    int fd = reader.executor->sendQueryAsync();
                    if (fd >= 0)
                    {
                        readers_epoll.add(fd);
                        fd_to_reader[fd] = reader_index;
                        return;
                    }
                    reader.query_sent = true;
                }

                while (true)
                {
                    auto result = reader.executor->readAsync();

                    if (result.getType() == RemoteQueryExecutor::ReadResult::Type::FileDescriptor)
                    {
                        readers_epoll.add(result.getFileDescriptor());
                        fd_to_reader[result.getFileDescriptor()] = reader_index;
                        return;
                    }

                    if (result.getType() == RemoteQueryExecutor::ReadResult::Type::Data)
                    {
                        auto block = result.getBlock();
                        if (block.empty())
                            break;
                        parseIndexAnalysisBlock(std::move(block), res[i].second);
                        continue;
                    }

                    break;
                }

                LOG_TRACE(logger, "Received {} parts from {} (index {}): {}", res[i].second.size(), replica_addresses[i], i, res[i].second);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                    throw;
                ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica (index {}). They will be analyzed on initiator", replica_addresses[i], i), LogsLevel::warning);
            }
            catch (...)
            {
                ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica (index {}). They will be analyzed on initiator", replica_addresses[i], i), LogsLevel::warning);
            }
        };

        for (size_t ri = 0; ri < readers.size(); ++ri)
        {
            if (readers[ri].query_sent)
                process_reader(ri);
        }

        while (!readers_epoll.empty())
        {
            epoll_event event;
            event.data.fd = -1;
            readers_epoll.getManyReady(1, &event, -1);

            int ready_fd = event.data.fd;
            auto it = fd_to_reader.find(ready_fd);
            chassert(it != fd_to_reader.end());
            size_t reader_index = it->second;
            fd_to_reader.erase(it);
            readers_epoll.remove(ready_fd);

            process_reader(reader_index);
        }
    }
#else
    void executeRemoteAnalysisAsync(
        const std::vector<size_t> &,
        const std::vector<Connection *> &,
        const std::vector<std::vector<std::string_view>> &,
        const std::vector<size_t> &,
        const std::vector<size_t> &,
        DistributedIndexAnalysisPartsRanges &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Async remote analysis is not supported on this platform");
    }
#endif

    void executeRemoteAnalysisSync(
        const std::vector<size_t> & active_replica_indexes,
        const std::vector<Connection *> & connections_by_replica,
        const std::vector<std::vector<std::string_view>> & replicas_parts,
        const std::vector<size_t> & replicas_marks,
        const std::vector<size_t> & replicas_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        ThreadPoolCallbackRunnerLocal<void> runner(getThreadPool(), DB::ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
        for (const auto i : active_replica_indexes)
        {
            if (i == local_replica_index)
                continue;
            auto * connection = connections_by_replica[i];
            if (!connection || replicas_parts[i].empty())
                continue;

            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            runner.enqueueAndKeepTrack([this, i, connection, &replicas_parts, &replicas_marks, &replicas_rows, &res]()
            {
                try
                {
                    LOG_TRACE(logger, "Sending {} parts ({} marks, {} rows) to {} (index {}): {}",
                        replicas_parts[i].size(), replicas_marks[i], replicas_rows[i], replica_addresses[i], i, replicas_parts[i]);
                    auto parts_ranges = getIndexAnalysisFromReplicaSync(logger, storage_id, filter_query, vector_search_parameters, execution_context, external_tables, replicas_parts[i], *connection);
                    LOG_TRACE(logger, "Received {} parts from {} (index {}): {}", parts_ranges.size(), replica_addresses[i], i, parts_ranges);
                    res[i].second = std::move(parts_ranges);
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                        throw;
                    ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                    tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica (index {}). They will be analyzed on initiator", replica_addresses[i], i), LogsLevel::warning);
                }
                catch (...)
                {
                    ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                    tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica (index {}). They will be analyzed on initiator", replica_addresses[i], i), LogsLevel::warning);
                }
            }, Priority{});
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    void resolveMissingParts(DistributedIndexAnalysisPartsRanges & res) const
    {
        std::unordered_set<std::string_view> resolved_parts;
        for (const auto & [_, parts_ranges] : res)
        {
            for (const auto & [part, replica_ranges] : parts_ranges)
                resolved_parts.insert(part);
        }
        std::vector<std::string_view> missing_parts;
        size_t missing_parts_marks = 0;
        size_t missing_parts_rows = 0;
        for (const auto & part_ranges : parts_with_ranges)
        {
            const auto & part_name = part_ranges.data_part->name;
            if (resolved_parts.contains(part_name))
                continue;
            missing_parts.push_back(part_name);
            missing_parts_marks += part_ranges.getMarksCount();
            missing_parts_rows += part_ranges.getRowsCount();
        }

        if (!missing_parts.empty())
        {
            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisMissingParts);

            const auto & local_replica_address = replica_addresses[local_replica_index];
            LOG_TRACE(logger, "Resolving {} missing parts ({} marks, {} rows) from local replica {} (index {}): {}", missing_parts.size(), missing_parts_marks, missing_parts_rows, local_replica_address, local_replica_index, missing_parts);
            auto parts_ranges = local_index_analysis_callback(missing_parts);
            LOG_TRACE(logger, "Received {} missing parts from local replica {} (index {}): {}", parts_ranges.size(), local_replica_address, local_replica_index, parts_ranges);
            res[local_replica_index].first = local_replica_address;
            res[local_replica_index].second.insert_range(std::move(parts_ranges));
        }
    }

    const StorageID & storage_id;
    const RangesInDataParts & parts_with_ranges;
    const OptionalVectorSearchParameters & vector_search_parameters;
    LocalIndexAnalysisCallback local_index_analysis_callback;
    ContextPtr context;

    LoggerPtr logger;
    const Settings & settings;
    bool use_hedged_requests = false;
    bool use_async_reading = false;
    size_t total_replicas;
    size_t local_replica_index;
    size_t max_active_replicas;
    ConnectionPoolWithFailoverPtr remote_pool;
    std::vector<ShuffledPool> shuffled_pools;
    std::vector<std::string> replica_addresses;

    std::optional<std::string> filter_query;
    ContextMutablePtr execution_context;
    Tables external_tables;

    /// Shared thread pool for the sync path (connections + remote analysis).
    std::optional<ThreadPool> sync_thread_pool;

    /// Keep `ConnectionPool::Entry` objects alive for the sync path.
    std::vector<ConnectionPool::Entry> connection_entries;

#if defined(OS_LINUX)
    /// Keep `HedgedConnectionsFactory` alive for the async path.
    std::optional<HedgedConnectionsFactory> hedged_factory;
#endif
};

}

namespace DB
{

DistributedIndexAnalysisPartsRanges distributedIndexAnalysisOnReplicas(
    const StorageID & storage_id,
    const ActionsDAG * filter_actions_dag,
    const NameSet & indexes_column_names,
    const RangesInDataParts & parts_with_ranges,
    const OptionalVectorSearchParameters & vector_search_parameters,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::DistributedIndexAnalysisMicroseconds);

    DistributedIndexAnalyzer analyzer(
        storage_id, filter_actions_dag, indexes_column_names,
        parts_with_ranges, vector_search_parameters,
        std::move(local_index_analysis_callback), std::move(context));

    return analyzer.run();
}

}
