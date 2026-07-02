#include <algorithm>
#include <Common/CurrentThread.h>
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
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsUInt64 connections_with_failover_max_tries;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool use_hedged_requests;
    extern const SettingsBool distributed_index_analysis_only_on_coordinator;
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

/// Consistent hash of an individual mark segment, so segments of one part spread across replicas.
size_t partReplicaSegment(const std::string & part_name, size_t segment_index, size_t replicas_count)
{
    auto hash = SipHash();
    hash.update(part_name);
    hash.update(segment_index);
    return ConsistentHashing(hash.get64(), replicas_count);
}

using ShuffledPool = ConnectionPoolWithFailover::Base::ShuffledPool;

/// Find and remove the local replica from the pools.
/// Returns its original ShuffledPool (with `index` preserving the original position).
ShuffledPool extractLocalReplica(std::vector<ShuffledPool> & pools, const Cluster::Addresses & local_addresses)
{
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
            auto local = std::move(pools[i]);
            pools.erase(pools.begin() + i);
            return local;
        }
    }
    throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION, "Local replica in cluster_for_parallel_replicas");
}

/// Preserve replicas order as in cluster definition.
/// It's important for data locality during query execution independently of the query initiator.
GetPriorityForLoadBalancing::Func replicaIndexPriorityFunc()
{
    return [](size_t i) { return Priority{static_cast<Int64>(i)}; };
}

std::string buildAnalyzeIndexQuery(const StorageID & storage_id, const std::optional<std::string> & filter,
                                   const OptionalVectorSearchParameters & vector_search_parameters,
                                   const AssignedPartsRanges & parts, bool with_ranges)
{
    /// Without ranges: the plain Array(String) form (backward-compatible wire format).
    /// With ranges: array(('part', [(begin, end), ...]), ...) understood by upgraded replicas.
    std::string parts_arg;
    if (!with_ranges)
    {
        std::vector<std::string_view> names;
        names.reserve(parts.size());
        for (const auto & [name, _] : parts)
            names.push_back(name);
        parts_arg = fmt::format("['{}']", fmt::join(names, "','"));
    }
    else
    {
        parts_arg = "array(";
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (i)
                parts_arg += ", ";
            parts_arg += fmt::format("('{}', [{}])", parts[i].first, fmt::join(parts[i].second, ", "));
        }
        parts_arg += ")";
    }

    std::string query = fmt::format("SELECT * FROM mergeTreeAnalyzeIndexesUUID('{}', {}, {}",
        storage_id.uuid,
        filter.value_or("true"),
        parts_arg);

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
                                                     const AssignedPartsRanges & parts, bool with_ranges, Connection & connection)
{
    auto query = buildAnalyzeIndexQuery(storage_id, filter, vector_search_parameters, parts, with_ranges);
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
        ASTPtr sampling_filter,
        const NameSet & indexes_column_names,
        const RangesInDataParts & parts_with_ranges_,
        const OptionalVectorSearchParameters & vector_search_parameters_,
        size_t mark_segment_size_,
        size_t min_marks_to_split_part_,
        LocalIndexAnalysisCallback local_index_analysis_callback_,
        ContextPtr context_)
        : storage_id(storage_id_)
        , parts_with_ranges(parts_with_ranges_)
        , vector_search_parameters(vector_search_parameters_)
        , mark_segment_size(mark_segment_size_)
        , min_marks_to_split_part(min_marks_to_split_part_)
        /// Vector similarity index analysis requires whole-part ranges (the `all_match` guard in
        /// `filterMarksUsingIndex`), so splitting would silently disable it on every replica.
        , split_segments(mark_segment_size_ > 0 && !vector_search_parameters_.has_value())
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
        original_pool = shard.pool;
        remote_pools = shard.pool->getShuffledPools(settings, replicaIndexPriorityFunc());
        auto local_pool = extractLocalReplica(remote_pools, shard.local_addresses);
        local_address = dynamic_cast<ConnectionPool &>(*local_pool.pool).getAddress();
        local_original_index = local_pool.index;
        total_replicas = shard.getAllNodeCount();
        remote_replicas = remote_pools.size();
        max_active_replicas = std::min<size_t>(settings[Setting::max_parallel_replicas], total_replicas);

        replica_addresses.resize(remote_replicas);
        for (size_t i = 0; i < remote_replicas; ++i)
            replica_addresses[i] = dynamic_cast<ConnectionPool &>(*remote_pools[i].pool).getAddress();

        execution_context = Context::createCopy(context);
        external_tables = execution_context->getExternalTables();

        ASTPtr filter_ast;
        if (filter_actions_dag)
            filter_ast = getFilterAST(*filter_actions_dag, indexes_column_names, execution_context, &external_tables);

        if (filter_ast && sampling_filter)
            filter_ast = makeASTForLogicalAnd({filter_ast, sampling_filter});
        else if (sampling_filter)
            filter_ast = sampling_filter;

        if (filter_ast)
            filter_query = filter_ast->formatWithSecretsOneLine();
    }

    DistributedIndexAnalysisPartsRanges run()
    {
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
        auto connections = establishConnections(timeouts);

        auto active_remote_indexes = selectActiveRemoteReplicas(connections);

        auto distributed_parts = distributePartsToReplicas(active_remote_indexes);

        /// Remote results indexed by remote_pools index.
        DistributedIndexAnalysisPartsRanges res(remote_replicas);
        for (const auto i : active_remote_indexes)
        {
            res[i].address = replica_addresses[i];
            res[i].assigned_parts = distributed_parts.remote_parts[i].size();
            res[i].assigned_marks = distributed_parts.remote_marks[i];
        }

        /// Local result stored separately.
        DistributedIndexAnalysisReplicaResult local_result{
            local_address, distributed_parts.local_parts.size(), distributed_parts.local_marks, {}};

        auto [local_thread, local_exception] = executeLocalAnalysis(distributed_parts.local_parts, distributed_parts.local_marks, distributed_parts.local_rows, local_result);
        try
        {
            executeRemoteAnalysis(active_remote_indexes, connections, distributed_parts.remote_parts, distributed_parts.remote_marks, distributed_parts.remote_rows, res);
        }
        catch (...)
        {
            propagateErrorCounts();
            if (local_thread.joinable())
                local_thread.join();
            /// It make sense to re-throw exception from local resolving, since it should be more meaningful (it does not includes any network problems).
            if (*local_exception)
                std::rethrow_exception(*local_exception);
            throw;
        }

        propagateErrorCounts();

        if (local_thread.joinable())
            local_thread.join();
        if (*local_exception)
            std::rethrow_exception(*local_exception);

        resolveMissingParts(distributed_parts, local_result, res);

        res.push_back(std::move(local_result));
        return res;
    }

private:
    /// Propagate error counts from the temporary remote_pool back to the original cluster pool,
    /// so that subsequent queries can deprioritize replicas that failed during this analysis.
    void propagateErrorCounts()
    {
#if defined(OS_LINUX)
        if (hedged_factory.has_value())
        {
            /// Reset the factory so its destructor propagates error counts into remote_pool via updateSharedError.
            hedged_factory.reset();

            /// Copy error counts from remote_pool back into remote_pools entries
            /// (which still have the original indices valid for original_pool).
            auto statuses = remote_pool->getStatus();
            for (size_t i = 0; i < remote_replicas; ++i)
            {
                remote_pools[i].error_count = statuses[i].error_count;
                remote_pools[i].slowdown_count = statuses[i].slowdown_count;
            }
        }
#endif
        original_pool->updateSharedError(remote_pools);
    }

    /// Returns connections[remote_replicas], nullptr for unavailable.
    std::vector<Connection *> establishConnections(const ConnectionTimeouts & timeouts)
    {
        if (use_hedged_requests)
            return establishConnectionsAsync(timeouts);
        return establishConnectionsSync(timeouts);
    }

#if defined(OS_LINUX)
    std::vector<Connection *> establishConnectionsAsync(const ConnectionTimeouts & timeouts)
    {
        std::vector<Connection *> connections(remote_replicas, nullptr);

        ConnectionPoolPtrs failover_pools;
        failover_pools.reserve(remote_replicas);
        for (const auto & pool : remote_pools)
            failover_pools.push_back(pool.pool);
        /// LoadBalancing strategy does not matter, since we use replicaIndexPriorityFunc() later for HedgedConnectionsFactory
        remote_pool = std::make_shared<ConnectionPoolWithFailover>(std::move(failover_pools), LoadBalancing::NEAREST_HOSTNAME);

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

        /// Map connections back to replica indices via host:port.
        auto host_port = [](const auto & c) { return c.getHost() + ":" + std::to_string(c.getPort()); };

        std::unordered_map<std::string, size_t> address_to_replica;
        for (size_t i = 0; i < remote_replicas; ++i)
            address_to_replica[host_port(dynamic_cast<ConnectionPool &>(*remote_pools[i].pool))] = i;
        for (auto * conn : ready_connections)
        {
            if (auto it = address_to_replica.find(host_port(*conn)); it != address_to_replica.end())
                connections[it->second] = conn;
        }

        return connections;
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
                remote_replicas);
        return *sync_thread_pool;
    }

    std::vector<Connection *> establishConnectionsSync(const ConnectionTimeouts & timeouts)
    {
        std::vector<Connection *> connections(remote_replicas, nullptr);
        connection_entries.resize(remote_replicas);

        {
            ThreadPoolCallbackRunnerLocal<void> connect_runner(getThreadPool(), DB::ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
            for (size_t i = 0; i < remote_replicas; ++i)
            {
                connect_runner.enqueueAndKeepTrack([this, i, &timeouts]()
                {
                    try
                    {
                        /// NOTE: does not apply connections_with_failover_max_tries
                        connection_entries[i] = remote_pools[i].pool->get(timeouts, settings);
                    }
                    catch (...)
                    {
                        ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaUnavailable);
                        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
                        ++remote_pools[i].error_count;
                        tryLogCurrentException(logger, fmt::format("Cannot connect to {}. It will not participate in distributed index analysis", replica_addresses[i]), LogsLevel::warning);
                    }
                }, Priority{});
            }
            connect_runner.waitForAllToFinishAndRethrowFirstError();
        }

        for (size_t i = 0; i < remote_replicas; ++i)
        {
            if (!connection_entries[i].isNull())
                connections[i] = &*connection_entries[i];
        }

        return connections;
    }

    std::vector<size_t> selectActiveRemoteReplicas(const std::vector<Connection *> & connections) const
    {
        std::vector<size_t> active_remote_indexes;
        /// Reserve max_active_replicas - 1 because local always participates.
        active_remote_indexes.reserve(max_active_replicas - 1);
        for (size_t i = 0; i < remote_replicas && active_remote_indexes.size() + 1 < max_active_replicas; ++i)
        {
            if (connections[i] != nullptr)
                active_remote_indexes.push_back(i);
        }

        LOG_DEBUG(logger, "Distributed index analysis for {} (total replicas: {}, remote active: {} remote ({}))",
                  storage_id.getNameForLogs(),
                  total_replicas, active_remote_indexes.size(), fmt::join(active_remote_indexes, ", "));

        return active_remote_indexes;
    }

    struct DistributedParts
    {
        AssignedPartsRanges local_parts;
        size_t local_marks = 0;
        size_t local_rows = 0;
        std::vector<AssignedPartsRanges> remote_parts;
        std::vector<size_t> remote_marks;
        std::vector<size_t> remote_rows;
    };

    DistributedParts distributePartsToReplicas(const std::vector<size_t> & active_remote_indexes) const
    {
        DistributedParts result;
        result.remote_parts.resize(remote_replicas);
        result.remote_marks.resize(remote_replicas, 0);
        result.remote_rows.resize(remote_replicas, 0);

        /// Build a sorted list of original replica indices (from ShuffledPool::index)
        /// so that part distribution is deterministic regardless of which node is the initiator.
        std::vector<size_t> active_original_indexes;
        active_original_indexes.reserve(active_remote_indexes.size() + 1);
        active_original_indexes.push_back(local_original_index);
        for (const auto i : active_remote_indexes)
            active_original_indexes.push_back(remote_pools[i].index);
        std::sort(active_original_indexes.begin(), active_original_indexes.end());

        /// Reverse map: original index → remote_pools index (or local).
        std::unordered_map<size_t, size_t> original_to_remote;
        for (const auto i : active_remote_indexes)
            original_to_remote[remote_pools[i].index] = i;

        const bool split = split_segments;

        /// Accumulate ranges per (destination, part) so each part appears once per replica, and
        /// keep a lookup for computing marks/rows of the final assignment.
        std::unordered_map<std::string_view, MarkRanges> local_map;
        std::vector<std::unordered_map<std::string_view, MarkRanges>> remote_maps(remote_replicas);
        std::unordered_map<std::string_view, const RangesInDataPart *> part_by_name;

        auto destination = [&](size_t original_index) -> std::unordered_map<std::string_view, MarkRanges> &
        {
            if (original_index == local_original_index)
                return local_map;
            return remote_maps[original_to_remote[original_index]];
        };

        for (const auto & part_ranges : parts_with_ranges)
        {
            chassert(part_ranges.exact_ranges.empty());

            const std::string & part_name = part_ranges.data_part->name;
            part_by_name[part_name] = &part_ranges;

            if (!split || part_ranges.getMarksCount() <= min_marks_to_split_part)
            {
                /// Whole part on a single replica. Without splitting we send a bare name (empty
                /// ranges marker); with splitting the tuple wire form requires explicit ranges, and
                /// the full range keeps the remote on the whole-part path (stable full-index cache),
                /// same as the non-split behavior.
                const auto original_index = active_original_indexes[partReplica(part_name, active_original_indexes.size())];
                auto & ranges = destination(original_index)[part_name];
                if (split)
                    ranges.push_back(MarkRange{0, part_ranges.data_part->index_granularity->getMarksCountWithoutFinal()});
            }
            else
            {
                /// Split the part's ranges into mark segments aligned to the absolute grid
                /// (segment k covers [k*S, (k+1)*S) intersected with the input ranges) and hash each
                /// grid index independently. Grid alignment keeps the segment -> replica mapping
                /// stable across queries even when the input ranges differ (e.g. trimmed by the
                /// query condition cache). The partial primary-index cache key on the replica is
                /// built from the exact range boundaries though, so trimmed inputs produce
                /// distinct cache entries.
                for (const auto & range : part_ranges.ranges)
                {
                    for (size_t begin = range.begin; begin < range.end;)
                    {
                        const size_t segment_index = begin / mark_segment_size;
                        MarkRange segment{begin, std::min((segment_index + 1) * mark_segment_size, range.end)};
                        const auto original_index = active_original_indexes[partReplicaSegment(part_name, segment_index, active_original_indexes.size())];
                        destination(original_index)[part_name].push_back(segment);
                        begin = segment.end;
                    }
                }
            }
        }

        auto flatten = [&](std::unordered_map<std::string_view, MarkRanges> & map, AssignedPartsRanges & out, size_t & marks, size_t & rows)
        {
            for (auto & [name, ranges] : map)
            {
                const auto & part = *part_by_name.at(name);
                if (ranges.empty())
                {
                    marks += part.getMarksCount();
                    rows += part.getRowsCount();
                }
                else
                {
                    /// Segments are appended in increasing mark order; adjacent grid cells hashed
                    /// to the same replica merge into one range - the assignment is unchanged,
                    /// only the wire form shrinks (the replica coalesces its load ranges anyway).
                    ranges.coalesce();
                    marks += ranges.getNumberOfMarks();
                    rows += part.data_part->index_granularity->getRowsCountInRanges(ranges);
                }
                out.emplace_back(name, std::move(ranges));
            }
        };

        flatten(local_map, result.local_parts, result.local_marks, result.local_rows);
        for (size_t i = 0; i < remote_replicas; ++i)
        {
            size_t rows = 0;
            flatten(remote_maps[i], result.remote_parts[i], result.remote_marks[i], rows);
            result.remote_rows[i] = rows;
        }

        return result;
    }

    struct LocalAnalysisResult
    {
        ThreadFromGlobalPool thread;
        std::shared_ptr<std::exception_ptr> exception = std::make_shared<std::exception_ptr>();
    };

    LocalAnalysisResult executeLocalAnalysis(
        const AssignedPartsRanges & local_parts,
        size_t local_marks,
        size_t local_rows,
        DistributedIndexAnalysisReplicaResult & local_result)
    {
        LocalAnalysisResult result;
        if (!local_parts.empty())
        {
            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            auto exception_ptr = result.exception;
            result.thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup(), &local_parts, local_marks, local_rows, &local_result, exception_ptr]()
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
                try
                {
                    LOG_TRACE(logger, "Resolving {} parts ({} marks, {} rows) from local replica {}: {}",
                        local_parts.size(), local_marks, local_rows, local_address, local_parts);
                    auto parts_ranges = local_index_analysis_callback(local_parts);
                    LOG_TRACE(logger, "Received {} parts from local replica {}: {}",
                        parts_ranges.size(), local_address, parts_ranges);
                    local_result.parts_ranges = std::move(parts_ranges);
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
        const std::vector<size_t> & active_remote_indexes,
        const std::vector<Connection *> & connections,
        const std::vector<AssignedPartsRanges> & remote_parts,
        const std::vector<size_t> & remote_marks,
        const std::vector<size_t> & remote_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        if (use_async_reading)
            executeRemoteAnalysisAsync(active_remote_indexes, connections, remote_parts, remote_marks, remote_rows, res);
        else
            executeRemoteAnalysisSync(active_remote_indexes, connections, remote_parts, remote_marks, remote_rows, res);
    }

#if defined(OS_LINUX)
    void executeRemoteAnalysisAsync(
        const std::vector<size_t> & active_remote_indexes,
        const std::vector<Connection *> & connections,
        const std::vector<AssignedPartsRanges> & remote_parts,
        const std::vector<size_t> & remote_marks,
        const std::vector<size_t> & remote_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        struct ReplicaReader
        {
            std::shared_ptr<RemoteQueryExecutor> executor;
            size_t remote_index;
            bool query_sent = false;
        };

        auto sample_block = indexAnalysisSampleBlock();
        Epoll readers_epoll;
        std::vector<ReplicaReader> readers;
        std::unordered_map<int, size_t> fd_to_reader;

        for (const auto i : active_remote_indexes)
        {
            auto * connection = connections[i];
            if (!connection || remote_parts[i].empty())
                continue;

            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            auto query = buildAnalyzeIndexQuery(storage_id, filter_query, vector_search_parameters, remote_parts[i], split_segments);
            auto executor = std::make_shared<RemoteQueryExecutor>(*connection, query, sample_block, execution_context, ThrottlerPtr{}, Scalars{}, external_tables);
            executor->setLogger(logger);

            LOG_TRACE(logger, "Sending {} parts ({} marks, {} rows) to {}: {}",
                remote_parts[i].size(), remote_marks[i], remote_rows[i], replica_addresses[i], remote_parts[i]);

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

        std::exception_ptr cancellation_exception;

        auto process_reader = [&](size_t reader_index)
        {
            auto & reader = readers[reader_index];
            const auto i = reader.remote_index;
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
                        parseIndexAnalysisBlock(std::move(block), res[i].parts_ranges);
                        continue;
                    }

                    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Unexpected result type: {}", static_cast<int>(result.getType()));
                }

                LOG_TRACE(logger, "Received {} parts from {}: {}", res[i].parts_ranges.size(), replica_addresses[i], res[i].parts_ranges);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                {
                    /// Do not throw right now, to properly finish other replicas, to preserve connection
                    if (!cancellation_exception)
                        cancellation_exception = std::current_exception();
                    return;
                }
                ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica. They will be analyzed on initiator", replica_addresses[i]), LogsLevel::warning);
            }
            catch (...)
            {
                ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica. They will be analyzed on initiator", replica_addresses[i]), LogsLevel::warning);
            }
        };

        for (size_t ri = 0; ri < readers.size(); ++ri)
        {
            if (readers[ri].query_sent)
                process_reader(ri);
        }

        while (!readers_epoll.empty())
        {
            epoll_event event{};
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

        if (cancellation_exception)
            std::rethrow_exception(cancellation_exception);
    }
#else
    void executeRemoteAnalysisAsync(
        const std::vector<size_t> &,
        const std::vector<Connection *> &,
        const std::vector<AssignedPartsRanges> &,
        const std::vector<size_t> &,
        const std::vector<size_t> &,
        DistributedIndexAnalysisPartsRanges &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Async remote analysis is not supported on this platform");
    }
#endif

    void executeRemoteAnalysisSync(
        const std::vector<size_t> & active_remote_indexes,
        const std::vector<Connection *> & connections,
        const std::vector<AssignedPartsRanges> & remote_parts,
        const std::vector<size_t> & remote_marks,
        const std::vector<size_t> & remote_rows,
        DistributedIndexAnalysisPartsRanges & res)
    {
        ThreadPoolCallbackRunnerLocal<void> runner(getThreadPool(), DB::ThreadName::DISTRIBUTED_INDEX_ANALYSIS);
        for (const auto i : active_remote_indexes)
        {
            auto * connection = connections[i];
            if (!connection || remote_parts[i].empty())
                continue;

            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
            runner.enqueueAndKeepTrack([this, i, connection, &remote_parts, &remote_marks, &remote_rows, &res]()
            {
                try
                {
                    LOG_TRACE(logger, "Sending {} parts ({} marks, {} rows) to {}: {}",
                        remote_parts[i].size(), remote_marks[i], remote_rows[i], replica_addresses[i], remote_parts[i]);
                    auto parts_ranges = getIndexAnalysisFromReplicaSync(logger, storage_id, filter_query, vector_search_parameters, execution_context, external_tables, remote_parts[i], split_segments, *connection);
                    LOG_TRACE(logger, "Received {} parts from {}: {}", parts_ranges.size(), replica_addresses[i], parts_ranges);
                    res[i].parts_ranges = std::move(parts_ranges);
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                        throw;
                    ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                    tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica. They will be analyzed on initiator", replica_addresses[i]), LogsLevel::warning);
                }
                catch (...)
                {
                    ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisReplicaFallback);
                    tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica. They will be analyzed on initiator", replica_addresses[i]), LogsLevel::warning);
                }
            }, Priority{});
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    void resolveMissingParts(
        const DistributedParts & distributed_parts,
        DistributedIndexAnalysisReplicaResult & local_result,
        const DistributedIndexAnalysisPartsRanges & remote_results) const
    {
        /// Detection must be per (replica, part), not per part: with segment splitting one part is
        /// assigned to several replicas, and a successful replica returns every part it was
        /// assigned (even with empty ranges). A part absent from its replica's result therefore
        /// means that replica failed, and exactly its assigned ranges must be re-analyzed locally
        /// (empty assigned ranges denote the whole part).
        std::unordered_map<std::string_view, MarkRanges> missing;
        for (size_t i = 0; i < remote_replicas; ++i)
        {
            const auto & replica_result = remote_results[i].parts_ranges;
            for (const auto & [part, assigned_ranges] : distributed_parts.remote_parts[i])
            {
                if (replica_result.contains(std::string(part)))
                    continue;

                auto [it, inserted] = missing.try_emplace(part, assigned_ranges);
                if (!inserted)
                {
                    /// Whole-part (empty) assignment subsumes any concrete ranges.
                    if (assigned_ranges.empty() || it->second.empty())
                        it->second.clear();
                    else
                        it->second.insert(it->second.end(), assigned_ranges.begin(), assigned_ranges.end());
                }
            }
        }

        AssignedPartsRanges missing_parts;
        size_t missing_parts_marks = 0;
        size_t missing_parts_rows = 0;
        std::unordered_map<std::string_view, const RangesInDataPart *> part_by_name;
        for (const auto & part_ranges : parts_with_ranges)
            part_by_name[part_ranges.data_part->name] = &part_ranges;

        for (auto & [part, ranges] : missing)
        {
            /// Segments collected from different failed replicas are disjoint but unordered
            /// (MarkRange::operator< also validates the disjointness).
            std::sort(ranges.begin(), ranges.end());
            ranges.coalesce();

            const auto & part_info = *part_by_name.at(part);
            if (ranges.empty())
            {
                missing_parts_marks += part_info.getMarksCount();
                missing_parts_rows += part_info.getRowsCount();
            }
            else
            {
                missing_parts_marks += ranges.getNumberOfMarks();
                missing_parts_rows += part_info.data_part->index_granularity->getRowsCountInRanges(ranges);
            }
            missing_parts.emplace_back(part, std::move(ranges));
        }

        if (!missing_parts.empty())
        {
            ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisMissingParts);

            LOG_TRACE(logger, "Resolving {} missing parts ({} marks, {} rows) from local replica {}: {}", missing_parts.size(), missing_parts_marks, missing_parts_rows, local_address, missing_parts);
            auto parts_ranges = local_index_analysis_callback(missing_parts);
            LOG_TRACE(logger, "Received {} missing parts from local replica {}: {}", parts_ranges.size(), local_address, parts_ranges);

            local_result.assigned_parts += missing_parts.size();
            local_result.assigned_marks += missing_parts_marks;

            /// A failed replica may hold only SOME segments of a part while another replica already
            /// returned the rest, so merge ranges per part instead of insert (which skips existing keys).
            for (auto & [part_name, ranges] : parts_ranges)
            {
                auto & dst = local_result.parts_ranges[part_name];
                dst.insert(dst.end(), std::make_move_iterator(ranges.begin()), std::make_move_iterator(ranges.end()));
            }
        }
    }

    const StorageID & storage_id;
    const RangesInDataParts & parts_with_ranges;
    const OptionalVectorSearchParameters & vector_search_parameters;
    size_t mark_segment_size;
    size_t min_marks_to_split_part;
    bool split_segments;
    LocalIndexAnalysisCallback local_index_analysis_callback;
    ContextPtr context;

    LoggerPtr logger;
    const Settings & settings;
    bool use_hedged_requests = false;
    bool use_async_reading = false;
    size_t total_replicas;
    size_t remote_replicas;
    size_t max_active_replicas;

    std::string local_address;
    size_t local_original_index; /// Position among all replicas (for consistent hash distribution).
    /// Original pool from cluster, used to propagate error counts back.
    ConnectionPoolWithFailoverPtr original_pool;
    /// Combined pool with failover logic, used by `HedgedConnectionsFactory` for async connections.
    ConnectionPoolWithFailoverPtr remote_pool;
    /// Individual per-replica pools (local extracted), used by sync connections via `pool->get`, sorted by replica index.
    std::vector<ShuffledPool> remote_pools;
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
    ASTPtr sampling_filter,
    const NameSet & indexes_column_names,
    const RangesInDataParts & parts_with_ranges,
    const OptionalVectorSearchParameters & vector_search_parameters,
    size_t mark_segment_size,
    size_t min_marks_to_split_part,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::DistributedIndexAnalysisMicroseconds);

    auto context_copy = Context::createCopy(context);
    /// Disable parallel replicas to avoid O(N^2) spawned queries when the predicate has a subquery,
    /// because each replica would independently spawn parallel replicas for the subquery.
    context_copy->setSetting("enable_parallel_replicas", false);

    DistributedIndexAnalyzer analyzer(
        storage_id,
        filter_actions_dag,
        std::move(sampling_filter),
        indexes_column_names,
        parts_with_ranges,
        vector_search_parameters,
        mark_segment_size,
        min_marks_to_split_part,
        std::move(local_index_analysis_callback),
        std::move(context_copy));

    return analyzer.run();
}

}
