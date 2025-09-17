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
#include <Core/Settings.h>
#include <Common/CurrentMetrics.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Columns/ColumnString.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/ranges.h>
#include <consistent_hashing.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
}

namespace DB::Setting
{
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsBool use_hedged_requests;
}

namespace ProfileEvents
{
    extern const Event DistributedIndexAnalysisMicroseconds;
    extern const Event DistributedIndexAnalysisFailedReplicas;
    extern const Event DistributedIndexAnalysisMissingParts;
    extern const Event DistributedIndexAnalysisScheduledReplicas;
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

ContextMutablePtr updateContext(ContextPtr orignal_context)
{
    auto context = Context::createCopy(orignal_context);

    Settings new_settings = context->getSettingsCopy();
    new_settings[Setting::max_result_rows] = 0;
    new_settings[Setting::max_result_bytes] = 0;
    /// FIXME: Do we even need this?
    new_settings[Setting::use_hedged_requests] = false;
    context->setSettings(new_settings);

    return context;
}

size_t partReplica(const std::string & part_name, size_t replicas_count)
{
    auto hash = SipHash();
    hash.update(part_name);
    return ConsistentHashing(hash.get64(), replicas_count);
}

size_t findLocalReplica(std::vector<ConnectionPoolPtr> & pools, const Cluster::Addresses & local_addresses)
{
    std::optional<size_t> local_replica_index;
    for (size_t i = 0, s = pools.size(); i < s; ++i)
    {
        const auto & hostname = pools[i]->getHost();
        const auto & port = pools[i]->getPort();
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

std::vector<ConnectionPoolPtr> prepareConnectionPools(const ContextPtr & context, const Cluster::ShardInfo & shard)
{
    /// Try to preserve replicas order as in cluster definition.
    /// It's important for data locality during query execution independently of the query initiator.
    ///
    /// NOTE: There can be a problem if the cluster definition will be updated between
    /// distributed index analysis and initiating reading from parallel replicas.
    auto priority_func = [](size_t i) { return Priority{static_cast<Int64>(i)}; };
    auto shuffled_pool = shard.pool->getShuffledPools(context->getSettingsRef(), priority_func);

    std::vector<ConnectionPoolPtr> pools_to_use;
    pools_to_use.reserve(shuffled_pool.size());
    for (auto & pool : shuffled_pool)
        pools_to_use.emplace_back(std::move(pool.pool));

    return pools_to_use;
}

IndexAnalysisPartsRanges getIndexAnalysisFromReplica(const LoggerPtr & logger, const StorageID & storage_id, const std::string & filter, ContextPtr context, const std::vector<std::string_view> & parts, ConnectionPoolPtr pool)
{
    const auto analyze_index_query = fmt::format("SELECT * FROM mergeTreeAnalyzeIndexUUID('{}', '^({})$', {})",
        storage_id.uuid,
        fmt::join(parts, "|"),
        filter);

    IndexAnalysisPartsRanges res;

    auto sample_block = std::make_shared<const Block>(Block
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

    const auto & settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    auto connection = pool->get(timeouts, settings);

    RemoteQueryExecutor executor(*connection, analyze_index_query, sample_block, context);
    executor.setLogger(logger);
    for (Block current = executor.readBlock(); !current.empty(); current = executor.readBlock())
    {
        current = convertBLOBColumns(current);

        const auto & col_part_name = assert_cast<const ColumnString &>(*current.getByName("part_name").column);
        const auto & col_ranges_array = assert_cast<const ColumnArray &>(*current.getByName("ranges").column);
        const auto & col_ranges_array_offsets = col_ranges_array.getOffsets();
        const auto & col_ranges_tuple = assert_cast<const ColumnTuple &>(col_ranges_array.getData());
        const auto & col_range_start = assert_cast<const ColumnUInt64 &>(col_ranges_tuple.getColumn(0)).getData();
        const auto & col_range_end = assert_cast<const ColumnUInt64 &>(col_ranges_tuple.getColumn(1)).getData();

        for (size_t i = 0; i < col_part_name.size(); ++i)
        {
            auto & ranges_dst = res[col_part_name.getDataAt(i).toString()];
            for (size_t range_i = col_ranges_array_offsets[i - 1]; range_i < col_ranges_array_offsets[i]; ++range_i)
                ranges_dst.push_back(MarkRange{col_range_start[range_i], col_range_end[range_i]});
        }
    }
    executor.finish();

    return res;
}

ASTPtr getQueryWithNonQualifiedIdentifiers(const QueryTreeNodePtr & query_tree)
{
    ASTPtr result_ast = query_tree->toAST(ConvertToASTOptions{
        .fully_qualified_identifiers = false,
    });

    while (true)
    {
        if (auto * /*select_query*/ _ = result_ast->as<ASTSelectQuery>())
            break;
        if (auto * select_with_union = result_ast->as<ASTSelectWithUnionQuery>())
            result_ast = select_with_union->list_of_selects->children.at(0);
        else if (auto * subquery = result_ast->as<ASTSubquery>())
            result_ast = subquery->children.at(0);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query node invalid conversion to select query");
    }

    if (result_ast == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query node invalid conversion to select query");

    return result_ast;
}


}

namespace DB
{

DistributedIndexAnalysisPartsRanges distributedIndexAnalysisOnReplicas(
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const RangesInDataParts & parts_with_ranges,
    LocalIndexAnalysisCallback local_index_analysis_callback,
    ContextPtr context)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::DistributedIndexAnalysisMicroseconds);
    context = updateContext(context);

    const auto & settings = context->getSettingsRef();
    auto logger = getLogger("DistributedIndexAnalysis");
    LOG_DEBUG(logger, "Distributed index analysis for {}", storage_id.getNameForLogs());

    auto cluster = context->getClusterForParallelReplicas();
    const auto & shard = cluster->getShardsInfo().at(0);
    auto connection_pools = prepareConnectionPools(context, shard);
    size_t local_replica_index = findLocalReplica(connection_pools, shard.local_addresses);
    size_t replicas = std::min<size_t>(settings[Setting::max_parallel_replicas], shard.getAllNodeCount());

    chassert(replicas <= connection_pools.size());
    connection_pools.resize(replicas);

    std::vector<std::vector<std::string_view>> replicas_parts;
    replicas_parts.resize(replicas);

    std::vector<size_t> replicas_marks;
    replicas_marks.resize(replicas);
    std::vector<size_t> replicas_rows;
    replicas_rows.resize(replicas);

    for (const auto & part_ranges : parts_with_ranges)
    {
        chassert(part_ranges.ranges.size() == 1);
        chassert(part_ranges.exact_ranges.empty());

        const auto & part_name = part_ranges.data_part->name;
        const auto & part_replica_index = partReplica(part_name, replicas);
        replicas_parts[part_replica_index].push_back(part_name);

        replicas_marks[part_replica_index] += part_ranges.getMarksCount();
        replicas_rows[part_replica_index] += part_ranges.getRowsCount();
    }

    DistributedIndexAnalysisPartsRanges res;
    res.resize(replicas);

    ASTs filter_asts;
    ASTPtr query = getQueryWithNonQualifiedIdentifiers(query_info.query_tree);
    const auto & select_query = query->as<ASTSelectQuery &>();
    if (auto prewhere = select_query.prewhere())
        filter_asts.push_back(prewhere);
    if (auto where = select_query.where())
        filter_asts.push_back(where);
    auto filter_ast = makeASTForLogicalAnd(std::move(filter_asts));
    auto filter_query = filter_ast->formatWithSecretsOneLine();

    ThreadPool pool(CurrentMetrics::DistributedIndexAnalysisThreads,
                    CurrentMetrics::DistributedIndexAnalysisThreadsActive,
                    CurrentMetrics::DistributedIndexAnalysisThreadsScheduled,
                    /// TODO: limit amount of threads (maybe shared thread pool)
                    replicas_parts.size());
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "DistIdxAnalysis");
    for (size_t i = 0; i < replicas_parts.size(); ++i)
    {
        const auto & replica_parts = replicas_parts[i];
        const auto & connection_pool = connection_pools.at(i);
        const auto & replica_address = connection_pool->getAddress();

        if (replica_parts.empty())
            continue;

        ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisScheduledReplicas);
        if (i == local_replica_index)
        {
            runner([&, i, replica_address]()
            {
                LOG_TRACE(logger, "Resolving {} parts ({} marks, {} rows) from local replica {} (index {}): {}", replica_parts.size(), replicas_marks[i], replicas_rows[i], replica_address, i, replica_parts);
                auto parts_ranges = local_index_analysis_callback(replica_parts);
                LOG_TRACE(logger, "Received {} parts from local replica {} (index {}): {}", parts_ranges.size(), replica_address, i, parts_ranges);
                res[i] = std::make_pair(replica_address, std::move(parts_ranges));
            }, Priority{});
        }
        else
        {
            runner([&, i, replica_address, connection_pool]()
            {
                try
                {
                    LOG_TRACE(logger, "Sending {} parts ({} marks, {} rows) to {} (index {}): {}", replica_parts.size(), replicas_marks[i], replicas_rows[i], replica_address, i, replica_parts);
                    auto parts_ranges = getIndexAnalysisFromReplica(logger, storage_id, filter_query, context, replica_parts, connection_pool);
                    LOG_TRACE(logger, "Received {} parts from {} (index {}): {}", parts_ranges.size(), replica_address, i, parts_ranges);
                    res[i] = std::make_pair(replica_address, std::move(parts_ranges));
                }
                catch (...)
                {
                    ProfileEvents::increment(ProfileEvents::DistributedIndexAnalysisFailedReplicas);
                    /// Ignore any exceptions, everything will be analyzed on a local replica
                    tryLogCurrentException(logger, fmt::format("Cannot analyze parts on {} replica (index {}). They will be analyzed on initiator", replica_address, i), LogsLevel::warning);
                }
            }, Priority{});
        }
    }
    runner.waitForAllToFinishAndRethrowFirstError();

    /// Resolve leftovers
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

        const auto & local_replica_address = connection_pools[local_replica_index]->getAddress();
        LOG_TRACE(logger, "Resolving {} missing parts ({} marks, {} rows) from local replica {} (index {}): {}", missing_parts.size(), missing_parts_marks, missing_parts_rows, local_replica_address, local_replica_index, missing_parts);
        auto parts_ranges = local_index_analysis_callback(missing_parts);
        LOG_TRACE(logger, "Received {} missing parts from local replica {} (index {}): {}", parts_ranges.size(), local_replica_address, local_replica_index, parts_ranges);
        res[local_replica_index].first = local_replica_address;
        res[local_replica_index].second.insert_range(std::move(parts_ranges));
    }

    /// And now we can remove empty ranges
    for (auto & [_, parts_ranges] : res)
        std::erase_if(parts_ranges, [&](const auto & ranges) { return ranges.second.empty(); });

    return res;
}

}
