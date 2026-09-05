#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Client/ConnectionPool.h>
#include <Columns/ColumnBLOB.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Parsers/parseQuery.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <ranges>
#include <set>


namespace DB
{

namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsBool insert_distributed_one_random_shard;
    extern const SettingsUInt64 insert_shard_id;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace DistributedSetting
{
    extern const DistributedSettingsBool skip_unavailable_shards;
    extern const DistributedSettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_TABLE_ENGINE;
}

std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage)
{
    const auto * distributed = typeid_cast<const StorageDistributed *>(&storage);
    if (!distributed)
    {
        if (typeid_cast<const StorageTimeSeries *>(&storage))
            return {};

        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation can be executed on a TimeSeries table or on a Distributed table over TimeSeries tables only, "
            "the engine of table {} is not TimeSeries",
            storage.getStorageID().getNameForLogs());
    }

    PrometheusQueryDistributedTarget target;

    /// getClusterName() returns "<remote>" for ENGINE=Remote(), which is not a name we can pass to cluster().
    target.cluster_name = distributed->getClusterName();
    if (target.cluster_name == "<remote>")
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "This operation is not supported over table {} because it has no cluster name: "
            "a prometheus query over a Distributed table requires a cluster defined in the server configuration",
            storage.getStorageID().getNameForLogs());

    target.remote_time_series_storage_id.database_name = distributed->getRemoteDatabaseName();
    target.remote_time_series_storage_id.table_name = distributed->getRemoteTableName();
    /// Restated on the generated cluster() call as its own declaration, so ClusterProxy applies its usual precedence.
    const auto & declared = distributed->getDistributedSettingsRef();
    target.skip_unavailable_shards = declared[DistributedSetting::skip_unavailable_shards].value;
    target.skip_unavailable_shards_mode = declared[DistributedSetting::skip_unavailable_shards_mode].toString();
    return target;
}

namespace
{
bool hasInstantSelector(const PrometheusQueryTree::Node * node)
{
    return node
        && (node->node_type == PrometheusQueryTree::NodeType::InstantSelector || std::ranges::any_of(node->children, hasInstantSelector));
}
}

bool prometheusQueryReadsTimeSeries(const PrometheusQueryTree & promql_query)
{
    /// A range selector carries an instant selector as its child, so one node type covers both.
    return hasInstantSelector(promql_query.getRoot());
}

namespace
{
    /// Parsed as the planner parses it: a literal true, which isAlwaysTrue() exempts for a row policy, restricts nothing.
    bool isRestrictiveFilter(const String & filter, const ContextPtr & context)
    {
        if (filter.empty())
            return false;
        const auto & settings = context->getSettingsRef();
        ParserExpression parser;
        const auto ast = parseQuery(
            parser, filter.data(), filter.data() + filter.size(), "additional filter",
            settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        bool value = false;
        return !(tryGetLiteralBool(ast.get(), value) && value);
    }

    /// Asks every replica itself, not one per shard as cluster() would, and afresh on every request: a verdict kept
    /// for later would let a same-schema table swapped in under the name meanwhile take a write unchecked.
    void checkShardTargets(
        const IStorage & storage, const PrometheusQueryDistributedTarget & target, const ContextPtr & context, bool refuse_unavailable)
    {
        const auto & remote_id = target.remote_time_series_storage_id;
        const auto cluster = typeid_cast<const StorageDistributed &>(storage).getCluster();
        const auto metadata = storage.getInMemoryMetadataPtr(context, false);
        const auto time_series_type = metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type->getName();

        /// An undeclared database is each replica's own default, except on a replica that is this server itself: the
        /// read and the write pin prefer_localhost_replica on and parallel replicas off, so it runs on this context.
        auto make_probe_query = [&](bool runs_on_the_caller)
        {
            const String database_predicate = !remote_id.database_name.empty()
                ? quoteString(remote_id.database_name)
                : (runs_on_the_caller ? quoteString(context->getCurrentDatabase()) : "currentDatabase()");
            const String table_predicate = quoteString(remote_id.table_name);
            return "SELECT (SELECT engine FROM system.tables WHERE database = " + database_predicate + " AND name = " + table_predicate
                + ") AS engine, (SELECT type FROM system.columns WHERE database = " + database_predicate + " AND table = " + table_predicate
                + " AND name = " + quoteString(TimeSeriesColumnNames::TimeSeries) + ") AS ts_type";
        };
        const auto nullable_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        const auto probe_header = std::make_shared<const Block>(Block{{nullable_string, "engine"}, {nullable_string, "ts_type"}});

        /// On the server's own context, and only ever after the caller's own grants are checked: a read
        /// requires READ ON REMOTE above, so the probe reports nothing the caller's own cluster() could not.
        auto probe_context = Context::createCopy(context->getGlobalContext());
        probe_context->makeQueryContext();
        probe_context->setCurrentQueryId("");
        /// An unreachable replica then answers nothing rather than failing the probe; judged below.
        probe_context->setSetting("skip_unavailable_shards", true);

        UInt64 wrong_engine_replicas = 0;
        UInt64 wrong_type_replicas = 0;
        std::set<String> wrong_types;
        /// Unreachable, or without the table: the sink cannot use them either, and what answers to the name later is unchecked.
        Strings unavailable_replicas;
        for (const auto [shard_info, shard_addresses] : std::views::zip(cluster->getShardsInfo(), cluster->getShardsAddresses()))
            for (const auto [pool, address] : std::views::zip(shard_info.per_replica_pools, shard_addresses))
            {
                RemoteQueryExecutor probe(pool, make_probe_query(address.is_local), probe_header, probe_context);
                bool answered = false;
                try
                {
                    for (Block block = probe.readBlock(); !block.empty(); block = probe.readBlock())
                    {
                        block = convertBLOBColumns(block);
                        answered = true;
                        const Field engine = (*block.getByPosition(0).column)[0];
                        const Field ts_type = (*block.getByPosition(1).column)[0];
                        if (engine.isNull())
                            unavailable_replicas.push_back(
                                fmt::format("{} (no table {})", pool->getAddress(), backQuoteIfNeed(remote_id.table_name)));
                        else if (engine.safeGet<String>() != "TimeSeries")
                            ++wrong_engine_replicas;
                        /// Not exposed to the probe, or not there at all: the type went unchecked either way.
                        else if (ts_type.isNull())
                            unavailable_replicas.push_back(fmt::format(
                                "{} (no `{}` column on {})", pool->getAddress(), TimeSeriesColumnNames::TimeSeries,
                                backQuoteIfNeed(remote_id.table_name)));
                        else if (ts_type.safeGet<String>() != time_series_type)
                        {
                            ++wrong_type_replicas;
                            wrong_types.insert(ts_type.safeGet<String>());
                        }
                    }
                }
                catch (const NetException &)
                {
                    /// A pooled connection to a replica that went away unnoticed fails on first use, not when handed out.
                    answered = false;
                }
                if (!answered)
                    unavailable_replicas.push_back(fmt::format("{} (unreachable)", pool->getAddress()));
            }

        if (wrong_engine_replicas)
            throw Exception(
                ErrorCodes::UNEXPECTED_TABLE_ENGINE,
                "This operation is not supported over table {}: {} shard-local target(s) named {} are not TimeSeries tables",
                storage.getStorageID().getNameForLogs(), wrong_engine_replicas, backQuoteIfNeed(remote_id.table_name));

        if (wrong_type_replicas)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "This operation is not supported over table {}: {} shard-local target(s) named {} declare `{}` as {} "
                "while the table declares {}",
                storage.getStorageID().getNameForLogs(), wrong_type_replicas, backQuoteIfNeed(remote_id.table_name),
                TimeSeriesColumnNames::TimeSeries, fmt::join(wrong_types, ", "), time_series_type);

        /// A replica the check could not see would take the samples unchecked.
        if (refuse_unavailable && !unavailable_replicas.empty())
            throw Exception(
                ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
                "Remote write over table {} is refused while it has no verified target on {}: retry once it has one",
                storage.getStorageID().getNameForLogs(), fmt::join(unavailable_replicas, ", "));
    }
}

void checkNoBypassedReadRestriction(
    const StorageID & storage_id, const ContextPtr & context, std::string_view operation, std::string_view rewrite)
{
    auto row_policy_filter
        = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "{} is not supported on table {} while a row policy applies to it: {} and the policy would not be applied",
            operation, storage_id.getNameForLogs(), rewrite);

    /// Matched the way the planner matches filter keys: the short name only from the same current
    /// database, the full unquoted name from anywhere.
    for (const auto & filter_entry : context->getSettingsRef()[Setting::additional_table_filters].value)
    {
        const auto & name_and_filter = filter_entry.safeGet<Tuple>();
        const auto & filtered_table = name_and_filter.at(0).safeGet<String>();
        bool matches = (filtered_table == storage_id.getTableName() && context->getCurrentDatabase() == storage_id.getDatabaseName())
            || filtered_table == storage_id.getFullNameNotQuoted();
        if (matches && isRestrictiveFilter(name_and_filter.at(1).safeGet<String>(), context))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "{} is not supported on table {} with an additional_table_filters entry for it: {} and the filter would not be applied",
                operation, storage_id.getNameForLogs(), rewrite);
    }
}

void checkPrometheusQueryDistributedRead(const IStorage & storage, const ContextPtr & context)
{
    /// The planner never sees the wrapper (the rewrite hands it a cluster() call), so its SELECT grant
    /// is checked explicitly: again here, before a probe that runs on the server's own context.
    const auto storage_id = storage.getStorageID();
    context->checkAccess(AccessType::SELECT, storage_id);
    const auto target = resolvePrometheusQueryTarget(storage);
    if (!target)
        return;

    /// A plain SELECT through the wrapper applies both; the generated read never names the wrapper.
    /// The shard-local table's own policy and filters are each shard's to check, in the selector.
    checkNoBypassedReadRestriction(
        storage_id, context, "A prometheus query over a Distributed table", "the read is rewritten to the shard-local TimeSeries tables");

    /// Grant before existence: the probe below runs on the server's own context, so the grant the generated
    /// cluster() call enforces only later is required here, before it can report on a shard-local target.
    context->checkAccess(AccessType::READ, AccessTypeObjects::toStringSource(AccessTypeObjects::Source::REMOTE));

    /// The read pins prefer_localhost_replica on and parallel replicas off, so a shard that is this server itself
    /// runs in-process on the caller's context: the selector's own grants are asked for here, before the probe.
    if (typeid_cast<const StorageDistributed &>(storage).getCluster()->getLocalShardCount())
    {
        context->checkAccess(AccessType::SELECT, context->resolveStorageID(target->remote_time_series_storage_id));
        context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE);
    }

    /// Whether an unavailable replica fails the read is the read's own decision, as for any cluster() call.
    checkShardTargets(storage, *target, context, /* refuse_unavailable = */ false);
}

void checkPrometheusQueryDistributedWrite(const IStorage & storage, const ContextPtr & context)
{
    const auto target = resolvePrometheusQueryTarget(storage);
    if (!target)
        return;

    /// The sink honours both as for any INSERT (the second only without a key); here a batch goes where the key sends it or nowhere.
    const auto & settings = context->getSettingsRef();
    if (settings[Setting::insert_shard_id] || settings[Setting::insert_distributed_one_random_shard])
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Remote write over table {} does not accept insert_shard_id or insert_distributed_one_random_shard: "
            "samples are routed by the table's sharding key alone",
            storage.getStorageID().getNameForLogs());

    checkShardTargets(storage, *target, context, /* refuse_unavailable = */ true);
}

}
