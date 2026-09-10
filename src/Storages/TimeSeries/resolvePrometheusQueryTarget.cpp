#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Client/ConnectionPool.h>
#include <Columns/ColumnBLOB.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
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
    extern const SettingsBool insert_allow_materialized_columns;
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
    extern const int ACCESS_DENIED;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int INCOMPATIBLE_SCHEMA;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_TABLE_ENGINE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
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

    /// The engine a replica's own DDL names, empty when it names none.
    String engineOfCreateQuery(const String & create_query, const ContextPtr & context)
    {
        const auto & settings = context->getSettingsRef();
        ParserCreateQuery parser;
        const auto ast = parseQuery(
            parser, create_query.data(), create_query.data() + create_query.size(), "SHOW CREATE TABLE",
            settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        const auto * create = ast->as<ASTCreateQuery>();
        return create && create->storage && create->storage->engine ? create->storage->engine->name : "";
    }

    /// Asks every replica itself, not one per shard as cluster() would, and afresh on every request: a verdict kept
    /// for later would let a same-schema table swapped in under the name meanwhile take a write unchecked.
    void checkShardTargets(
        const IStorage & storage,
        const PrometheusQueryDistributedTarget & target,
        const ContextPtr & context,
        const ClusterPtr & cluster,
        bool for_write)
    {
        const auto & remote_id = target.remote_time_series_storage_id;
        const auto metadata = storage.getInMemoryMetadataPtr(context, false);
        const auto time_series_type = metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type->getName();

        /// An undeclared database is each replica's own default, as it is for the read and the write themselves.
        const String qualified_name = remote_id.database_name.empty()
            ? backQuoteIfNeed(remote_id.table_name)
            : backQuoteIfNeed(remote_id.database_name) + "." + backQuoteIfNeed(remote_id.table_name);
        const auto string_type = std::make_shared<DataTypeString>();
        const auto create_header = std::make_shared<const Block>(Block{{string_type, "statement"}});
        const auto describe_header = std::make_shared<const Block>(Block{{string_type, "name"}, {string_type, "type"}});

        /// On the server's own context, and only ever after the caller's own grants are checked: a read
        /// requires READ ON REMOTE above, so the probe reports nothing the caller's own cluster() could not.
        auto probe_context = Context::createCopy(context->getGlobalContext());
        probe_context->makeQueryContext();
        probe_context->setCurrentQueryId("");
        /// An unreachable replica then answers nothing rather than failing the probe; a missing table is still
        /// an exception, which the caller of a shard cannot be left to discover for itself.
        probe_context->setSetting("skip_unavailable_shards", true);
        probe_context->setSetting("skip_unavailable_shards_mode", String("unavailable"));
        /// The type must come back as the column declares it, and only the table's own columns.
        probe_context->setSetting("describe_include_subcolumns", false);
        probe_context->setSetting("describe_include_virtual_columns", false);
        probe_context->setSetting("print_pretty_type_names", false);

        UInt64 wrong_engine_replicas = 0;
        UInt64 wrong_type_replicas = 0;
        std::set<String> wrong_types;
        /// Unreachable, or without the table: the sink cannot use them either, and what answers to the name later is unchecked.
        Strings unavailable_replicas;
        auto judge = [&](const String & replica, const String & engine, const String & ts_type, const String & unavailable)
        {
            if (!unavailable.empty())
                unavailable_replicas.push_back(fmt::format("{} ({})", replica, unavailable));
            /// A replica that answered but names no engine of its own holds a view or a dictionary.
            else if (engine != "TimeSeries")
                ++wrong_engine_replicas;
            /// Not exposed to the probe, or not there at all: the type went unchecked either way.
            else if (ts_type.empty())
                unavailable_replicas.push_back(fmt::format(
                    "{} (no `{}` column on {})", replica, TimeSeriesColumnNames::TimeSeries, backQuoteIfNeed(remote_id.table_name)));
            else if (ts_type != time_series_type)
            {
                ++wrong_type_replicas;
                wrong_types.insert(ts_type);
            }
        };

        /// One service query on the replica's own connection, block by block.
        auto ask = [&](const auto & pool, const String & query, const auto & header, auto && on_block)
        {
            RemoteQueryExecutor probe(pool, query, header, probe_context);
            for (Block block = probe.readBlock(); !block.empty(); block = probe.readBlock())
                on_block(convertBLOBColumns(block));
        };

        for (const auto [shard_info, shard_addresses] : std::views::zip(cluster->getShardsInfo(), cluster->getShardsAddresses()))
            for (const auto [pool, address] : std::views::zip(shard_info.per_replica_pools, shard_addresses))
            {
                /// A replica that is this server itself is read and written in-process on this context (both pin
                /// prefer_localhost_replica on, the read parallel replicas off), so its table is resolved here, as they will.
                if (address.is_local)
                {
                    String engine;
                    String ts_type;
                    String unavailable;
                    if (const auto table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(remote_id), context))
                    {
                        engine = table->getName();
                        const auto local_metadata = table->getInMemoryMetadataPtr(context, false);
                        if (const auto * column = local_metadata->columns.tryGet(TimeSeriesColumnNames::TimeSeries))
                            ts_type = column->type->getName();
                    }
                    else
                        unavailable = fmt::format("no table {}", backQuoteIfNeed(remote_id.table_name));
                    judge(pool->getAddress(), engine, ts_type, unavailable);
                    continue;
                }

                /// SHOW CREATE TABLE and DESC TABLE ask the replica for SHOW COLUMNS on that table alone, so the
                /// probe reads no `system` table of its own; a replica that will not answer even that is skipped below.
                String engine;
                String ts_type;
                String unavailable;
                bool answered = false;
                try
                {
                    ask(pool, "SHOW CREATE TABLE " + qualified_name, create_header, [&](const Block & block)
                    {
                        answered = true;
                        engine = engineOfCreateQuery((*block.getByPosition(0).column)[0].safeGet<String>(), context);
                    });
                    /// The DDL of a table attached from an older version can still name the outer columns it had then.
                    if (engine == "TimeSeries")
                        ask(pool, "DESC TABLE " + qualified_name, describe_header, [&](const Block & block)
                        {
                            const auto & names = *block.getByPosition(0).column;
                            const auto & types = *block.getByPosition(1).column;
                            for (size_t row = 0; row != names.size(); ++row)
                                if (names[row].safeGet<String>() == TimeSeriesColumnNames::TimeSeries)
                                    ts_type = types[row].safeGet<String>();
                        });
                }
                catch (const Exception & e)
                {
                    /// A column-level INSERT, all the write itself needs, does not carry the right to read metadata:
                    /// a replica that may not answer here is left to the check its own insert makes on its target.
                    if (e.code() == ErrorCodes::ACCESS_DENIED)
                        continue;
                    /// SHOW CREATE TABLE reports a name it cannot produce a DDL for as its own code, DESC as
                    /// the plain one; anything else leaves the target unverified, as an unreachable replica does.
                    const bool no_table = e.code() == ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY
                        || e.code() == ErrorCodes::UNKNOWN_TABLE || e.code() == ErrorCodes::UNKNOWN_DATABASE;
                    unavailable = no_table ? fmt::format("no table {}", backQuoteIfNeed(remote_id.table_name)) : "unreachable";
                }
                /// A connection the pool could not open is skipped rather than raised, so it answers nothing.
                if (unavailable.empty() && !answered)
                    unavailable = "unreachable";
                judge(pool->getAddress(), engine, ts_type, unavailable);
            }

        if (wrong_engine_replicas)
            throw Exception(
                ErrorCodes::UNEXPECTED_TABLE_ENGINE,
                "This operation is not supported over table {}: {} shard-local target(s) named {} are not TimeSeries tables",
                storage.getStorageID().getNameForLogs(), wrong_engine_replicas, backQuoteIfNeed(remote_id.table_name));

        if (wrong_type_replicas)
            throw Exception(
                /// A write must be refused with a retryable status, or Prometheus drops the batch it could resend.
                for_write ? ErrorCodes::INCOMPATIBLE_SCHEMA : ErrorCodes::TYPE_MISMATCH,
                "This operation is not supported over table {}: {} shard-local target(s) named {} declare `{}` as {} "
                "while the table declares {}",
                storage.getStorageID().getNameForLogs(), wrong_type_replicas, backQuoteIfNeed(remote_id.table_name),
                TimeSeriesColumnNames::TimeSeries, fmt::join(wrong_types, ", "), time_series_type);

        /// A replica the check could not see would take the samples unchecked.
        if (for_write && !unavailable_replicas.empty())
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
    const auto cluster = typeid_cast<const StorageDistributed &>(storage).getCluster();
    if (cluster->getLocalShardCount())
    {
        /// A name that resolves to nothing is left to the probe, which reports it as a target the read has not got.
        if (const auto local_id = context->tryResolveStorageID(target->remote_time_series_storage_id))
            context->checkAccess(AccessType::SELECT, local_id);
        context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE);
    }

    /// Whether an unavailable replica fails the read is the read's own decision, as for any cluster() call.
    checkShardTargets(storage, *target, context, cluster, /* for_write = */ false);
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

    /// The write pins prefer_localhost_replica on, so a shard that is this server itself is written in-process on the
    /// caller's context: the sink's own INSERT grant on that table, with the columns it sends, is asked for before the probe.
    const auto cluster = typeid_cast<const StorageDistributed &>(storage).getCluster();
    if (cluster->getLocalShardCount())
    {
        /// A name that resolves to nothing is left to the probe, which refuses the write as having no target here.
        if (const auto local_id = context->tryResolveStorageID(target->remote_time_series_storage_id))
        {
            const auto metadata = storage.getInMemoryMetadataPtr(context, false);
            const auto columns_to_send = settings[Setting::insert_allow_materialized_columns]
                ? metadata->getSampleBlock().getNames()
                : metadata->getSampleBlockNonMaterialized().getNames();
            context->checkAccess(AccessType::INSERT, local_id, columns_to_send);
        }
    }

    checkShardTargets(storage, *target, context, cluster, /* for_write = */ true);
}

}
