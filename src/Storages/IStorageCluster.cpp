#include <Storages/IStorageCluster.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Core/QueryProcessingStage.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/narrowPipe.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>
#include <DataTypes/DataTypeFunction.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Common/ProfileEvents.h>

#include <algorithm>
#include <memory>
#include <string>


namespace ProfileEvents
{
    extern const Event Shards;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

IStorageCluster::IStorageCluster(
    const String & cluster_name_,
    const StorageID & table_id_,
    LoggerPtr log_)
    : IStorage(table_id_)
    , log(log_)
    , cluster_name(cluster_name_)
{
}

namespace
{

ASTPtr filterNodeToAST(const ActionsDAG::Node & node, const std::unordered_map<String, ASTPtr> & read_columns)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
        {
            auto it = read_columns.find(node.result_name);
            if (it == read_columns.end())
                return nullptr;
            auto expression = it->second->clone();
            if (auto * with_alias = dynamic_cast<ASTWithAlias *>(expression.get()))
                with_alias->setAlias("");
            return expression;
        }
        case ActionsDAG::ActionType::COLUMN:
        {
            if (!node.column || !node.is_deterministic_constant || node.is_runtime_filter_id || node.is_masked_secret)
                return nullptr;
            if (typeid_cast<const DataTypeFunction *>(node.result_type.get()))
                return nullptr;
            if (const auto * column_set = checkAndGetColumn<ColumnSet>(&node.column->getDataColumn()))
            {
                const auto * tuple_set = dynamic_cast<const FutureSetFromTuple *>(column_set->getData().get());
                if (!tuple_set || !tuple_set->getSourceAST() || tuple_set->getInputRowCount() > 100000)
                    return nullptr;
                return tuple_set->getSourceAST()->clone();
            }
            return makeASTFunction("_CAST", make_intrusive<ASTLiteral>(node.column->getField()), make_intrusive<ASTLiteral>(node.result_type->getName()));
        }
        case ActionsDAG::ActionType::ALIAS:
            return filterNodeToAST(*node.children.at(0), read_columns);
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node.function_base || !node.function_base->isDeterministic() || node.function_base->getName().starts_with("__")
                || !FunctionFactory::instance().hasNameOrAlias(node.function_base->getName()))
                return nullptr;
            auto function = makeASTFunction(node.function_base->getName());
            for (const auto * child : node.children)
            {
                auto argument = filterNodeToAST(*child, read_columns);
                if (!argument)
                    return nullptr;
                function->arguments->children.push_back(std::move(argument));
            }
            return function;
        }
        default:
            return nullptr;
    }
}

ASTPtr filterToAST(const ActionsDAG::Node & predicate, const std::unordered_map<String, ASTPtr> & read_columns)
{
    std::vector<const ActionsDAG::Node *> conjuncts;
    const auto * root = &predicate;
    while (root->type == ActionsDAG::ActionType::ALIAS)
        root = root->children.at(0);
    if (root->type == ActionsDAG::ActionType::FUNCTION && root->function_base && root->function_base->getName() == "and")
        conjuncts.assign(root->children.begin(), root->children.end());
    else
        conjuncts.push_back(root);

    ASTs translated;
    for (const auto * conjunct : conjuncts)
        if (auto ast = filterNodeToAST(*conjunct, read_columns))
            translated.push_back(std::move(ast));

    if (translated.empty())
        return nullptr;
    if (translated.size() == 1)
        return translated.front();
    auto result = makeASTFunction("and");
    result->arguments->children = std::move(translated);
    return result;
}

}

void ReadFromCluster::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    const ActionsDAG * filter = filter_actions_dag ? filter_actions_dag.get() : query_info.filter_actions_dag.get();
    if (filter)
        predicate = filter->getOutputs().at(0);

    if (filter_actions_dag && !filter_actions_dag->getOutputs().empty())
    {
        if (!query_to_send_without_filter)
            query_to_send_without_filter = query_to_send->clone();

        auto query = query_to_send_without_filter->clone();
        auto * select = query->as<ASTSelectQuery>();
        const auto & header_columns = getOutputHeader()->getColumnsWithTypeAndName();
        std::unordered_map<String, ASTPtr> read_columns;
        if (select && select->select() && select->select()->children.size() == header_columns.size())
            for (size_t i = 0; i < header_columns.size(); ++i)
            {
                const auto & expression = select->select()->children[i];
                read_columns.emplace(header_columns[i].name, expression);
                read_columns.emplace(expression->getAliasOrColumnName(), expression);
            }

        if (auto where = read_columns.empty() ? nullptr : filterToAST(*filter_actions_dag->getOutputs().at(0), read_columns))
        {
            if (auto existing = select->where())
                where = makeASTFunction("and", existing->clone(), where);
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));
            query_to_send = std::move(query);
        }
    }

    createExtension(predicate);
}

void ReadFromCluster::createExtension(const ActionsDAG::Node * predicate)
{
    if (extension)
        return;

    extension = storage->getTaskIteratorExtension(
        predicate,
        filter_actions_dag ? filter_actions_dag.get() : query_info.filter_actions_dag.get(),
        context,
        cluster,
        getStorageSnapshot()->metadata);
}

/// The code executes on initiator
void IStorageCluster::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    updateConfigurationIfNeeded(context);

    storage_snapshot->check(column_names);

    updateBeforeRead(context);
    auto cluster = getCluster(context);

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)

    SharedHeader sample_block;
    ASTPtr query_to_send = query_info.query;

    sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query_info.query, context, SelectQueryOptions(processed_stage));

    /// The nodes that will run the query are the nodes of this storage's own cluster.
    updateQueryToSendIfNeeded(query_to_send, storage_snapshot, context, getClusterName());

    RestoreQualifiedNamesVisitor::Data data;
    data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query_info.query->as<ASTSelectQuery &>(), 0));
    data.remote_table.database = context->getCurrentDatabase();
    data.remote_table.table = getName();
    RestoreQualifiedNamesVisitor(data).visit(query_to_send);
    AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase(),
                                      /* only_replace_current_database_function_= */false,
                                      /* only_replace_in_join_= */true);
    visitor.visit(query_to_send);

    /// Strip initiator-only settings from the forwarded query text as well: the inter-server settings
    /// packet is stripped in `ReadFromCluster::updateSettings`, but `query_to_send` is also serialized via
    /// `formatWithSecretsOneLine()` with its `SETTINGS` clause intact, which would otherwise leak those
    /// names to shards (and trip `UNKNOWN_SETTING` on an older shard in a rolling upgrade).
    ClusterProxy::stripInitiatorOnlySettingsFromQuery(query_to_send);

    auto this_ptr = std::static_pointer_cast<IStorageCluster>(shared_from_this());

    auto reading = std::make_unique<ReadFromCluster>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        sample_block,
        std::move(this_ptr),
        std::move(query_to_send),
        processed_stage,
        cluster,
        log);

    query_plan.addStep(std::move(reading));
}

void ReadFromCluster::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    Pipes pipes;
    auto new_context = updateSettings(context->getSettingsRef());
    const auto & current_settings = new_context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

    size_t replica_index = 0;
    auto max_replicas_to_use = static_cast<UInt64>(cluster->getShardsInfo().size());
    if (current_settings[Setting::max_parallel_replicas] > 1)
        max_replicas_to_use = std::min(max_replicas_to_use, current_settings[Setting::max_parallel_replicas].value);

    createExtension(nullptr);

    ProfileEvents::increment(ProfileEvents::Shards, max_replicas_to_use);

    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (pipes.size() >= max_replicas_to_use)
            break;

        /// We're taking all replicas as shards,
        /// so each shard will have only one address to connect to.
        auto try_results = shard_info.pool->getMany(
            timeouts,
            current_settings,
            PoolMode::GET_ONE,
            {},
            /*skip_unavailable_endpoints=*/true);

        if (try_results.empty())
            continue;

        IConnections::ReplicaInfo replica_info{.number_of_current_replica = replica_index++};

        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            std::vector<IConnectionPool::Entry>{try_results.front()},
            query_to_send->formatWithSecretsOneLine(),
            getOutputHeader(),
            new_context,
            /*throttler=*/nullptr,
            scalars,
            Tables(),
            processed_stage,
            nullptr,
            RemoteQueryExecutor::Extension{.task_iterator = extension->task_iterator, .replica_info = std::move(replica_info)},
            shard_info.pool);

        remote_query_executor->setLogger(log);
        /// The cluster of a *Cluster function uses every replica as a shard, so `shard_num`
        /// identifies the fan-out entry rather than a shard of the original cluster definition.
        remote_query_executor->setShardScope({storage->getClusterName(), shard_info.shard_num});
        Pipe pipe{std::make_shared<RemoteSource>(
            remote_query_executor,
            add_agg_info,
            current_settings[Setting::async_socket_for_remote],
            current_settings[Setting::async_query_sending_for_remote])};
        pipe.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<UnmarshallBlocksTransform>(header); });
        pipes.emplace_back(std::move(pipe));
    }

    if (pipes.empty())
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot connect to any replica for query execution");

    auto pipe = Pipe::unitePipes(std::move(pipes));
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

QueryProcessingStage::Enum IStorageCluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    /// Only a follower reached by another node's cluster function (SECONDARY_QUERY) just fetches
    /// raw data. Everything else is the initiator of the distributed read, including internal
    /// contexts that never set the kind (NO_QUERY), e.g. a Replicated database DDL worker.
    if (context->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}

ContextPtr ReadFromCluster::updateSettings(const Settings & settings)
{
    Settings new_settings{settings};

    /// Cluster table functions should always skip unavailable shards.
    new_settings[Setting::skip_unavailable_shards] = true;

    /// Strip the initiator-only settings (the query-shaping settings, the result-serialisation
    /// settings, and `database`): they are materialized on the initiator and must not be forwarded
    /// to the remote servers, where they would re-shape the per-shard subquery a second time or
    /// break it (e.g. `format = 'Null'`). This mirrors the `Distributed` fan-out.
    ClusterProxy::stripInitiatorOnlySettings(new_settings);

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    return new_context;
}

ClusterPtr IStorageCluster::getCluster(ContextPtr context) const
{
    return context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());
}

}
