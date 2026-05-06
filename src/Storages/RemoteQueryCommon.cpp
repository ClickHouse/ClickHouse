#include <Storages/RemoteQueryCommon.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTPartition.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Core/Block.h>
#include <Common/logger_useful.h>

#include <limits>

namespace DB
{

Block adoptBlock(const Block & header, const Block & block, LoggerPtr log)
{
    if (blocksHaveEqualStructure(header, block))
        return block;

    LOG_WARNING(log,
        "Structure does not match (remote: {}, local: {}), implicit conversion will be done.",
        header.dumpStructure(), block.dumpStructure());

    auto converting_dag = ActionsDAG::makeConvertingActions(
        block.cloneEmpty().getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        nullptr);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    Block converted = block;
    converting_actions->execute(converted);

    return converted;
}

ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Names & column_names)
{
    auto query = make_intrusive<ASTInsertQuery>();
    query->table_id = StorageID(database, table);
    auto columns = make_intrusive<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & column_name : column_names)
        columns->children.push_back(make_intrusive<ASTIdentifier>(column_name));
    return query;
}

ASTPtr createOptimizeForRemoteTableQuery(
    const StorageID & storage_id,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup)
{
    auto query = make_intrusive<ASTOptimizeQuery>();
    query->setDatabase(storage_id.database_name);
    query->setTable(storage_id.table_name);

    auto partition = make_intrusive<ASTPartition>();
    partition->setPartitionID(make_intrusive<ASTLiteral>(Field(partition_id)));
    query->partition = partition;
    query->children.push_back(query->partition);

    query->final = final;
    query->deduplicate = deduplicate;
    if (!deduplicate_by_columns.empty())
    {
        auto columns = make_intrusive<ASTExpressionList>();
        for (const auto & name : deduplicate_by_columns)
            columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        query->deduplicate_by_columns = columns;
        query->children.push_back(query->deduplicate_by_columns);
    }
    query->cleanup = cleanup;
    return query;
}

ASTPtr buildPartitionFilterAST(const Strings & partition_ids)
{
    if (partition_ids.empty())
        return nullptr;

    Array partition_array;
    partition_array.reserve(partition_ids.size());
    for (const auto & pid : partition_ids)
        partition_array.push_back(pid);

    return makeASTFunction("in",
        make_intrusive<ASTIdentifier>("_partition_id"),
        make_intrusive<ASTLiteral>(Field(partition_array)));
}

QueryPlanPtr createReadFromRemotePlan(
    ClusterProxy::SelectStreamFactory::Shards remote_shards,
    SharedHeader header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextMutablePtr remote_context,
    ContextPtr source_context,
    LoggerPtr log,
    UInt32 shard_count,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    const String & cluster_name,
    const String & step_description,
    ThrottlerPtr throttler,
    UnavailableShardTrackerPtr unavailable_shard_tracker)
{
    Scalars scalars = source_context->hasQueryContext()
        ? source_context->getQueryContext()->getScalars()
        : Scalars{};
    scalars.emplace(
        "_shard_count",
        Block{{DataTypeUInt32().createColumnConst(1, shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});

    auto external_tables = source_context->getExternalTables();

    auto query_plan = std::make_unique<QueryPlan>();
    auto read_from_remote = std::make_unique<ReadFromRemote>(
        std::move(remote_shards),
        std::move(header),
        processed_stage,
        main_table,
        table_func_ptr,
        remote_context,
        std::move(throttler),
        std::move(scalars),
        std::move(external_tables),
        log,
        shard_count,
        std::move(storage_limits),
        cluster_name,
        std::move(unavailable_shard_tracker));

    read_from_remote->setStepDescription(step_description, std::numeric_limits<size_t>::max());
    query_plan->addStep(std::move(read_from_remote));
    query_plan->addInterpreterContext(remote_context);

    return query_plan;
}

void unitePlanList(QueryPlan & result, std::vector<QueryPlanPtr> plans)
{
    if (plans.empty())
        return;

    if (plans.size() == 1)
    {
        result = std::move(*plans.front());
        return;
    }

    SharedHeaders input_headers;
    input_headers.reserve(plans.size());
    for (auto & plan : plans)
        input_headers.emplace_back(plan->getCurrentHeader());

    auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
    result.unitePlans(std::move(union_step), std::move(plans));
}

}
