#include <Storages/System/StorageSystemUserQueryLog.h>

#include <Common/quoteString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryLogElement.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

#include <fmt/format.h>

namespace DB
{

ColumnsDescription StorageSystemUserQueryLog::getColumnsDescription()
{
    /// LowCardinality is removed from the exposed types: a LowCardinality column read from the query
    /// log table can share a dictionary with filtered-out rows, and returning it as is would leak
    /// other users' values (e.g. user names) through the dictionary.
    ColumnsDescription res;
    for (auto column : QueryLogElement::getColumnsDescription())
    {
        column.type = recursiveRemoveLowCardinality(column.type);
        res.add(std::move(column));
    }
    return res;
}

StorageSystemUserQueryLog::StorageSystemUserQueryLog(const StorageID & table_id_, ColumnsDescription columns_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_));
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemUserQueryLog::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto expected_header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));

    auto query_log = context->getQueryLog();
    StoragePtr source_table = query_log ? DatabaseCatalog::instance().tryGetTable(query_log->getTableID(), context) : nullptr;
    if (!source_table)
    {
        /// The query log is not configured, or its table has not been created yet (this happens on the first flush).
        query_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(expected_header))));
        return;
    }

    /// The inner query runs with full access under a fresh context, so the user does not need access
    /// to the query log table itself. The user's session settings are deliberately not propagated into
    /// it: settings such as `additional_table_filters` inject expressions into the query, and inside
    /// this query they would be evaluated on other users' rows before the filter below hides them.
    auto inner_context = Context::createCopy(context->getGlobalContext());
    inner_context->makeQueryContext();
    inner_context->setProgressCallback(context->getProgressCallback());
    inner_context->setProcessListElement(context->getProcessListElement());

    String select_columns;
    for (const auto & name : column_names)
    {
        if (!select_columns.empty())
            select_columns += ", ";
        select_columns += backQuoteIfNeed(name);
    }

    /// For queries received from an initiator (e.g. from a distributed query), the initiating user is
    /// recorded in `initial_user`, while `user` may be the interserver connection user. `currentUser()`
    /// is `ClientInfo::initial_user` as well, so both sides of the comparison use the initiating user.
    String select_query = fmt::format(
        "SELECT {} FROM {} WHERE if(initial_user != '', initial_user, user) = {}",
        select_columns,
        query_log->getTableID().getFullTableName(),
        quoteString(context->getClientInfo().initial_user));

    ParserSelectWithUnionQuery parser;
    ASTPtr select_ast = parseQuery(
        parser,
        select_query.data(),
        select_query.data() + select_query.size(),
        "user query log query",
        0,
        DBMS_DEFAULT_MAX_PARSER_DEPTH,
        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    InterpreterSelectQueryAnalyzer interpreter(select_ast, inner_context, SelectQueryOptions(QueryProcessingStage::Complete));
    interpreter.addStorageLimits(*query_info.storage_limits);
    auto builder = interpreter.buildQueryPipeline();

    /// Convert to the exposed structure. This also casts LowCardinality columns to full ones.
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        builder.getHeader().getColumnsWithTypeAndName(),
        expected_header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context, false, false, nullptr, nullptr, false);
    auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), ExpressionActionsSettings(context));
    builder.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });

    /// The pipeline is added to the plan as an opaque source: plan-level optimizations of the outer
    /// query (e.g. filter push-down and filter merging) must not move any expression of the outer
    /// query below the user filter, where it would observe other users' rows.
    QueryPlanResourceHolder resources;
    auto read_step = std::make_unique<ReadFromPreparedSource>(QueryPipelineBuilder::getPipe(std::move(builder), resources));
    read_step->setStepDescription("Read the query log records of the current user");
    query_plan.addStep(std::move(read_step));
    query_plan.addResources(std::move(resources));
    query_plan.addInterpreterContext(inner_context);
}

}
