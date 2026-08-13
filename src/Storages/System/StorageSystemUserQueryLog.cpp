#include <Storages/System/StorageSystemUserQueryLog.h>

#include <Columns/ColumnConst.h>
#include <Common/quoteString.h>
#include <Common/SettingsChanges.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryLogElement.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <fmt/format.h>

#include <optional>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Predicates of the outer query can be pushed down into the internal query only when they cannot
/// observe anything about other users' rows. A pushed-down predicate is evaluated on rows of all
/// users before the user filter hides the foreign ones, so it must be limited to expressions that
/// are deterministic (in the scope of a query), have no side effects, and cannot fail depending on
/// the value of a cell: a function like `throwIf`, or arithmetic that may overflow, would act as an
/// error-based oracle on other users' rows.

/// The columns that may appear in a pushed-down predicate: the partition / point-lookup columns
/// (this is what makes the pushdown useful: `event_date` and the times prune partitions and ranges
/// of the backing table) and the scalar identity columns. Comparing any of them with a constant
/// cannot fail per row.
const std::unordered_set<std::string_view> pushable_columns
{
    "type",
    "event_date",
    "event_time",
    "event_time_microseconds",
    "query_start_time",
    "query_start_time_microseconds",
    "query_id",
    "initial_query_id",
    "query_kind",
    "is_initial_query",
    "user",
    "initial_user",
    "current_database",
};

/// The functions that may appear in a pushed-down predicate: logical connectives and comparisons.
/// They never throw depending on the value of an argument row.
const std::unordered_set<std::string_view> pushable_functions
{
    "and",
    "or",
    "not",
    "equals",
    "notEquals",
    "less",
    "greater",
    "lessOrEquals",
    "greaterOrEquals",
    "like",
    "notLike",
    "ilike",
    "notILike",
};

/// The pattern argument of the LIKE family is compiled once only when it is constant. A pattern
/// coming from a cell would be compiled per row, and a malformed pattern (e.g. a trailing escape)
/// throws, which would make it an error-based oracle on other users' rows.
const std::unordered_set<std::string_view> functions_with_constant_second_argument
{
    "like",
    "notLike",
    "ilike",
    "notILike",
};

bool isPushableConstantType(const DataTypePtr & type)
{
    DataTypePtr unwrapped = removeNullable(removeLowCardinality(type));
    return isNumber(unwrapped) || isStringOrFixedString(unwrapped) || isDateOrDate32(unwrapped) || isDateTime(unwrapped)
        || isDateTime64(unwrapped) || isEnum(unwrapped) || isUUID(unwrapped) || isNothing(unwrapped);
}

bool isConstantNode(const ActionsDAG::Node & node)
{
    return node.column && isColumnConst(*node.column);
}

/// Returns the AST of a predicate subtree if the whole subtree is safe to evaluate on other users'
/// rows inside the internal query, nullptr otherwise.
ASTPtr tryBuildPushableAST(const ActionsDAG::Node & node)
{
    /// A folded constant, e.g. a literal or `today()`. Its value has already been computed by the
    /// outer query, so no function is re-executed (with full access) inside the internal query; the
    /// value is inlined as a literal with an explicit cast to its type.
    if (isConstantNode(node))
    {
        if (!isPushableConstantType(node.result_type))
            return nullptr;

        Field value;
        node.column->get(0, value);
        return addTypeConversionToAST(make_intrusive<ASTLiteral>(std::move(value)), node.result_type->getName());
    }

    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
        {
            if (!pushable_columns.contains(node.result_name))
                return nullptr;
            return make_intrusive<ASTIdentifier>(node.result_name);
        }
        case ActionsDAG::ActionType::ALIAS:
        {
            return tryBuildPushableAST(*node.children.front());
        }
        case ActionsDAG::ActionType::FUNCTION:
        {
            const String & function_name = node.function_base->getName();
            if (!pushable_functions.contains(function_name))
                return nullptr;

            if (functions_with_constant_second_argument.contains(function_name)
                && !(node.children.size() == 2 && isConstantNode(*node.children[1])))
                return nullptr;

            auto function = makeASTFunction(function_name);
            for (const auto * child : node.children)
            {
                ASTPtr child_ast = tryBuildPushableAST(*child);
                if (!child_ast)
                    return nullptr;
                function->arguments->children.push_back(std::move(child_ast));
            }
            return function;
        }
        default:
            return nullptr;
    }
}

}

/** The internal query is built and executed when the pipeline is initialized, so that the plan
  * optimization pass has already offered the outer filter to this step (`SourceStepWithFilter`):
  * the safe subset of its conjuncts is embedded into the internal query, where it restores
  * partition pruning and index lookups on the backing query log table. The step itself stays an
  * opaque source: the outer `FilterStep` remains in the plan and re-applies the full predicate,
  * so the pushdown only reduces the read, it does not replace the outer filtering, and no outer
  * expression is ever moved below the user filter unvetted.
  */
class ReadFromUserQueryLog final : public SourceStepWithFilter
{
public:
    ReadFromUserQueryLog(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader header_,
        StorageID log_table_id_)
        : SourceStepWithFilter(std::move(header_), column_names_, query_info_, storage_snapshot_, context_)
        , log_table_id(std::move(log_table_id_))
    {
        setStepDescription("Read the query log records of the current user");
    }

    String getName() const override { return "ReadFromUserQueryLog"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    String buildPushedDownConditions() const;

    StorageID log_table_id;
};

String ReadFromUserQueryLog::buildPushedDownConditions() const
{
    if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
        return {};

    ASTs pushable;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
    {
        /// A conjunct that folded into a constant cannot reduce the read.
        if (isConstantNode(*atom))
            continue;

        if (ASTPtr ast = tryBuildPushableAST(*atom))
            pushable.push_back(std::move(ast));
    }

    if (pushable.empty())
        return {};

    ASTPtr condition;
    if (pushable.size() == 1)
    {
        condition = std::move(pushable.front());
    }
    else
    {
        auto function = makeASTFunction("and");
        function->arguments->children = std::move(pushable);
        condition = function;
    }

    return condition->formatWithSecretsOneLine();
}

void ReadFromUserQueryLog::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    ContextPtr outer_context = getContext();

    /// The internal query runs with full access under a fresh context (a copy of the global context,
    /// which has no bound user), so the user does not need access to the query log table itself. It is
    /// not a bare global context, though: the caller's identity, normalized query hash, and execution
    /// settings are replayed the same way `StorageInMemoryMetadata::getSQLSecurityOverriddenContext`
    /// does, so that quotas keep bucketing per user and per `NORMALIZED_QUERY_HASH` (a fresh context
    /// starts with hash 0) and the backing query log scan honors the caller's profile
    /// (e.g. `max_threads`) instead of the server defaults.
    auto inner_context = Context::createCopy(outer_context->getGlobalContext());
    inner_context->makeQueryContext();
    inner_context->setClientInfo(outer_context->getClientInfo());
    inner_context->setProgressCallback(outer_context->getProgressCallback());
    inner_context->setProcessListElement(outer_context->getProcessListElement());
    inner_context->setNormalizedQueryHash(outer_context->getNormalizedQueryHash());

    /// Replay the caller's settings, except the expression-injection ones: `additional_table_filters`
    /// and `additional_result_filter` add expressions that would be evaluated on other users' rows before
    /// the user filter below hides them, which could leak their contents. They keep their default (empty)
    /// value from the global context because they are dropped from the applied changes rather than reset.
    SettingsChanges settings_changes = outer_context->getSettingsRef().changes();
    settings_changes.removeSetting("additional_table_filters");
    settings_changes.removeSetting("additional_result_filter");
    inner_context->applySettingsChanges(settings_changes);

    String select_columns;
    for (const auto & name : requiredSourceColumns())
    {
        if (!select_columns.empty())
            select_columns += ", ";
        select_columns += backQuoteIfNeed(name);
    }

    /// For queries received from an initiator (e.g. from a distributed query), the initiating user is
    /// recorded in `initial_user`, while `user` may be the interserver connection user. `currentUser()`
    /// is `ClientInfo::initial_user` as well, so both sides of the comparison use the initiating user.
    String select_query = fmt::format(
        "SELECT {} FROM {} WHERE (if(initial_user != '', initial_user, user) = {})",
        select_columns,
        log_table_id.getFullTableName(),
        quoteString(outer_context->getClientInfo().initial_user));

    if (String pushed_conditions = buildPushedDownConditions(); !pushed_conditions.empty())
        select_query += fmt::format(" AND ({})", pushed_conditions);

    /// The limit is set by the plan optimization only when nothing between the source and the `LIMIT`
    /// changes the number of rows, with the exception of `FilterStep`s, which it walks through. When a
    /// filter is present, applying the limit inside (where the possibly unpushed part of the predicate
    /// has not been applied yet) would return too few rows, so it is only used without a filter; the
    /// user filter is not a concern because the internal query applies it before its `LIMIT`.
    if (limit && !filter_actions_dag)
        select_query += fmt::format(" LIMIT {}", *limit);

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
    interpreter.addStorageLimits(*getQueryInfo().storage_limits);
    auto builder = interpreter.buildQueryPipeline();

    /// Convert to the exposed structure. This also casts LowCardinality columns to full ones.
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        builder.getHeader().getColumnsWithTypeAndName(),
        getOutputHeader()->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        outer_context, false, false, nullptr, nullptr, false);
    auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), ExpressionActionsSettings(outer_context));
    builder.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });

    builder.addContext(inner_context);

    /// The processors above were built from an internal query plan which does not outlive this call, so
    /// they still refer to its steps. Hand them to `ISourceStep::updatePipeline`, which attributes them
    /// to this step instead: it is the step of the outer plan that stands for this read, and per-step
    /// facilities of the outer plan (`EXPLAIN PIPELINE`, and the per-step wall clocks of
    /// `EXPLAIN ANALYZE`, which are looked up by the plan step of every executed processor) only know
    /// about the steps of that plan.
    processors = builder.getProcessors();

    pipeline = std::move(builder);
}

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

    /// Preserve the alias columns that `system.query_log` exposes (`ProfileEvents.Names`,
    /// `ProfileEvents.Values`, `Settings.Names`, `Settings.Values`), so queries that use these
    /// aliases keep working against `system.user_query_log` as well. Their types are de-LowCardinality-ed
    /// the same way as the physical columns; the aliases are computed by the outer query plan on top of
    /// the per-user-filtered read, so they cannot observe other users' rows.
    NamesAndAliases aliases = QueryLogElement::getNamesAndAliases();
    for (auto & alias : aliases)
        alias.type = recursiveRemoveLowCardinality(alias.type);
    res.setAliases(std::move(aliases));

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

    /// The backing query log table is identified by the live `QueryLog` object when the system loggers
    /// are running. When they are not, the absence of that object does not mean there is no readable
    /// backing table: `clickhouse-local --only-system-tables` deliberately skips starting the loggers
    /// while still loading the persisted `system` tables from disk, so the configured query log table
    /// can be present and queryable even though `getQueryLog()` returns nothing. Resolve the table from
    /// the `query_log.database` / `query_log.table` server settings in that case. The query log table is
    /// always created in the `system` database (a custom `query_log.database` is coerced back to `system`
    /// in `createSystemLog`), so only the table name is configurable; `system.query_log` is the default.
    /// This resolution is limited to the case where the query log is actually configured: `createSystemLog`
    /// returns nothing both when the loggers are not started and when the `query_log` section is missing
    /// altogether, while a `system.query_log` table created by an earlier run is still attached from disk.
    /// Without a configured query log, `system.user_query_log` must be empty, not a window into a table
    /// that the server no longer writes to.
    auto query_log = context->getQueryLog();
    std::optional<StorageID> log_table_id;
    if (query_log)
        log_table_id = query_log->getTableID();
    else if (context->getConfigRef().has("query_log"))
        log_table_id = StorageID("system", context->getConfigRef().getString("query_log.table", "query_log"));

    StoragePtr source_table = log_table_id ? DatabaseCatalog::instance().tryGetTable(*log_table_id, context) : nullptr;
    if (!source_table)
    {
        /// The query log is not configured, or its table has not been created yet (this happens on the
        /// first flush), or the persisted system tables were loaded without a query log table present.
        query_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(expected_header))));
        return;
    }

    /// `query_log.engine` can configure the backing table as a delegating storage, e.g. `Distributed`.
    /// The internal query below runs under a context that has full access only locally: a delegating
    /// storage forwards the outer query's `ClientInfo`, not this local full access, to whatever it
    /// delegates to. So the delegate would be read under the calling user's own identity there, which
    /// either fails for a user without explicit grants (breaking the promise that `system.user_query_log`
    /// needs none) or, if that identity does have grants, does not visibly narrow to the calling user on
    /// the initiator's side the way a local read does. Refuse rather than silently depend on either.
    if (source_table->isRemote())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The query log table {} configured via `query_log.database`/`query_log.table` is a delegating "
            "storage (engine {}), so `system.user_query_log` cannot read it under a local full-access context. "
            "Reconfigure `query_log.engine` to a local storage, or set `query_log.enable_user_query_log` to 0",
            source_table->getStorageID().getNameForLogs(), source_table->getName());

    /// The pipeline over the query log table is built inside an opaque source step: plan-level
    /// optimizations of the outer query (e.g. filter push-down and filter merging) must not move any
    /// expression of the outer query below the user filter, where it would observe other users' rows.
    /// Only the vetted subset of the outer predicate is pushed into the internal query (see
    /// `ReadFromUserQueryLog`), keeping efficient partition pruning and point lookups on the backing
    /// table.
    query_plan.addStep(std::make_unique<ReadFromUserQueryLog>(
        column_names, query_info, storage_snapshot, context, std::move(expected_header), source_table->getStorageID()));
}

}
