#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnTuple.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Planner/Utils.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Chunk.h>

#include <Core/Settings.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Access/EnabledQuota.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <Poco/String.h>
#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event ScalarSubqueriesGlobalCacheHit;
    extern const Event ScalarSubqueriesLocalCacheHit;
    extern const Event ScalarSubqueriesCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

namespace Setting
{
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsString implicit_table_at_top_level;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
    extern const SettingsBool enable_scalar_subquery_optimization;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool optimize_merge_scalar_subqueries;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsUInt64 max_temporary_columns;
    extern const SettingsUInt64 max_temporary_non_const_columns;
}

namespace
{

bool subtreeHasViewSource(const IQueryTreeNode * node, const Context & context)
{
    if (!node)
        return false;

    if (const auto * table_node = node->as<TableNode>())
    {
        if (table_node->getStorageID().getFullNameNotQuoted() == context.getViewSource()->getStorageID().getFullNameNotQuoted())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (subtreeHasViewSource(child.get(), context))
            return true;

    return false;
}

bool hasSimpleSelectQueryClauses(const QueryNode & query)
{
    return !query.isCorrelated()
        && !query.hasSettingsChanges()
        && !query.isCTE()
        && !query.isMaterialized()
        && !query.isRecursiveWith()
        && !query.isDistinct()
        && !query.isGroupByAll()
        && !query.isGroupByWithTotals()
        && !query.isGroupByWithRollup()
        && !query.isGroupByWithCube()
        && !query.isGroupByWithGroupingSets()
        && !query.isOrderByAll()
        && !query.isLimitByAll()
        && !query.isLimitAfterAll()
        && !query.isLimitWithTies()
        && !query.isShuffle()
        && !query.hasWith()
        && !query.hasPrewhere()
        && query.hasWhere()
        && !query.hasGroupBy()
        && !query.hasHaving()
        && !query.hasWindow()
        && !query.hasQualify()
        && !query.hasOrderBy()
        && !query.hasInterpolate()
        && !query.hasLimitByLimit()
        && !query.hasLimitByOffset()
        && !query.hasLimitBy()
        && !query.hasLimit()
        && !query.hasLimitAfter()
        && !query.hasLimitUntil()
        && !query.hasOffset();
}

bool hasSupportedTableModifiers(const std::optional<TableExpressionModifiers> & modifiers)
{
    if (!modifiers)
        return true;

    return !modifiers->hasSampleSizeRatio()
        && !modifiers->hasSampleOffsetRatio()
        && !modifiers->hasStream();
}

bool isNarrowColumnType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(removeLowCardinality(type));
    if (unwrapped->isValueRepresentedByNumber())
        return true;

    return isFixedString(unwrapped) && unwrapped->getSizeOfValueInMemory() <= 32;
}

bool areTypesSafeToCompare(const DataTypePtr & lhs, const DataTypePtr & rhs)
{
    auto t0 = removeNullable(removeLowCardinality(lhs));
    auto t1 = removeNullable(removeLowCardinality(rhs));

    if (t0->onlyNull() || t1->onlyNull())
        return !t0->hasDynamicSubcolumnsData() && !t1->hasDynamicSubcolumnsData();

    if ((isInteger(t0) || isFloat(t0)) && (isInteger(t1) || isFloat(t1)))
        return true;

    /// Rescaling a decimal or a date/time to a different scale can overflow on rows that the
    /// subquery's own filter would have excluded, so only same-scale decimals may be compared.
    if (isDecimal(t0) && isDecimal(t1))
        return getDecimalScale(*t0) == getDecimalScale(*t1)
            && !WhichDataType(t0).isDecimal256() && !WhichDataType(t1).isDecimal256();

    if (isDecimal(t0) || isDecimal(t1))
        return false;

    if (isStringOrFixedString(t0) && isStringOrFixedString(t1))
        return true;

    return t0->equals(*t1) && !t0->hasDynamicSubcolumnsData();
}

bool isAllowedAggregateName(const String & name_lowercase, bool & is_count)
{
    if (name_lowercase == "count" || name_lowercase == "countornull")
    {
        is_count = true;
        return true;
    }

    is_count = false;
    return name_lowercase == "sum" || name_lowercase == "sumornull"
        || name_lowercase == "avg" || name_lowercase == "avgornull"
        || name_lowercase == "min" || name_lowercase == "minornull"
        || name_lowercase == "max" || name_lowercase == "maxornull";
}

bool isSafeLogicalOrNullFunction(const String & name_lowercase)
{
    return name_lowercase == "and" || name_lowercase == "or" || name_lowercase == "xor"
        || name_lowercase == "not" || name_lowercase == "isnull" || name_lowercase == "isnotnull";
}

bool isSafeComparisonFunction(const String & name_lowercase)
{
    return name_lowercase == "equals" || name_lowercase == "notequals"
        || name_lowercase == "less" || name_lowercase == "greater"
        || name_lowercase == "lessorequals" || name_lowercase == "greaterorequals";
}

bool isSafeResolvedExpression(
    const QueryTreeNodePtr & expr,
    const TableNode & expected_table,
    const ColumnsDescription & columns_desc,
    bool & all_columns_narrow)
{
    if (!expr)
        return false;

    if (const auto * constant = expr->as<ConstantNode>())
        return constant->isDeterministic() && !constant->hasSourceExpression();

    if (const auto * column = expr->as<ColumnNode>())
    {
        if (column->getColumnSourceOrNull().get() != &expected_table || column->hasExpression())
            return false;

        const auto & col_name = column->getColumnName();
        if (!columns_desc.hasPhysical(col_name))
            return false;

        if (const auto default_desc = columns_desc.getDefault(col_name); default_desc && default_desc->expression)
            return false;

        if (!isNarrowColumnType(column->getColumnType()))
            all_columns_narrow = false;

        return true;
    }

    const auto * function = expr->as<FunctionNode>();
    if (!function || !function->isOrdinaryCall() || !function->isResolved() || function->hasWindow())
        return false;

    if (!function->getParameters().getNodes().empty() || function->getNullsAction() != NullsAction::EMPTY)
        return false;

    if (auto fn_base = function->getFunction(); !fn_base || !fn_base->isDeterministic() || !fn_base->isDeterministicInScopeOfQuery())
        return false;

    const String lower_name = Poco::toLower(function->getFunctionName());
    const auto & args = function->getArguments().getNodes();

    if (isSafeComparisonFunction(lower_name))
    {
        if (args.size() != 2)
            return false;
        if (!areTypesSafeToCompare(args[0]->getResultType(), args[1]->getResultType()))
            return false;
    }
    else if (isSafeLogicalOrNullFunction(lower_name))
    {
        if (args.empty())
            return false;
    }
    else
    {
        return false;
    }

    return std::ranges::all_of(args, [&](const QueryTreeNodePtr & child)
    {
        return isSafeResolvedExpression(child, expected_table, columns_desc, all_columns_narrow);
    });
}

void splitConjunctionsQueryTree(const QueryTreeNodePtr & node, QueryTreeNodes & out)
{
    if (!node)
        return;

    if (const auto * fn = node->as<FunctionNode>();
        fn && fn->isOrdinaryCall() && fn->getFunctionName() == "and" && fn->getParameters().getNodes().empty())
    {
        for (const auto & child : fn->getArguments().getNodes())
            splitConjunctionsQueryTree(child, out);
        return;
    }

    out.push_back(node);
}

bool expressionReferencesAnyColumn(const QueryTreeNodePtr & node, const std::unordered_set<String> & target_columns)
{
    if (!node || target_columns.empty())
        return false;

    if (const auto * col = node->as<ColumnNode>())
        return target_columns.contains(col->getColumnName());

    for (const auto & child : node->getChildren())
        if (child && expressionReferencesAnyColumn(child, target_columns))
            return true;

    return false;
}

std::unordered_set<String> collectIndexAndKeyColumns(const StorageInMemoryMetadata & metadata)
{
    std::unordered_set<String> result;
    const auto key_column_lists = {
        metadata.getColumnsRequiredForPartitionKey(),
        metadata.getColumnsRequiredForSortingKey(),
        metadata.getColumnsRequiredForPrimaryKey(),
        metadata.getColumnsRequiredForSampling()};
    for (const auto & names : key_column_lists)
    {
        result.insert(names.begin(), names.end());
    }

    for (const auto & index : metadata.getSecondaryIndices())
    {
        result.insert(index.column_names.begin(), index.column_names.end());
        if (index.expression)
        {
            auto required = index.expression->getRequiredColumns();
            result.insert(required.begin(), required.end());
        }
    }

    return result;
}

struct ScalarCandidate
{
    QueryTreeNodePtr node;
    TableNodePtr table_node;
    String group_key;
};

/// A scalar subquery that can be merged with other ones: an allowed aggregate over a local `MergeTree` or `Memory` table
/// with a simple `WHERE`, whose conditions and argument can be evaluated on the rows of the other subqueries.
std::optional<ScalarCandidate> tryGetScalarCandidate(const QueryTreeNodePtr & node)
{
    const auto * query = node->as<QueryNode>();
    if (!query || !query->isResolved() || !hasSimpleSelectQueryClauses(*query))
        return {};

    const auto & context = query->getContext();
    const auto & settings = context->getSettingsRef();
    /// A merged scan reads the columns of all subqueries in one step, so the limits checked per read step could reject it.
    /// Table filters are keyed by table alias, which the merged scan keeps for only one subquery.
    if (!settings[Setting::optimize_merge_scalar_subqueries]
        || settings[Setting::empty_result_for_aggregation_by_empty_set]
        || settings[Setting::max_columns_to_read]
        || settings[Setting::max_temporary_columns]
        || settings[Setting::max_temporary_non_const_columns]
        || !settings[Setting::additional_table_filters].value.empty())
        return {};

    if (context->getViewSource() && subtreeHasViewSource(query, *context))
        return {};

    auto table_node = std::dynamic_pointer_cast<TableNode>(query->getJoinTreeNode());
    if (!table_node || table_node->isMaterializedCTE() || !hasSupportedTableModifiers(table_node->getTableExpressionModifiers()))
        return {};

    const auto & storage = table_node->getStorage();
    const auto & snapshot = table_node->getStorageSnapshot();
    if (!storage || !snapshot || !snapshot->metadata || snapshot->metadata->hasProjections())
        return {};

    if (!dynamic_cast<const MergeTreeData *>(storage.get()) && storage->getName() != "Memory")
        return {};

    const auto & projection_nodes = query->getProjection().getNodes();
    if (projection_nodes.size() != 1 || query->getProjectionColumns().size() != 1)
        return {};

    const auto * aggregate = projection_nodes[0]->as<FunctionNode>();
    if (!aggregate || !aggregate->isAggregationCall() || !aggregate->isResolved()
        || !aggregate->getParameters().getNodes().empty() || aggregate->getNullsAction() != NullsAction::EMPTY)
        return {};

    bool is_count = false;
    const auto & arguments = aggregate->getArguments().getNodes();
    if (!isAllowedAggregateName(Poco::toLower(aggregate->getFunctionName()), is_count) || arguments.size() > 1 || (!is_count && arguments.empty()))
        return {};

    const auto & columns = snapshot->metadata->getColumns();
    bool all_columns_narrow = true;
    for (const auto & argument : arguments)
        if (!isSafeResolvedExpression(argument, *table_node, columns, all_columns_narrow))
            return {};

    if (!isSafeResolvedExpression(query->getWhere(), *table_node, columns, all_columns_narrow))
        return {};

    /// Subqueries are merged only when they share the conditions on key and index columns, so the merged query keeps
    /// their index analysis. When a condition reads a wide column, all conditions must be shared, so the merged scan
    /// does not read wide columns for the rows of other subqueries.
    String group_key = table_node->isTemporaryTable()
        ? "T:" + table_node->getTemporaryTableName()
        : "C:" + table_node->getStorageID().getFullNameNotQuoted();
    if (const auto & modifiers = table_node->getTableExpressionModifiers(); modifiers && modifiers->hasFinal())
        group_key += ":FINAL";
    group_key += all_columns_narrow ? "|K:" : "|W:";

    QueryTreeNodes conditions;
    splitConjunctionsQueryTree(query->getWhere(), conditions);
    const auto index_columns = collectIndexAndKeyColumns(*snapshot->metadata);
    std::set<String> condition_hashes;
    for (const auto & condition : conditions)
    {
        if (all_columns_narrow && !expressionReferencesAnyColumn(condition, index_columns))
            continue;
        auto hash = QueryTreeNodePtrWithHashIgnoreAliases(condition).hash;
        condition_hashes.insert(fmt::format("{:016x}{:016x}", hash.high64, hash.low64));
    }
    for (const auto & hash : condition_hashes)
        group_key += hash + ',';

    return ScalarCandidate{node, std::move(table_node), std::move(group_key)};
}

QueryTreeNodePtr toBoolCondition(QueryTreeNodePtr condition, const ContextPtr & context)
{
    const auto & type = condition->getResultType();
    if (!type->lowCardinality() && isUInt8(removeNullable(type)))
        return condition;

    return createResolvedFunction(context, "toBool", {std::move(condition)});
}

QueryTreeNodePtr makeLogicalFunction(const String & name, QueryTreeNodes arguments, const ContextPtr & context)
{
    if (arguments.size() == 1)
        return std::move(arguments[0]);

    for (auto & argument : arguments)
        argument = toBoolCondition(std::move(argument), context);

    return createResolvedFunction(context, name, std::move(arguments));
}

/// SELECT aggIf(x, own conditions), ... FROM table WHERE shared conditions AND (own conditions OR ...)
QueryTreeNodePtr buildMergedScalarQuery(const std::vector<ScalarCandidate> & group, const ContextPtr & context)
{
    auto merged_context = Context::createCopy(context);
    auto merged_query = std::make_shared<QueryNode>(merged_context);
    merged_query->setIsSubquery(true);

    auto table_node = std::static_pointer_cast<TableNode>(group.front().table_node->clone());
    table_node->removeAlias();
    merged_query->getJoinTreeNode() = table_node;

    QueryTreeNodes aggregates;
    std::vector<QueryTreeNodes> conditions(group.size());
    for (size_t i = 0; i < group.size(); ++i)
    {
        const auto & query = group[i].node->as<const QueryNode &>();
        const IQueryTreeNode::ReplacementMap replacement{{group[i].table_node.get(), table_node}};
        aggregates.push_back(query.getProjection().getNodes()[0]->cloneAndReplace(replacement));
        aggregates.back()->removeAlias();
        splitConjunctionsQueryTree(query.getWhere()->cloneAndReplace(replacement), conditions[i]);
        for (auto & condition : conditions[i])
            condition->removeAlias();
    }

    /// The conditions of all subqueries are kept in `WHERE` as they are, so the index analysis can use them.
    QueryTreeNodePtrWithHashIgnoreAliasesSet shared_conditions;
    QueryTreeNodes where_conditions;
    for (const auto & condition : conditions.front())
    {
        QueryTreeNodePtrWithHashIgnoreAliases key(condition);
        if (std::ranges::all_of(conditions, [&](const QueryTreeNodes & other)
            {
                return std::ranges::any_of(other, [&](const QueryTreeNodePtr & node) { return QueryTreeNodePtrWithHashIgnoreAliases(node) == key; });
            }))
        {
            shared_conditions.insert(key);
            where_conditions.push_back(condition);
        }
    }

    auto & projection = merged_query->getProjection().getNodes();
    NamesAndTypes projection_columns;
    QueryTreeNodes alternatives;
    QueryTreeNodePtrWithHashIgnoreAliasesSet seen_alternatives;
    bool reads_all_rows = false;

    for (size_t i = 0; i < group.size(); ++i)
    {
        QueryTreeNodes own_conditions;
        for (const auto & condition : conditions[i])
            if (!shared_conditions.contains(QueryTreeNodePtrWithHashIgnoreAliases(condition)))
                own_conditions.push_back(condition);

        auto aggregate = aggregates[i];
        if (own_conditions.empty())
        {
            reads_all_rows = true;
        }
        else
        {
            auto condition = makeLogicalFunction("and", std::move(own_conditions), merged_context);
            if (seen_alternatives.insert(QueryTreeNodePtrWithHashIgnoreAliases(condition)).second)
                alternatives.push_back(condition);

            auto aggregate_if = std::make_shared<FunctionNode>(aggregate->as<const FunctionNode &>().getFunctionName() + "If");
            aggregate_if->getArguments().getNodes() = aggregate->as<const FunctionNode &>().getArguments().getNodes();
            aggregate_if->getArguments().getNodes().push_back(toBoolCondition(condition->clone(), merged_context));
            resolveAggregateFunctionNodeByName(*aggregate_if, aggregate_if->getFunctionName());

            const auto & type = aggregate->getResultType();
            aggregate = aggregate_if->getResultType()->equals(*type) ? aggregate_if : buildCastFunction(aggregate_if, type, merged_context);
        }

        projection_columns.emplace_back(fmt::format("_{}", i), aggregate->getResultType());
        projection.push_back(std::move(aggregate));
    }

    if (!reads_all_rows)
        where_conditions.push_back(makeLogicalFunction("or", std::move(alternatives), merged_context));
    if (!where_conditions.empty())
        merged_query->getWhere() = makeLogicalFunction("and", std::move(where_conditions), merged_context);

    merged_query->resolveProjectionColumns(std::move(projection_columns));
    return merged_query;
}

}

/// Evaluate scalar subquery and perform constant folding if scalar subquery does not have constant value
void QueryAnalyzer::evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool execute_for_exists)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual: {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    bool is_correlated_subquery = (query_node != nullptr && query_node->isCorrelated())
                                || (union_node != nullptr && union_node->isCorrelated());
    if (is_correlated_subquery)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot evaluate correlated scalar subquery");

    /// The subquery is executed now, so decide here whether its replaced tables read with parallel replicas.
    restoreReplacedTableStoragesIfNeeded(node);

    /// The scalar subqueries of a `SELECT` list are evaluated after the whole list is resolved,
    /// so the ones over the same table can be merged, see `evaluateDeferredScalarSubqueries`.
    if (deferred_scalar_subqueries && deferred_scalar_subqueries->projection_scope == scope.parent_scope
        && !only_analyze && !execute_for_exists && !early_short_circuit_type_inference_in_process && tryGetScalarCandidate(node))
    {
        deferred_scalar_subqueries->nodes.push_back(node);
        return;
    }

    auto & context = scope.context;

    /// Scalar subqueries in table function or parameterized view arguments are executed even in
    /// only-analyze mode. The table function / view is resolved into a storage during analysis
    /// (its header is needed), so it requires real argument values rather than type-only
    /// placeholders. Otherwise a placeholder would be passed, e.g. an empty URL to `s3`:
    /// CREATE TABLE t ENGINE = MergeTree ORDER BY () AS
    /// WITH (SELECT path FROM table_with_paths) AS path SELECT * FROM s3(path, NOSIGN);
    /// or a default (empty) value substituted for a parameterized view parameter.
    /// Early short-circuit inference is different: it must never execute speculative work. If a
    /// table function or view needs the actual scalar value, resolution fails and the optimization
    /// falls back to the regular analyzer.
    const bool only_analyze_subquery = early_short_circuit_type_inference_in_process
        || (only_analyze
            && !table_function_arguments_in_resolve_process
            && !parameterized_view_arguments_in_resolve_process);

    if (early_short_circuit_type_inference_in_process && (execute_for_exists || !query_node))
    {
        /// Type-only analysis cannot validate scalar cardinality or determine the runtime value
        /// of EXISTS. Keep resolving the clone for its type, but prevent the early fold.
        early_short_circuit_type_inference_failed = true;
    }

    Block scalar_block;

    auto node_without_alias = node->clone();
    node_without_alias->removeAlias();

    QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
    auto str_hash = DB::toString(node_with_hash.hash);

    bool can_use_global_scalars = !only_analyze_subquery && !(context->getViewSource() && subtreeHasViewSource(node_without_alias.get(), *context));

    auto & scalars_cache = early_short_circuit_type_inference_in_process
        ? scalar_subquery_to_scalar_value_type_only
        : (can_use_global_scalars ? scalar_subquery_to_scalar_value_global : scalar_subquery_to_scalar_value_local);

    if (scalars_cache.contains(node_with_hash))
    {
        if (can_use_global_scalars)
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
        else
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesLocalCacheHit);

        scalar_block = scalars_cache.at(node_with_hash);
    }
    else if (context->hasQueryContext() && can_use_global_scalars && context->getQueryContext()->hasComputedScalar(str_hash))
    {
        scalar_block = context->getQueryContext()->getScalar(str_hash);
        scalar_subquery_to_scalar_value_global.emplace(node_with_hash, scalar_block);
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);
        auto subquery_context = Context::createCopy(context);

        Settings subquery_settings = context->getSettingsCopy();
        subquery_settings[Setting::max_result_rows] = 1;
        subquery_settings[Setting::extremes] = false;
        subquery_settings[Setting::implicit_table_at_top_level] = "";
        /// When execute `INSERT INTO t WITH ... SELECT ...`, it may lead to `Unknown columns`
        /// exception with this settings enabled(https://github.com/ClickHouse/ClickHouse/issues/52494).
        subquery_settings[Setting::use_structure_from_insertion_table_in_table_functions] = false;
        subquery_settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
        subquery_context->setSettings(subquery_settings);

        auto query_tree = node->clone();
        /// Update context for the QueryTree, because apparently Planner would use this context.
        if (auto * new_query_node = query_tree->as<QueryNode>())
            new_query_node->getMutableContext() = subquery_context;
        if (auto * new_union_node = query_tree->as<UnionNode>())
            new_union_node->getMutableContext() = subquery_context;

        /// The Planner reads the context from every node, so parallel replicas must be disabled
        /// in nested QueryNode/UnionNode contexts too, not just on the top node. Same recursive
        /// walk as createLocalPlanForParallelReplicas.
        std::vector<IQueryTreeNode *> nodes_to_visit;
        for (const auto & child : query_tree->getChildren())
            if (child)
                nodes_to_visit.push_back(child.get());
        while (!nodes_to_visit.empty())
        {
            auto * current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * nested_query_node = current->as<QueryNode>())
            {
                auto nested_context = Context::createCopy(nested_query_node->getContext());
                nested_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                nested_query_node->getMutableContext() = std::move(nested_context);
            }
            else if (auto * nested_union_node = current->as<UnionNode>())
            {
                auto nested_context = Context::createCopy(nested_union_node->getContext());
                nested_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                nested_union_node->getMutableContext() = std::move(nested_context);
            }

            for (const auto & child : current->getChildren())
                if (child)
                    nodes_to_visit.push_back(child.get());
        }

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, scope.subquery_depth, true /*is_subquery*/);
        options.only_analyze = only_analyze_subquery;
        options.view_source_in_subqueries = view_source_in_subqueries;

        /// Scalar subqueries may reference materialized CTEs that haven't been populated yet.
        /// Force CTE materialization in the sub-plan so that the pipeline execution
        /// populates the CTE StorageMemory tables before the scalar subquery reads from them.
        /// This is analogous to addBuildSubqueriesForSetsStepIfNeeded using forceMaterializeCTE()
        /// for set subqueries that reference CTEs.
        options.forceMaterializeCTE();

        QueryTreePassManager query_tree_pass_manager(subquery_context);
        addQueryTreePasses(query_tree_pass_manager, options.only_analyze);
        query_tree_pass_manager.run(query_tree);

        /// The inner query of an ordinary view referenced by the view query reads the table itself,
        /// not the inserted block.
        if (auto storage = getViewSourceIfRead(subquery_context, view_source_in_subqueries))
            replaceStorageInQueryTree(query_tree, subquery_context, storage);
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_tree, subquery_context, options);

        auto wrap_with_nullable_or_tuple = [](Block & block)
        {
            block = materializeBlock(block);
            if (block.columns() == 1)
            {
                auto & column = block.getByPosition(0);
                /// Here we wrap type to nullable if we can.
                /// It is needed cause if subquery return no rows, it's result will be Null.
                /// In case of many columns, do not check it cause tuple can't be nullable.
                if (!column.type->isNullable() && column.type->canBeInsideNullable())
                {
                    column.type = makeNullable(column.type);
                    column.column = makeNullable(column.column);
                }
            } else
            {
                /** Make unique column names for tuple.
                *
                * Example: SELECT (SELECT 2 AS x, x)
                */
                makeUniqueColumnNamesInBlock(block);
                block = Block({{
                        ColumnTuple::create(block.getColumns()),
                        std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames()),
                        "tuple"
                    }});
            }
        };

        if (only_analyze_subquery)
        {
            /// If query is only analyzed, then constants are not correct.
            scalar_block = *interpreter->getSampleBlock();
            for (auto & column : scalar_block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    /// Not `NULL`: the placeholder is still evaluated in the enclosing expression, and
                    /// e.g. a cast to a non-Nullable type would throw. `Nothing` has no other value.
                    auto nested_type = removeNullable(removeLowCardinality(column.type));
                    if (isNothing(nested_type))
                        mut_col->insertDefault();
                    else
                        mut_col->insert(nested_type->getDefault());
                    column.column = std::move(mut_col);
                }
            }

            wrap_with_nullable_or_tuple(scalar_block);
        }
        else
        {
            BlockIO io;
            auto & query_plan = interpreter->getQueryPlan();
            bool skip_execution_for_exists = false;

            /// A scalar subquery is executed during the analysis of the query, before the quotas of the query are checked,
            /// so an exhausted quota over profile events is checked here, before the subquery consumes any resources.
            /// The query plan has been built above, so `ignoreQuota` is known (e.g. for exempt tables in `system`).
            /// The query is then charged with what the subquery consumes even if it is exempt itself (see
            /// `QueryStatus::executed_non_exempt_scalar_subquery`).
            if (!interpreter->ignoreQuota())
            {
                if (auto quota = subquery_context->getQuota())
                    quota->checkExceededProfileEvents(subquery_context->getNormalizedQueryHash());
                if (auto process_list_elem = subquery_context->getProcessListElement())
                    process_list_elem->markNonExemptScalarSubqueryExecuted();
            }

            if (execute_for_exists)
            {
                if (optimizePlanForExists(query_plan, /*is_correlated=*/false))
                    skip_execution_for_exists = true;
                else
                {
                    auto limit_step = std::make_unique<LimitStep>(query_plan.getCurrentHeader(), 1, 0);
                    query_plan.addStep(std::move(limit_step));
                }
            }

            if (!skip_execution_for_exists)
            {
                QueryPlanOptimizationSettings optimization_settings(subquery_context);
                BuildQueryPipelineSettings build_pipeline_settings(subquery_context);

                query_plan.setConcurrencyControl(subquery_context->getSettingsRef()[Setting::use_concurrency_control]);

                auto pipeline_builder = std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));

                io.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
                io.pipeline.setQuota(subquery_context->getQuota());
                io.pipeline.setNormalizedQueryHash(subquery_context->getNormalizedQueryHash());
            }

            std::optional<PullingAsyncPipelineExecutor> executor;
            Chunk chunk;

            if (!skip_execution_for_exists)
            {
                io.pipeline.setProgressCallback(context->getProgressCallback());
                io.pipeline.setProcessListElement(context->getProcessListElement());
                io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

                executor.emplace(io.pipeline);
                if (auto cancel_cb = context->hasQueryContext() ? context->getQueryContext()->getInteractiveCancelCallback() : nullptr)
                    executor->setCancelCallback(std::move(cancel_cb), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
                while (chunk.getNumRows() == 0 && executor->pull(chunk))
                {
                }
            }

            if (chunk.getNumRows() == 0)
            {
                DataTypePtr type;
                if (execute_for_exists)
                    type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                else
                {
                    const auto & sample_block = interpreter->getSampleBlock();
                    auto types = sample_block->getDataTypes();
                    if (types.size() != 1)
                        types = {std::make_shared<DataTypeTuple>(types)};

                    type = types[0];
                    if (!type->isNullable())
                    {
                        if (!type->canBeInsideNullable())
                            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                                "Scalar subquery returned empty result of type {} which cannot be Nullable",
                                type->getName());

                        type = makeNullable(type);
                    }
                }

                auto scalar_column = type->createColumn();
                if (skip_execution_for_exists)
                    scalar_column->insert(1);
                else
                    scalar_column->insert(Null());
                scalar_block.insert({std::move(scalar_column), type, (execute_for_exists ? "exists" : "null")});
            }
            else
            {
                if (chunk.getNumRows() != 1)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                Chunk tmp_chunk;
                while (tmp_chunk.getNumRows() == 0 && executor->pull(tmp_chunk))
                {
                }

                if (tmp_chunk.getNumRows() != 0)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                if (execute_for_exists)
                {
                    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                    auto scalar_column = type->createColumn();
                    scalar_column->insert(1);
                    scalar_block.insert({std::move(scalar_column), type, "exists"});
                }
                else
                {
                    /// Detach columns from the chunk so that the resulting block holds
                    /// uniquely-owned columns. `wrap_with_nullable_or_tuple` calls
                    /// `materializeBlock` (which goes through `IColumn::mutate` for
                    /// `Const` / `LowCardinality` columns) and may construct a
                    /// `ColumnTuple`, both of which require `use_count() == 1` for
                    /// the inputs in debug/sanitizer builds.
                    auto block = executor->getHeader().cloneWithColumns(chunk.detachColumns());
                    wrap_with_nullable_or_tuple(block);
                    scalar_block = std::move(block);
                }
            }

            logProcessorProfile(context, io.pipeline.getProcessors());
        }

        scalars_cache.emplace(node_with_hash, scalar_block);
        if (can_use_global_scalars && context->hasQueryContext())
            context->getQueryContext()->addScalar(str_hash, scalar_block);
    }

    const auto & scalar_column_with_type = scalar_block.safeGetByPosition(0);
    const auto & scalar_type = scalar_column_with_type.type;

    if (early_short_circuit_type_inference_in_process && !execute_for_exists)
    {
        /// During early short-circuit type inference the scalar value is intentionally not
        /// executed, so do not expose the arbitrary default from the sample block as a constant.
        /// A non-constant placeholder preserves the real type. Functions that require the actual
        /// constant value will reject it, causing the optimization to fall back to normal analysis.
        node = std::make_shared<ColumnNode>(
            NameAndTypePair{"_subquery_" + std::to_string(subquery_counter - 1), scalar_type},
            TableExpressionNodeWeakPtr{});
        return;
    }

    const auto * scalar_type_name = scalar_block.safeGetByPosition(0).type->getFamilyName();
    static const std::set<std::string_view> useless_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};
    auto * nearest_query_scope = scope.getNearestQueryScope();

    /// Always convert to literals when there is no query context, or when resolving a
    /// parameterized view argument (its value must fold to a literal to be matched against
    /// the view's query parameters, see `parameterized_view_arguments_in_resolve_process`).
    /// The EXISTS caller also requires a ConstantNode. In type-only analysis its sample value is
    /// only a placeholder; `early_short_circuit_type_inference_failed` prevents folding from it.
    if (execute_for_exists || !context->getSettingsRef()[Setting::enable_scalar_subquery_optimization]
        || !useless_literal_types.contains(scalar_type_name) || !context->hasQueryContext()
        || !nearest_query_scope || parameterized_view_arguments_in_resolve_process)
    {
        ConstantValue constant_value{ ConstantValue::wrapToColumnConst(scalar_column_with_type.column), scalar_type };
        auto constant_node = std::make_shared<ConstantNode>(constant_value, node);

        if (scalar_column_with_type.column->isNullAt(0))
        {
            node = buildCastFunction(constant_node, constant_node->getResultType(), context);
            node = std::make_shared<ConstantNode>(std::move(constant_value), node);
        }
        else
            node = std::move(constant_node);

        return;
    }

    auto & nearest_query_scope_query_node = nearest_query_scope->scope_node->as<QueryNode &>();
    auto & mutable_context = nearest_query_scope_query_node.getMutableContext();

    auto scalar_query_hash_string = DB::toString(node_with_hash.hash) + (only_analyze_subquery ? "_analyze" : "");

    if (mutable_context->hasQueryContext())
        mutable_context->getQueryContext()->addScalar(scalar_query_hash_string, scalar_block);

    mutable_context->addScalar(scalar_query_hash_string, scalar_block);

    std::string get_scalar_function_name = "__getScalar";

    auto scalar_query_hash_constant_node = std::make_shared<ConstantNode>(std::move(scalar_query_hash_string), std::make_shared<DataTypeString>());

    auto get_scalar_function_node = std::make_shared<FunctionNode>(get_scalar_function_name);
    get_scalar_function_node->getArguments().getNodes().push_back(std::move(scalar_query_hash_constant_node));

    auto get_scalar_function = FunctionFactory::instance().get(get_scalar_function_name, mutable_context);
    get_scalar_function_node->resolveAsFunction(get_scalar_function->build(get_scalar_function_node->getArgumentColumns()));

    node = std::move(get_scalar_function_node);
}

void QueryAnalyzer::evaluateDeferredScalarSubqueries(QueryTreeNodePtr & projection_node_list, const QueryTreeNodes & nodes, IdentifierResolveScope & scope)
{
    /// Execute each group of subqueries over the same table as one query and put the values into the scalar cache,
    /// as if the subqueries were evaluated one by one.
    std::vector<std::vector<ScalarCandidate>> groups;
    std::unordered_map<String, size_t> group_indexes;
    QueryTreeNodePtrWithHashSet seen_nodes;
    for (const auto & node : nodes)
    {
        auto node_without_alias = node->clone();
        node_without_alias->removeAlias();

        /// A subquery can repeat another one, or be already evaluated as a required constant.
        QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
        if (scalar_subquery_to_scalar_value_global.contains(node_with_hash) || !seen_nodes.insert(node_with_hash).second)
            continue;

        if (auto candidate = tryGetScalarCandidate(node_without_alias))
        {
            auto [it, inserted] = group_indexes.try_emplace(candidate->group_key, groups.size());
            if (inserted)
                groups.emplace_back();
            groups[it->second].push_back(std::move(*candidate));
        }
    }

    for (const auto & group : groups)
    {
        if (group.size() < 2)
            continue;

        QueryTreeNodePtr merged_query = buildMergedScalarQuery(group, scope.context);
        QueryTreeNodePtrWithHash merged_query_with_hash(merged_query);
        auto & merged_query_scope = createIdentifierResolveScope(merged_query, &scope);
        merged_query_scope.subquery_depth = scope.subquery_depth + 1;
        auto evaluated_query = merged_query;
        evaluateScalarSubqueryIfNeeded(evaluated_query, merged_query_scope);

        Block merged_block = scalar_subquery_to_scalar_value_global.at(merged_query_with_hash);
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*merged_block.getByPosition(0).column);
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*merged_block.getByPosition(0).type);
        for (size_t i = 0; i < group.size(); ++i)
        {
            ColumnWithTypeAndName value{tuple_column.getColumnPtr(i), tuple_type.getElement(i), "scalar"};
            if (value.type->canBeInsideNullable())
            {
                value.type = makeNullable(value.type);
                value.column = makeNullable(value.column);
            }

            Block value_block{std::move(value)};
            QueryTreeNodePtrWithHash node_with_hash(group[i].node);
            scalar_subquery_to_scalar_value_global.emplace(node_with_hash, value_block);
            if (scope.context->hasQueryContext())
                scope.context->getQueryContext()->addScalar(DB::toString(node_with_hash.hash), value_block);
        }
    }

    std::unordered_set<const IQueryTreeNode *> deferred_nodes;
    for (const auto & node : nodes)
        deferred_nodes.insert(node.get());

    replaceDeferredScalarSubqueries(projection_node_list, deferred_nodes, scope);
}

bool QueryAnalyzer::replaceDeferredScalarSubqueries(
    QueryTreeNodePtr & node, const std::unordered_set<const IQueryTreeNode *> & deferred_nodes, IdentifierResolveScope & scope)
{
    if (deferred_nodes.contains(node.get()))
    {
        evaluateScalarSubqueryIfNeeded(node, node_to_scope_map.at(node));
        return true;
    }

    const auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION || node_type == QueryTreeNodeType::LAMBDA)
        return false;

    bool replaced = false;
    for (auto & child : node->getChildren())
        if (child)
            replaced |= replaceDeferredScalarSubqueries(child, deferred_nodes, scope);

    /// The function was resolved with the subquery as an argument, resolve it again to fold the value.
    if (replaced && node_type == QueryTreeNodeType::FUNCTION && node->as<FunctionNode &>().isOrdinaryCall())
        resolveFunction(node, scope);

    return replaced;
}

bool QueryAnalyzer::evaluateDeferredScalarSubqueryNow(QueryTreeNodePtr & node)
{
    if (!deferred_scalar_subqueries || std::ranges::find(deferred_scalar_subqueries->nodes, node) == deferred_scalar_subqueries->nodes.end())
        return false;

    auto * deferred = std::exchange(deferred_scalar_subqueries, nullptr);
    SCOPE_EXIT({ deferred_scalar_subqueries = deferred; });
    evaluateScalarSubqueryIfNeeded(node, node_to_scope_map.at(node));
    return true;
}

}
