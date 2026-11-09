#include <Analyzer/Passes/CorrelatedScalarAggregateToWindowPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/WindowNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/Context.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageSnapshot.h>

#include <set>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsBool optimize_correlated_scalar_aggregate_to_window;
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
}

namespace
{

struct TableExpressionInfo
{
    QueryTreeNodePtr * slot = nullptr;
    /// On the NULL-extended side of an outer join or in a `PASTE JOIN`, which pairs rows by position.
    bool excluded = false;
};

using TableExpressions = std::unordered_map<const IQueryTreeNode *, TableExpressionInfo>;

void collectTableExpressions(QueryTreeNodePtr & node, bool excluded, TableExpressions & result)
{
    if (node->as<TableNode>())
    {
        result[node.get()] = {&node, excluded};
    }
    else if (auto * join = node->as<JoinNode>())
    {
        const auto kind = join->getKind();
        excluded |= kind == JoinKind::Paste;
        collectTableExpressions(join->getLeftTableExpressionNode(), excluded || kind == JoinKind::Right || kind == JoinKind::Full, result);
        collectTableExpressions(join->getRightTableExpressionNode(), excluded || kind == JoinKind::Left || kind == JoinKind::Full, result);
    }
    else if (auto * cross_join = node->as<CrossJoinNode>())
    {
        for (auto & table_expression : cross_join->getTableExpressions())
            collectTableExpressions(table_expression, excluded, result);
    }
    else if (auto * array_join = node->as<ArrayJoinNode>())
    {
        collectTableExpressions(array_join->getTableExpressionNode(), excluded, result);
    }
}

void collectConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    if (const auto * function = node->as<FunctionNode>(); function && function->getFunctionName() == "and")
    {
        for (const auto & argument : function->getArguments().getNodes())
            collectConjuncts(argument, result);
        return;
    }
    result.push_back(node);
}

/// Conditions that hold for every row of the enclosing query: the WHERE and the ON of inner joins.
void collectEnforcedConjuncts(const QueryTreeNodePtr & join_tree, QueryTreeNodes & result)
{
    if (const auto * join = join_tree->as<JoinNode>())
    {
        const auto strictness = join->getStrictness();
        if (join->getKind() == JoinKind::Inner && join->isOnJoinExpression()
            && (strictness == JoinStrictness::All || strictness == JoinStrictness::Any || strictness == JoinStrictness::Unspecified))
            collectConjuncts(join->getJoinExpression(), result);
        collectEnforcedConjuncts(join->getLeftTableExpressionNode(), result);
        collectEnforcedConjuncts(join->getRightTableExpressionNode(), result);
    }
    else if (const auto * cross_join = join_tree->as<CrossJoinNode>())
    {
        for (const auto & table_expression : cross_join->getTableExpressions())
            collectEnforcedConjuncts(table_expression, result);
    }
    else if (const auto * array_join = join_tree->as<ArrayJoinNode>())
    {
        collectEnforcedConjuncts(array_join->getTableExpressionNode(), result);
    }
}

/// A runtime filter is not built for a `GLOBAL` join.
bool hasGlobalJoin(const QueryTreeNodePtr & join_tree)
{
    if (const auto * join = join_tree->as<JoinNode>())
        return join->getLocality() == JoinLocality::Global || hasGlobalJoin(join->getLeftTableExpressionNode())
            || hasGlobalJoin(join->getRightTableExpressionNode());
    if (const auto * cross_join = join_tree->as<CrossJoinNode>())
        return std::ranges::any_of(cross_join->getTableExpressions(), hasGlobalJoin);
    if (const auto * array_join = join_tree->as<ArrayJoinNode>())
        return hasGlobalJoin(array_join->getTableExpressionNode());
    return false;
}

/// A `Join` table, a dictionary or a key-value storage on the right side of a join is looked up by the keys
/// instead of being read into a hash table, so no runtime filter is built from it. On the left side, it has no
/// row estimate, so the join sides are not swapped, and the runtime filter would restrict this table only.
bool isJoinedByLookup(const IQueryTreeNode * table_expression)
{
    StoragePtr storage;
    if (const auto * table = table_expression->as<TableNode>())
        storage = table->getStorage();
    else if (const auto * table_function = table_expression->as<TableFunctionNode>())
        storage = table_function->getStorage();
    if (!storage)
        return false;
    return castStorage<StorageJoin>(storage, DeferredTable::Load) || castStorage<StorageDictionary>(storage, DeferredTable::Load)
        || castStorage<IKeyValueEntity>(storage, DeferredTable::Load);
}

std::pair<const ColumnNode *, const ColumnNode *> asColumnEquality(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    if (!function || function->getFunctionName() != "equals" || function->getArguments().getNodes().size() != 2)
        return {};
    const auto * lhs = function->getArguments().getNodes()[0]->as<ColumnNode>();
    const auto * rhs = function->getArguments().getNodes()[1]->as<ColumnNode>();
    if (!lhs || !rhs)
        return {};
    return {lhs, rhs};
}

/// Subqueries are passed to `on_subquery` and not entered.
template <typename OnNode, typename OnSubquery>
void visitExpression(QueryTreeNodePtr & node, OnNode && on_node, OnSubquery && on_subquery)
{
    const auto type = node->getNodeType();
    if (type == QueryTreeNodeType::QUERY || type == QueryTreeNodeType::UNION)
    {
        on_subquery(node);
        return;
    }
    on_node(node);
    for (auto & child : node->getChildren())
        if (child)
            visitExpression(child, on_node, on_subquery);
}

template <typename OnNode, typename OnSubquery>
void visitQueryExpressions(QueryTreeNodePtr & query_node, OnNode && on_node, OnSubquery && on_subquery)
{
    for (auto & child : query_node->getChildren())
        if (child)
            visitExpression(child, on_node, on_subquery);
}

bool subqueryReadsTable(const QueryTreeNodePtr & subquery, const IQueryTreeNode * table_expression)
{
    const auto * query = subquery->as<QueryNode>();
    if (!query)
        return true;
    for (const auto & column : query->getCorrelatedColumns().getNodes())
        if (column->as<ColumnNode &>().getColumnSourceOrNull().get() == table_expression)
            return true;
    return false;
}

bool isDeterministicExpression(QueryTreeNodePtr node)
{
    bool result = true;
    visitExpression(
        node,
        [&](QueryTreeNodePtr & current)
        {
            const auto * function = current->as<FunctionNode>();
            if (!function || !function->isOrdinaryFunction())
                return;
            const auto & function_base = function->getFunctionOrThrow();
            if (!function_base->isDeterministic() || function_base->getName() == "arrayJoin")
                result = false;
        },
        [&](QueryTreeNodePtr &) { result = false; });
    return result;
}

/// The rule of the filter push-down below a join: functions that are deterministic in the scope of the query,
/// no `arrayJoin`. A subquery is computed once for the query, e.g. the set of `IN`.
bool isDeterministicInScopeOfQueryExpression(QueryTreeNodePtr node)
{
    bool result = true;
    visitExpression(
        node,
        [&](QueryTreeNodePtr & current)
        {
            const auto * function = current->as<FunctionNode>();
            if (!function || !function->isOrdinaryFunction())
                return;
            const auto & function_base = function->getFunctionOrThrow();
            if (!function_base->isDeterministicInScopeOfQuery() || function_base->getName() == "arrayJoin")
                result = false;
        },
        [](QueryTreeNodePtr &) {});
    return result;
}

bool sameTable(const TableNode & lhs, const TableNode & rhs)
{
    return lhs.getStorageID() == rhs.getStorageID() && lhs.getTableExpressionModifiers() == rhs.getTableExpressionModifiers();
}

bool isColumnOf(const ColumnNode & column, const ColumnNode & other)
{
    return column.getColumnName() == other.getColumnName() && column.getColumnSourceOrNull() == other.getColumnSourceOrNull();
}

struct Candidate
{
    QueryTreeNodePtr * slot = nullptr;
    const IQueryTreeNode * target = nullptr;
    QueryTreeNodePtr subquery_table;
    /// Same names in the subquery and in the target.
    Names keys;
    /// Correlated columns of the subquery. A rewrite replaces the columns of its target in the whole query, but
    /// a target is not rewritten while a subquery of another candidate is correlated to it, so these stay valid.
    std::vector<const ColumnNode *> outer_keys;
    QueryTreeNodes conditions;
    /// Whether a row of the target may have no row satisfying the conditions of the subquery (the conditions
    /// or a NULL key). The decorrelated subquery returns NULL then, not the aggregate over no rows.
    bool may_match_nothing = false;
};

class CorrelatedScalarAggregateToWindowVisitor : public InDepthQueryTreeVisitorWithContext<CorrelatedScalarAggregateToWindowVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CorrelatedScalarAggregateToWindowVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (node->as<QueryNode>() && getSettings()[Setting::optimize_correlated_scalar_aggregate_to_window])
            rewriteQuery(node);
    }

private:
    size_t window_counter = 0;

    void rewriteQuery(QueryTreeNodePtr & query_node_ptr)
    {
        auto & query_node = query_node_ptr->as<QueryNode &>();
        /// Filters defined per table alias would make two reads of the same table see different rows.
        if (query_node.getPrewhere() || !query_node.getJoinTreeNode() || !getSettings()[Setting::additional_table_filters].value.empty())
            return;

        TableExpressions tables;
        collectTableExpressions(query_node.getJoinTreeNode(), false, tables);

        QueryTreeNodes enforced;
        if (query_node.getWhere())
            collectConjuncts(query_node.getWhere(), enforced);
        collectEnforcedConjuncts(query_node.getJoinTreeNode(), enforced);

        /// With aggregation the subquery value is only usable before it, in WHERE.
        const bool has_aggregation = query_node.hasGroupBy() || query_node.hasHaving()
            || hasAggregateFunctionNodes(query_node.getProjectionNode()) || hasAggregateFunctionNodes(query_node.getOrderByNode());

        std::vector<Candidate> candidates;
        /// Expressions may be shared (e.g. by the projection and `ORDER BY ALL`), each slot must be rewritten once.
        std::unordered_set<const QueryTreeNodePtr *> visited_slots;
        auto collect = [&](QueryTreeNodePtr & expression)
        {
            visitExpression(
                expression,
                [](QueryTreeNodePtr &) {},
                [&](QueryTreeNodePtr & subquery)
                {
                    if (!visited_slots.insert(&subquery).second)
                        return;
                    if (auto candidate = matchSubquery(subquery, tables, enforced))
                        candidates.push_back(std::move(*candidate));
                });
        };
        if (query_node.getWhere())
            collect(query_node.getWhere());
        if (!has_aggregation)
        {
            collect(query_node.getProjectionNode());
            if (query_node.hasOrderBy())
                collect(query_node.getOrderByNode());
        }
        if (candidates.empty())
            return;

        std::unordered_map<const IQueryTreeNode *, std::vector<Candidate *>> by_target;
        for (auto & candidate : candidates)
            by_target[candidate.target].push_back(&candidate);

        const QueryPlanOptimizationSettings optimization_settings(getContext());
        /// Without the filter push-down, no condition restricts the windows.
        if (!optimization_settings.filter_push_down)
            return;
        const JoinSettings join_settings(getSettings());
        const auto & join_algorithms = join_settings.join_algorithms;
        const bool can_run_grace_hash = join_settings.canRunGraceHash();
        /// A runtime filter restricts only the probe (left) side of a join. With the automatic swap of the join sides,
        /// the larger table becomes the probe side, and the window over a smaller one is small. With a fixed choice,
        /// the target may stay on the build side, as without the join order optimization, which makes the swap.
        /// An `ie_join` listed first takes the joins with inequalities, and builds no runtime filter.
        const bool runtime_join_filters = optimization_settings.enable_join_runtime_filters
            /// The conditions come only from the ON of `ALL` and `ANY` joins, see `collectEnforcedConjuncts`.
            && std::ranges::any_of(join_algorithms, [can_run_grace_hash](JoinAlgorithm join_algorithm)
                { return QueryPlanOptimizations::supportsRuntimeFilter(join_algorithm, can_run_grace_hash, JoinStrictness::All); })
            && !optimization_settings.join_swap_table.has_value()
            && optimization_settings.query_plan_optimize_join_order_limit > 0
            && join_algorithms.front() != JoinAlgorithm::IE_JOIN
            && !hasGlobalJoin(query_node.getJoinTreeNode());

        for (auto & [target, target_candidates] : by_target)
        {
            /// Any other subquery reading the target (including candidates for other targets) would still
            /// refer to the table moved into the derived table.
            bool has_other_references = false;
            visitQueryExpressions(
                query_node_ptr,
                [](QueryTreeNodePtr &) {},
                [&](QueryTreeNodePtr & subquery)
                {
                    for (const auto * candidate : target_candidates)
                        if (candidate->slot->get() == subquery.get())
                            return;
                    has_other_references |= subqueryReadsTable(subquery, target);
                });

            /// The windows are computed one after another, a filter passes all of them only if it is on the keys of each.
            Names common_keys = target_candidates.front()->keys;
            for (const auto * candidate : target_candidates)
                std::erase_if(common_keys, [&](const String & key) { return !std::ranges::contains(candidate->keys, key); });

            /// A candidate that is not rewritten would refer to the table moved into the derived table.
            if (!has_other_references && mayRestrictKeys(target, enforced, common_keys, runtime_join_filters))
                rewriteTarget(query_node_ptr, *tables.at(target).slot, target_candidates);
        }
    }

    std::optional<Candidate> matchSubquery(QueryTreeNodePtr & subquery_node, const TableExpressions & tables, const QueryTreeNodes & enforced) const
    {
        auto * subquery = subquery_node->as<QueryNode>();
        if (!subquery || !subquery->isCorrelated() || subquery->hasGroupBy() || subquery->hasHaving() || subquery->hasWindow()
            || subquery->hasQualify() || subquery->hasOrderBy() || subquery->hasLimit() || subquery->hasOffset()
            || subquery->hasLimitAfter() || subquery->hasLimitUntil()
            || subquery->hasLimitBy() || subquery->isDistinct() || subquery->isGroupByWithTotals() || subquery->hasSettingsChanges()
            || subquery->getPrewhere() || !subquery->getWhere() || subquery->getProjection().getNodes().size() != 1)
            return {};

        auto subquery_table = subquery->getJoinTreeNode();
        const auto * table = subquery_table->as<TableNode>();
        if (!table || !table->getStorage()->isMergeTree())
            return {};

        auto projection = subquery->getProjection().getNodes().front();
        auto aggregates = collectAggregateFunctionNodes(projection);
        if (aggregates.empty() || hasWindowFunctionNodes(projection) || !isDeterministicExpression(projection))
            return {};
        for (const auto & aggregate_node : aggregates)
        {
            const auto & aggregate = aggregate_node->as<FunctionNode &>();
            if (hasAggregateFunctionNodes(aggregate.getArgumentsNode()))
                return {};
            /// The window function is created again by name, but the `Null` combinator resolves an aggregate over only NULLs
            /// (e.g. `uniqUpTo(5)(NULL)`) to a `nothing` function with parameters, which it does not take.
            const auto & name = aggregate.getFunctionName();
            if (!aggregate.getParameters().getNodes().empty()
                && (name == NameAggregateFunctionNothing::name || name == NameAggregateFunctionNothingNull::name
                    || name == NameAggregateFunctionNothingUInt64::name))
                return {};
            /// `count(x)` of a Nullable `x` is not supported as a window function.
            if (aggregate.getFunctionName() == "count")
                for (const auto & argument : aggregate.getArguments().getNodes())
                    if (isNullableOrLowCardinalityNullable(argument->getResultType()))
                        return {};
        }

        Candidate candidate;
        candidate.slot = &subquery_node;
        candidate.subquery_table = subquery_table;

        QueryTreeNodes conjuncts;
        collectConjuncts(subquery->getWhere(), conjuncts);
        for (const auto & conjunct : conjuncts)
        {
            auto [lhs, rhs] = asColumnEquality(conjunct);
            if (lhs && rhs)
            {
                if (lhs->getColumnSourceOrNull() != subquery_table)
                    std::swap(lhs, rhs);
                if (lhs->getColumnSourceOrNull() == subquery_table && rhs->getColumnSourceOrNull() != subquery_table)
                {
                    if (!tables.contains(rhs->getColumnSourceOrNull().get()))
                        return {};
                    candidate.keys.push_back(lhs->getColumnName());
                    candidate.outer_keys.push_back(rhs);
                    continue;
                }
            }

            if (hasUnknownColumn(conjunct, subquery_table) || !isDeterministicExpression(conjunct))
                return {};
            candidate.conditions.push_back(conjunct);
        }
        if (candidate.keys.empty())
            return {};

        for (const auto & correlated : subquery->getCorrelatedColumns().getNodes())
        {
            const auto & column = correlated->as<ColumnNode &>();
            if (std::none_of(candidate.outer_keys.begin(), candidate.outer_keys.end(), [&](const auto * key) { return isColumnOf(*key, column); }))
                return {};
        }

        /// Outer keys in aggregate arguments are replaced by the keys of the table, which also take values
        /// the outer keys do not: allowed only as the argument itself, which cannot throw.
        for (const auto & aggregate_node : aggregates)
        {
            for (auto argument : aggregate_node->as<FunctionNode &>().getArguments().getNodes())
            {
                if (argument->as<ColumnNode>())
                    continue;
                bool reads_outer_key = false;
                visitExpression(
                    argument,
                    [&](QueryTreeNodePtr & current)
                    {
                        if (const auto * column = current->as<ColumnNode>())
                            for (const auto * key : candidate.outer_keys)
                                reads_outer_key |= isColumnOf(*column, *key);
                    },
                    [](QueryTreeNodePtr &) {});
                if (reads_outer_key)
                    return {};
            }
        }

        /// The target: a read of the same table in the enclosing query, such that for every key the outer
        /// column equals the key column of that read in every row of the enclosing query.
        const IQueryTreeNode * target = nullptr;
        for (size_t i = 0; i < candidate.keys.size(); ++i)
        {
            const auto & key = candidate.keys[i];
            const auto & outer = *candidate.outer_keys[i];
            const IQueryTreeNode * key_target = nullptr;

            if (const auto * outer_table = outer.getColumnSourceOrNull()->as<TableNode>();
                outer_table && sameTable(*outer_table, *table) && outer.getColumnName() == key)
            {
                key_target = outer_table;
            }
            else
            {
                for (const auto & conjunct : enforced)
                {
                    auto [lhs, rhs] = asColumnEquality(conjunct);
                    if (!lhs || !rhs)
                        continue;
                    if (!isColumnOf(*lhs, outer))
                        std::swap(lhs, rhs);
                    if (!isColumnOf(*lhs, outer))
                        continue;
                    const auto * other_table = rhs->getColumnSourceOrNull()->as<TableNode>();
                    if (other_table && tables.contains(other_table) && sameTable(*other_table, *table) && rhs->getColumnName() == key
                        && rhs->getColumnType()->equals(*lhs->getColumnType()))
                    {
                        if (key_target && key_target != other_table)
                            return {};
                        key_target = other_table;
                    }
                }
            }

            if (!key_target || (target && target != key_target))
                return {};
            target = key_target;
        }

        if (tables.at(target).excluded)
            return {};

        /// Equal keys must be grouped as `=` compares them (not the case for floats).
        const auto & target_table = target->as<const TableNode &>();
        bool nullable_key = false;
        for (const auto & key : candidate.keys)
        {
            auto column = target_table.getStorageSnapshot()->tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical), key);
            if (!column || QueryPlanOptimizations::keyTypeBreaksHashSharding(*column->type))
                return {};
            /// A NULL inside a key (e.g. `Tuple(Nullable(Int32))` or `Variant`) makes `=` not true, but not the grouping.
            auto key_type = removeNullable(removeLowCardinality(column->type));
            if (anyInTypeTree(*key_type, [](const IDataType & type) { return type.isNullable() || isVariant(type); }))
                return {};
            nullable_key |= isNullableOrLowCardinalityNullable(column->type);
        }
        candidate.may_match_nothing = nullable_key || !candidate.conditions.empty();

        if (candidate.may_match_nothing)
        {
            if (!isNullableOrLowCardinalityNullable(subquery->getResultType()))
                return {};
            /// The expression is also evaluated for rows without a match, where it could throw, unless it is short-circuited.
            bool short_circuit = getSettings()[Setting::short_circuit_function_evaluation] != ShortCircuitFunctionEvaluation::DISABLE;
            if (!short_circuit && (!projection->as<FunctionNode>() || !projection->as<FunctionNode &>().isAggregateFunction()))
                return {};
            /// The conditions are also evaluated for rows with a NULL key, which the subquery skips, unless they are short-circuited.
            if (!short_circuit && nullable_key && !candidate.conditions.empty())
                return {};
            for (const auto & aggregate_node : aggregates)
            {
                const auto & aggregate = aggregate_node->as<FunctionNode &>();
                /// The conditions are added with the `-If` combinator, which cannot be nested.
                if (aggregate.getFunctionName().ends_with("If"))
                    return {};
                /// The window evaluates the arguments also for rows the conditions or a NULL key skip, which could throw.
                for (const auto & argument : aggregate.getArguments().getNodes())
                    if (!argument->as<ColumnNode>() && !argument->as<ConstantNode>())
                        return {};
            }
        }

        candidate.target = target;
        return candidate;
    }

    /// The windows read the whole table unless a condition restricts `keys`, which all of them are partitioned by:
    /// - a deterministic one depending only on its keys, which is pushed below the window (as in the filter push-down
    ///   below a window, a non-deterministic one is not pushed, because it would also remove rows inside the partitions
    ///   it keeps);
    /// - one only on tables joined to the target on one of its keys (directly or through other joined tables), when
    ///   the join can build a runtime filter: it is on the join key, and only a filter on the `PARTITION BY` keys is
    ///   pushed below the window. The runtime filter is not built for a small probe side, but then the window is small.
    ///   The condition must also be one the filter push-down below a join moves to the joined table, otherwise it
    ///   does not shrink the table the runtime filter is built from. A table joined by lookups builds no runtime filter.
    /// Otherwise the decorrelated plan, which needs memory per key only, is better.
    static bool mayRestrictKeys(const IQueryTreeNode * target, const QueryTreeNodes & enforced, const Names & keys, bool runtime_join_filters)
    {
        auto is_key = [&](const ColumnNode & column) { return std::ranges::contains(keys, column.getColumnName()); };

        /// The tables joined to the target on one of its keys, directly or through other joined tables.
        std::unordered_set<const IQueryTreeNode *> joined_on_keys;
        auto reached = [&](const ColumnNode & column, const IQueryTreeNode * source)
        { return (source == target && is_key(column)) || joined_on_keys.contains(source); };
        /// A runtime filter is not built for a join with a key pair without a common supertype, also a computed one.
        std::set<std::pair<const IQueryTreeNode *, const IQueryTreeNode *>> joins_without_filter;
        auto column_sources = [](QueryTreeNodePtr node)
        {
            std::unordered_set<const IQueryTreeNode *> sources;
            visitExpression(
                node,
                [&](QueryTreeNodePtr & current)
                {
                    if (const auto * column = current->as<ColumnNode>())
                        sources.insert(column->getColumnSourceOrNull().get());
                },
                [](QueryTreeNodePtr &) {});
            return sources;
        };
        for (const auto & conjunct : enforced)
        {
            const auto * function = conjunct->as<FunctionNode>();
            if (!function || function->getFunctionName() != "equals" || function->getArguments().getNodes().size() != 2)
                continue;
            const auto & arguments = function->getArguments().getNodes();
            if (tryGetLeastSupertype(DataTypes{arguments[0]->getResultType(), arguments[1]->getResultType()}))
                continue;
            const auto rhs_sources = column_sources(arguments[1]);
            for (const auto * lhs_source : column_sources(arguments[0]))
                for (const auto * rhs_source : rhs_sources)
                    joins_without_filter.insert(std::minmax(lhs_source, rhs_source));
        }

        auto join = [&](const ColumnNode & from, const IQueryTreeNode * from_source, const IQueryTreeNode * to_source)
        {
            return reached(from, from_source) && to_source != target && !joined_on_keys.contains(to_source)
                && !isJoinedByLookup(to_source) && joined_on_keys.insert(to_source).second;
        };
        for (bool changed = true; changed;)
        {
            changed = false;
            for (const auto & conjunct : enforced)
            {
                auto [lhs, rhs] = asColumnEquality(conjunct);
                if (!lhs || !rhs)
                    continue;
                const auto * lhs_source = lhs->getColumnSourceOrNull().get();
                const auto * rhs_source = rhs->getColumnSourceOrNull().get();
                if (!lhs_source || !rhs_source || joins_without_filter.contains(std::minmax(lhs_source, rhs_source)))
                    continue;
                changed |= join(*lhs, lhs_source, rhs_source);
                changed |= join(*rhs, rhs_source, lhs_source);
            }
        }

        bool joined_table_condition = false;
        for (const auto & conjunct : enforced)
        {
            if (auto [lhs, rhs] = asColumnEquality(conjunct); lhs && rhs)
                continue;

            /// The table expressions the condition depends on, `nullptr` for an unknown one.
            std::unordered_set<const IQueryTreeNode *> sources;
            bool only_keys = true;
            QueryTreeNodePtr node = conjunct;
            visitExpression(
                node,
                [&](QueryTreeNodePtr & current)
                {
                    const auto * column = current->as<ColumnNode>();
                    if (!column)
                        return;
                    auto source = column->getColumnSourceOrNull();
                    if (source && source->as<LambdaNode>())
                        return;
                    sources.insert(source.get());
                    if (source.get() == target)
                        only_keys &= is_key(*column);
                },
                [&](QueryTreeNodePtr & subquery)
                {
                    const auto * query = subquery->as<QueryNode>();
                    if (!query)
                    {
                        sources.insert(nullptr);
                        return;
                    }
                    for (const auto & column : query->getCorrelatedColumns().getNodes())
                    {
                        const auto * source = column->as<ColumnNode &>().getColumnSourceOrNull().get();
                        sources.insert(source);
                        if (source == target)
                            only_keys = false;
                    }
                });

            if (sources.size() == 1 && sources.contains(target) && only_keys && isDeterministicExpression(conjunct))
                return true;
            joined_table_condition |= !sources.empty()
                && std::ranges::all_of(sources, [&](const IQueryTreeNode * source) { return joined_on_keys.contains(source); })
                && isDeterministicInScopeOfQueryExpression(conjunct);
        }
        return joined_table_condition && runtime_join_filters;
    }

    void rewriteTarget(QueryTreeNodePtr & query_node_ptr, QueryTreeNodePtr & target_slot, const std::vector<Candidate *> & candidates)
    {
        auto target = std::static_pointer_cast<ITableExpressionNode>(target_slot);
        const auto & storage_snapshot = target->as<TableNode &>().getStorageSnapshot();
        const auto context = getContext();

        NamesAndTypes columns;
        std::unordered_map<String, NameAndTypePair> column_by_name;
        bool supported = true;
        auto add_column = [&](const ColumnNode & column)
        {
            if (column.hasExpression())
                supported = false;
            if (!supported || column_by_name.contains(column.getColumnName()))
                return;
            auto physical = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(), column.getColumnName());
            if (!physical)
            {
                supported = false;
                return;
            }
            column_by_name.emplace(physical->name, *physical);
            columns.push_back(*physical);
        };
        auto add_columns_of = [&](QueryTreeNodePtr & query, const IQueryTreeNode * source)
        {
            visitQueryExpressions(
                query,
                [&](QueryTreeNodePtr & current)
                {
                    if (const auto * column = current->as<ColumnNode>(); column && column->getColumnSourceOrNull().get() == source)
                        add_column(*column);
                },
                [](QueryTreeNodePtr &) {});
        };
        add_columns_of(query_node_ptr, target.get());
        for (const auto * candidate : candidates)
            add_columns_of(*candidate->slot, candidate->subquery_table.get());
        for (const auto * candidate : candidates)
            for (const auto & key : candidate->keys)
                if (!column_by_name.contains(key))
                    supported = false;
        if (!supported)
            return;

        auto derived = std::static_pointer_cast<QueryNode>(buildSubqueryToReadColumnsFromTableExpression(columns, target, context));
        derived->setAlias(fmt::format("{}_window_{}", target->getAlias(), window_counter++));
        NamesAndTypes projection_columns = columns;
        size_t next_window_column = 0;

        auto add_window_function = [&](const Candidate & candidate, const String & name, NullsAction nulls_action, QueryTreeNodes parameters, QueryTreeNodes arguments)
        {
            auto window = std::make_shared<WindowNode>(WindowFrame{});
            for (const auto & key : candidate.keys)
                window->getPartitionBy().getNodes().push_back(std::make_shared<ColumnNode>(column_by_name.at(key), target));

            auto function = std::make_shared<FunctionNode>(name);
            function->setNullsAction(nulls_action);
            function->getParameters().getNodes() = std::move(parameters);
            function->getArguments().getNodes() = std::move(arguments);
            function->getWindowNode() = window;

            Array parameter_values;
            for (const auto & parameter : function->getParameters().getNodes())
                parameter_values.push_back(parameter->as<ConstantNode &>().getValue());
            DataTypes argument_types;
            for (const auto & argument : function->getArguments().getNodes())
                argument_types.push_back(argument->getResultType());
            AggregateFunctionProperties properties;
            function->resolveAsWindowFunction(AggregateFunctionFactory::instance().get(
                name, nulls_action, argument_types, parameter_values, properties, AggregateFunctionStateVariant::Window));

            String column_name;
            do
                column_name = fmt::format("__correlated_aggregate_{}", next_window_column++);
            while (column_by_name.contains(column_name));
            NameAndTypePair column{column_name, function->getResultType()};
            derived->getProjection().getNodes().push_back(function);
            projection_columns.push_back(column);
            return column;
        };

        auto resolve_function = [&](const String & name, QueryTreeNodes arguments)
        {
            auto function = std::make_shared<FunctionNode>(name);
            function->getArguments().getNodes() = std::move(arguments);
            resolveOrdinaryFunctionNodeByName(*function, name, context);
            return function;
        };

        for (auto * candidate : candidates)
        {
            /// Outer keys equal the keys of the table for all rows of the subquery.
            auto to_target = [&](const QueryTreeNodePtr & node)
            {
                auto copy = node->clone();
                visitExpression(
                    copy,
                    [&](QueryTreeNodePtr & current)
                    {
                        const auto * column = current->as<ColumnNode>();
                        if (!column)
                            return;
                        if (column->getColumnSourceOrNull() == candidate->subquery_table)
                        {
                            current = std::make_shared<ColumnNode>(column->getColumn(), target);
                            return;
                        }
                        for (size_t i = 0; i < candidate->keys.size(); ++i)
                            if (isColumnOf(*column, *candidate->outer_keys[i]))
                                current = std::make_shared<ColumnNode>(column_by_name.at(candidate->keys[i]), target);
                    },
                    [](QueryTreeNodePtr &) {});
                return copy;
            };

            QueryTreeNodePtr condition;
            if (candidate->may_match_nothing)
            {
                QueryTreeNodes conditions;
                /// First, so that the conditions of the subquery are short-circuited for rows with a NULL key, as it skips them.
                for (const auto & key : candidate->keys)
                    if (isNullableOrLowCardinalityNullable(column_by_name.at(key).type))
                        conditions.push_back(resolve_function("isNotNull", {std::make_shared<ColumnNode>(column_by_name.at(key), target)}));
                for (const auto & subquery_condition : candidate->conditions)
                    conditions.push_back(to_target(subquery_condition));
                conditions.push_back(std::make_shared<ConstantNode>(Field(UInt8(1))));
                condition = resolve_function("and", std::move(conditions));
                if (condition->getResultType()->isNullable())
                    condition = resolve_function("ifNull", {condition, std::make_shared<ConstantNode>(Field(UInt8(0)))});
            }

            std::optional<NameAndTypePair> matching_rows;
            if (condition)
                matching_rows = add_window_function(*candidate, "countIf", NullsAction::EMPTY, {}, {condition->clone()});

            auto & subquery = (*candidate->slot)->as<QueryNode &>();
            const auto result_type = subquery.getResultType();
            auto expression = subquery.getProjection().getNodes().front();
            std::unordered_map<const IQueryTreeNode *, NameAndTypePair> aggregate_to_column;
            for (const auto & aggregate_node : collectAggregateFunctionNodes(expression))
            {
                const auto & aggregate = aggregate_node->as<FunctionNode &>();
                QueryTreeNodes parameters;
                for (const auto & parameter : aggregate.getParameters().getNodes())
                    parameters.push_back(parameter->clone());
                QueryTreeNodes arguments;
                for (const auto & argument : aggregate.getArguments().getNodes())
                    arguments.push_back(to_target(argument));
                if (condition)
                    arguments.push_back(condition->clone());
                aggregate_to_column[aggregate_node.get()] = add_window_function(
                    *candidate, aggregate.getFunctionName() + (condition ? "If" : ""), aggregate.getNullsAction(),
                    std::move(parameters), std::move(arguments));
            }

            visitExpression(
                expression,
                [&](QueryTreeNodePtr & current)
                {
                    if (auto it = aggregate_to_column.find(current.get()); it != aggregate_to_column.end())
                        current = std::make_shared<ColumnNode>(it->second, derived);
                },
                [](QueryTreeNodePtr &) {});
            if (matching_rows)
            {
                auto has_rows = resolve_function(
                    "greater", {std::make_shared<ColumnNode>(*matching_rows, derived), std::make_shared<ConstantNode>(Field(UInt64(0)))});
                expression = resolve_function("if", {has_rows, expression, std::make_shared<ConstantNode>(Field(), result_type)});
            }
            if (!expression->getResultType()->equals(*result_type))
                expression = buildCastFunction(expression, result_type, context);
            *candidate->slot = expression;
        }

        derived->resolveProjectionColumns(std::move(projection_columns));

        std::unordered_map<std::string, QueryTreeNodePtr> column_name_to_node;
        for (const auto & column : columns)
            column_name_to_node[column.name] = std::make_shared<ColumnNode>(column, derived);
        replaceColumns(query_node_ptr, target, column_name_to_node);
        target_slot = derived;
    }
};

}

void CorrelatedScalarAggregateToWindowPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// A materialized view replaces the table by the inserted block after the passes, also in the derived table.
    if (!context->getSettingsRef()[Setting::optimize_correlated_scalar_aggregate_to_window] || context->getViewSource())
        return;

    CorrelatedScalarAggregateToWindowVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
