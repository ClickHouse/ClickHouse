#include <Analyzer/Passes/FuseSiblingAggregateSubqueriesPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

#include <Poco/String.h>

#include <array>
#include <optional>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 cross_to_inner_join_rewrite;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsString force_optimize_projection_name;
    extern const SettingsUInt64 max_bytes_in_join;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsUInt64 max_rows_in_join;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_temporary_columns;
    extern const SettingsUInt64 max_temporary_non_const_columns;
    extern const SettingsBool optimize_fuse_sibling_aggregate_subqueries;
}

namespace
{

using NodeSet = std::unordered_set<const IQueryTreeNode *>;

class RepointSiblingReferencesVisitor : public InDepthQueryTreeVisitor<RepointSiblingReferencesVisitor>
{
public:
    explicit RepointSiblingReferencesVisitor(const IQueryTreeNode::ReplacementMap & substitution_) : substitution(substitution_) { }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node || column_node->hasExpression())
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        auto it = substitution.find(column_source->asTableExpression());
        if (it == substitution.end())
            return;

        auto repointed = std::make_shared<ColumnNode>(column_node->getColumn(), it->second);
        repointed->setAlias(column_node->getAlias());
        node = std::move(repointed);
    }

private:
    const IQueryTreeNode::ReplacementMap & substitution;
};

void collectConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    const auto * function_node = node->as<FunctionNode>();
    if (function_node && function_node->getFunctionName() == "and")
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            collectConjuncts(argument, conjuncts);
        return;
    }

    conjuncts.push_back(node);
}

QueryTreeNodePtr makeLogicalFunction(const String & function_name, QueryTreeNodes arguments, const ContextPtr & context)
{
    if (arguments.size() == 1)
        return std::move(arguments.front());

    auto function_node = std::make_shared<FunctionNode>(function_name);
    function_node->getArguments().getNodes() = std::move(arguments);
    resolveOrdinaryFunctionNodeByName(*function_node, function_name, context);
    return function_node;
}

/// `Variant`, `Dynamic` and `JSON` dispatch on each row's own type. Comparison excludes them at the top level
/// (`FunctionsComparison.h`), but inside a tuple its elements are compared by a comparison of their own.
bool typeDispatchesPerRow(const IDataType & type)
{
    bool result = false;
    auto check = [&](const IDataType & nested)
    {
        WhichDataType which(nested);
        result |= which.isVariant() || which.isDynamic() || which.isObject();
    };
    check(type);
    type.forEachChild(check);
    return result;
}

/// Whether this expression yields a value for every row, whatever the column values are.
/// `IFunction::canThrow` cannot answer that: it falls back to short-circuit suitability, which reports a
/// cheap throwing function as not throwing. Hence the whitelist of shapes that are total by construction.
bool isTotalOnEveryRow(const QueryTreeNodePtr & root)
{
    static const std::unordered_set<std::string_view> total_function_names
        = {"and", "or", "not", "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals"};

    QueryTreeNodes nodes_to_visit{root};
    while (!nodes_to_visit.empty())
    {
        auto current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (current->as<ConstantNode>())
            continue;

        if (const auto * column_node = current->as<ColumnNode>())
        {
            if (column_node->hasExpression() || typeDispatchesPerRow(*column_node->getColumnType()))
                return false;
            continue;
        }

        if (const auto * function_node = current->as<FunctionNode>())
        {
            if (!function_node->isOrdinaryFunction() || !total_function_names.contains(function_node->getFunctionName()))
                return false;

            /// Only the standard resolver's adaptor exposes the `IFunction` that answers `canThrow`.
            const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(function_node->getFunction().get());
            if (!adaptor)
                return false;

            DataTypesWithConstInfo argument_types;
            for (const auto & argument_column : function_node->getArgumentColumns())
            {
                if (typeDispatchesPerRow(*argument_column.type))
                    return false;
                argument_types.push_back({argument_column.type, argument_column.column != nullptr});
            }

            if (adaptor->getFunction()->canThrow(argument_types))
                return false;
        }
        else if (!current->as<ListNode>())
            return false;

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }

    return true;
}

/// Whether the branch's expressions can be evaluated once for all siblings rather than once per branch:
/// separate `min(rand64())` and `max(rand64())` branches must go on drawing independent values.
bool isReproducibleBranch(const QueryTreeNodePtr & root)
{
    QueryTreeNodes nodes_to_visit{root};
    while (!nodes_to_visit.empty())
    {
        auto current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (const auto * function_node = current->as<FunctionNode>(); function_node && function_node->isOrdinaryFunction())
        {
            auto function_base = function_node->getFunction();
            if (!function_base || function_base->isStateful() || !function_base->isDeterministicInScopeOfQuery()
                || function_base->isServerConstant() || function_base->hasObservableSideEffects())
                return false;
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }

    return true;
}

/// Refuses an expression column, or one resolving outside `allowed_sources` (a correlated reference).
bool collectColumnSources(const QueryTreeNodePtr & root, const NodeSet & allowed_sources, NodeSet & used_sources)
{
    QueryTreeNodes nodes_to_visit{root};
    while (!nodes_to_visit.empty())
    {
        auto current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (const auto * column_node = current->as<ColumnNode>())
        {
            if (column_node->hasExpression())
                return false;

            auto column_source = column_node->getColumnSourceOrNull();
            if (!column_source || !allowed_sources.contains(column_source.get()))
                return false;

            used_sources.insert(column_source.get());
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }

    return true;
}

/// Whether every source returns stored values, with nothing evaluated on the way out and no predicate
/// attached to it after the passes run.
bool areSourcesAndColumnsSafe(const QueryTreeNodePtr & branch, const ContextPtr & context)
{
    /// `additional_table_filters` is matched by name and alias in the planner, so equal-looking sources can carry different filters.
    if (!context->getSettingsRef()[Setting::additional_table_filters].value.empty())
        return false;

    const auto & query_node = branch->as<const QueryNode &>();
    auto table_expressions = extractTableExpressions(query_node.getJoinTreeNodeTyped(), /*add_array_join=*/true);

    QueryTreeNodePtrWithHashIgnoreAliasesSet distinct_table_expressions;
    for (const auto & table_expression : table_expressions)
    {
        /// Two occurrences of one table compare equal below, so a column would bind to either one.
        QueryTreeNodePtr table_expression_node = table_expression;
        if (!distinct_table_expressions.insert(table_expression_node).second)
            return false;

        const auto * table_node = table_expression->as<TableNode>();
        if (!table_node)
            return false;

        if (!table_node->getStorage()->readsColumnsWithoutTransformations(table_node->getStorageSnapshot(), context))
            return false;

        if (table_node->getStorage()->readIsBoundedBySpanLimit(context))
            return false;

        if (table_node->getStorageSnapshot()->metadata->hasProjections())
            return false;

        /// An absolute SAMPLE is a function of the branch's own key condition, which fusion replaces.
        if (table_node->getTableExpressionModifiers().has_value())
            return false;

        const auto & storage_id = table_node->getStorageID();
        if (storage_id.hasDatabase()
            && context->getRowPolicyFilter(
                storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER))
            return false;
    }

    QueryTreeNodes nodes_to_visit{branch};
    while (!nodes_to_visit.empty())
    {
        auto current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (const auto * column_node = current->as<ColumnNode>())
        {
            auto column_source = column_node->getColumnSourceOrNull();
            if (const auto * source_table = column_source ? column_source->as<TableNode>() : nullptr)
            {
                /// A DEFAULT is evaluated in the reader for a part that does not store the column.
                const auto & columns = source_table->getStorageSnapshot()->metadata->getColumns();
                const auto & column_name = column_node->getColumnName();
                if (!columns.hasPhysical(column_name) || columns.hasDefault(column_name))
                    return false;
            }
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }

    return true;
}

/// A fusable branch returns exactly one row of argument-less aggregates over its FROM, with no clause
/// that could change its cardinality or its grouping.
bool isFusableBranch(const QueryTreeNodePtr & table_expression, const ContextPtr & context)
{
    const auto * query_node = table_expression->as<QueryNode>();
    if (!query_node)
        return false;

    if (!query_node->isSubquery() || query_node->isCTE() || query_node->isCorrelated() || query_node->isRecursiveWith())
        return false;

    if (query_node->hasWith() || query_node->hasPrewhere() || query_node->hasGroupBy() || query_node->hasHaving()
        || query_node->hasWindow() || query_node->hasQualify() || query_node->hasOrderBy() || query_node->hasInterpolate()
        || query_node->hasLimitBy() || query_node->hasLimitByLimit() || query_node->hasLimitByOffset() || query_node->hasLimit()
        || query_node->hasLimitAfter() || query_node->hasLimitUntil() || query_node->hasOffset())
        return false;

    if (query_node->isDistinct() || query_node->isGroupByWithTotals() || query_node->isGroupByWithRollup()
        || query_node->isGroupByWithCube() || query_node->isGroupByWithGroupingSets() || query_node->isGroupByAll()
        || query_node->isOrderByAll() || query_node->isLimitByAll() || query_node->isLimitWithTies())
        return false;

    if (query_node->hasSettingsChanges())
        return false;

    if (!query_node->hasWhere())
        return false;

    if (!query_node->getProjectionAliasesToOverride().empty())
        return false;

    const auto & projection_nodes = query_node->getProjection().getNodes();
    if (projection_nodes.empty() || projection_nodes.size() != query_node->getProjectionColumns().size())
        return false;

    for (const auto & projection_node : projection_nodes)
    {
        const auto * function_node = projection_node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction())
            return false;

        /// Under -If an argument is evaluated on every row the fused filter keeps, including the rows
        /// this branch's own filter excluded.
        if (!function_node->getArguments().getNodes().empty())
            return false;

        if (Poco::toLower(function_node->getFunctionName()).ends_with("if"))
            return false;
    }

    chassert(query_node->hasWhere());
    QueryTreeNodes conjuncts;
    collectConjuncts(query_node->getWhere(), conjuncts);
    for (const auto & conjunct : conjuncts)
        if (!isTotalOnEveryRow(conjunct))
            return false;

    if (!isReproducibleBranch(table_expression))
        return false;

    return areSourcesAndColumnsSafe(table_expression, context);
}

struct FusionPlan
{
    QueryTreeNodePtr where;
    QueryTreeNodes projection;
    NamesAndTypes projection_columns;
};

std::optional<FusionPlan> planFusion(const QueryTreeNodes & branches, const std::vector<size_t> & members, const ContextPtr & context)
{
    auto tableExpressionsOf = [](const QueryTreeNodePtr & branch)
    { return extractTableExpressions(branch->as<QueryNode &>().getJoinTreeNodeTyped(), /*add_array_join=*/true); };

    const auto base_table_expressions = tableExpressionsOf(branches[members.front()]);

    NodeSet base_sources;
    for (const auto & base_table_expression : base_table_expressions)
        base_sources.insert(base_table_expression.get());

    std::vector<QueryTreeNodes> branch_conjuncts;
    branch_conjuncts.reserve(members.size());
    for (auto member : members)
    {
        QueryTreeNodes conjuncts;
        const auto & branch = branches[member]->as<QueryNode &>();
        if (branch.hasWhere())
            collectConjuncts(branch.getWhere(), conjuncts);
        branch_conjuncts.push_back(std::move(conjuncts));
    }

    QueryTreeNodePtrWithHashIgnoreAliasesMap<size_t> branches_holding_conjunct;
    for (const auto & conjuncts : branch_conjuncts)
    {
        QueryTreeNodePtrWithHashIgnoreAliasesSet counted;
        for (const auto & conjunct : conjuncts)
            if (counted.insert(conjunct).second)
                ++branches_holding_conjunct[conjunct];
    }

    QueryTreeNodes common_conjuncts;
    QueryTreeNodePtrWithHashIgnoreAliasesSet common_conjuncts_set;
    for (const auto & conjunct : branch_conjuncts.front())
        if (branches_holding_conjunct[conjunct] == members.size() && common_conjuncts_set.insert(conjunct).second)
            common_conjuncts.push_back(conjunct);

    FusionPlan plan;
    QueryTreeNodes branch_conditions;
    NodeSet residual_sources;
    std::unordered_set<String> output_names;
    bool every_branch_has_a_residual = true;

    for (size_t member = 0; member < members.size(); ++member)
    {
        const auto & branch = branches[members[member]]->as<QueryNode &>();
        const auto branch_table_expressions
            = member == 0 ? base_table_expressions : tableExpressionsOf(branches[members[member]]);

        if (branch_table_expressions.size() != base_table_expressions.size())
            return {};

        IQueryTreeNode::ReplacementMap substitution;
        for (size_t position = 0; position < branch_table_expressions.size(); ++position)
        {
            /// Table-expression equality ignores the storage snapshot each sibling captured.
            const auto * branch_table = branch_table_expressions[position]->as<TableNode>();
            const auto * base_table = base_table_expressions[position]->as<TableNode>();
            if (!branch_table || !base_table || branch_table->getStorageSnapshot().get() != base_table->getStorageSnapshot().get())
                return {};

            if (member != 0)
                substitution.emplace(branch_table_expressions[position].get(), base_table_expressions[position]);
        }

        QueryTreeNodes residual_conjuncts;
        QueryTreeNodePtrWithHashIgnoreAliasesSet taken;
        for (const auto & conjunct : branch_conjuncts[member])
        {
            if (common_conjuncts_set.contains(conjunct) || !taken.insert(conjunct).second)
                continue;

            auto residual_conjunct = conjunct->cloneAndReplace(substitution);
            if (!collectColumnSources(residual_conjunct, base_sources, residual_sources))
                return {};

            residual_conjuncts.push_back(std::move(residual_conjunct));
        }

        QueryTreeNodePtr condition;
        if (residual_conjuncts.empty())
        {
            every_branch_has_a_residual = false;
        }
        else
        {
            /// Residuals over different tables lose their per-branch correlation inside the fused `OR`.
            if (residual_sources.size() > 1)
                return {};

            condition = makeLogicalFunction("and", residual_conjuncts, context);
            branch_conditions.push_back(condition);
        }

        const auto & branch_projection_columns = branch.getProjectionColumns();
        const auto & branch_projection_nodes = branch.getProjection().getNodes();
        if (branch_projection_columns.size() != branch_projection_nodes.size())
            return {};

        for (size_t position = 0; position < branch_projection_nodes.size(); ++position)
        {
            auto aggregate = branch_projection_nodes[position]->cloneAndReplace(substitution);
            const auto & output_column = branch_projection_columns[position];

            if (condition)
            {
                auto & function_node = aggregate->as<FunctionNode &>();
                const auto conditional_function_name = function_node.getFunctionName() + "If";
                /// Over the factory's length limit the lookup below throws instead of answering false.
                if (conditional_function_name.size() > AggregateFunctionFactory::MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
                    return {};
                if (!AggregateFunctionFactory::instance().isAggregateFunctionName(conditional_function_name))
                    return {};

                /// The factory unwraps `LowCardinality` and applies `Null` before -If validates its condition.
                const auto & condition_type = condition->getResultType();
                if (!isUInt8(removeNullable(removeLowCardinality(condition_type))) && !condition_type->onlyNull())
                    return {};

                function_node.getArguments().getNodes().push_back(condition->clone());
                resolveAggregateFunctionNodeByName(function_node, conditional_function_name);

                /// The combinator must not change the result type the enclosing query resolved against.
                if (!function_node.getResultType()->equals(*output_column.type))
                    return {};
            }

            aggregate->setAlias(output_column.name);
            if (!output_names.insert(output_column.name).second)
                return {};

            plan.projection.push_back(std::move(aggregate));
            plan.projection_columns.emplace_back(output_column.name, output_column.type);
        }
    }

    /// The fused filter keeps the rows at least one branch kept: the shared conjuncts, and the
    /// disjunction of the branches' own filters, which a branch with no residual makes true anyway.
    QueryTreeNodes where_conjuncts;
    for (const auto & conjunct : common_conjuncts)
        where_conjuncts.push_back(conjunct->clone());

    if (every_branch_has_a_residual && !branch_conditions.empty())
    {
        QueryTreeNodes disjuncts;
        for (const auto & condition : branch_conditions)
            disjuncts.push_back(condition->clone());
        where_conjuncts.push_back(makeLogicalFunction("or", std::move(disjuncts), context));
    }

    if (!where_conjuncts.empty())
        plan.where = makeLogicalFunction("and", std::move(where_conjuncts), context);

    return plan;
}

class FuseSiblingAggregateSubqueriesVisitor : public InDepthQueryTreeVisitorWithContext<FuseSiblingAggregateSubqueriesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FuseSiblingAggregateSubqueriesVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_fuse_sibling_aggregate_subqueries])
            return;

        /// With this setting an aggregation over an empty set returns no row at all, so a branch whose
        /// own filter matches nothing empties the whole cross join.
        if (getSettings()[Setting::empty_result_for_aggregation_by_empty_set])
            return;

        static constexpr std::array scope_changing_limits{
            &Setting::max_rows_to_read,
            &Setting::max_bytes_to_read,
            &Setting::max_rows_to_read_leaf,
            &Setting::max_bytes_to_read_leaf,
            &Setting::max_columns_to_read,
            &Setting::max_temporary_columns,
            &Setting::max_temporary_non_const_columns,
            &Setting::max_rows_in_join,
            &Setting::max_bytes_in_join,
        };
        for (const auto * limit : scope_changing_limits)
            if (getSettings()[*limit])
                return;

        /// The -If rewrite makes the read ineligible for the implicit minmax_count projection, and a
        /// forced projection turns a lost access path into an error.
        if (getSettings()[Setting::force_optimize_projection]
            || !getSettings()[Setting::force_optimize_projection_name].value.empty())
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & join_tree = query_node->getJoinTreeNode();
        auto * cross_join_node = join_tree->as<CrossJoinNode>();
        if (!cross_join_node)
            return;

        /// Every gap must be the same plain cross/comma join, so any subset can be re-joined the same way.
        const auto & join_types = cross_join_node->getJoinTypes();
        if (join_types.empty())
            return;
        for (const auto & join_type : join_types)
        {
            if (join_type.locality != JoinLocality::Unspecified || join_type.is_comma != join_types.front().is_comma)
                return;
        }

        /// At `cross_to_inner_join_rewrite = 2` CrossToInnerJoinPass rejects a comma join it cannot turn into
        /// an INNER JOIN, and it reaches that check only while the join tree is still a cross join.
        if (join_types.front().is_comma && getSettings()[Setting::cross_to_inner_join_rewrite] >= 2)
            return;

        auto branches = cross_join_node->getTableExpressions();

        std::vector<std::vector<size_t>> groups;
        QueryTreeNodePtrWithHashIgnoreAliasesMap<size_t> group_of_join_tree;
        for (size_t position = 0; position < branches.size(); ++position)
        {
            if (!isFusableBranch(branches[position], getContext()))
                continue;

            const auto & join_tree_of_branch = branches[position]->as<QueryNode &>().getJoinTreeNode();
            auto [it, inserted] = group_of_join_tree.emplace(join_tree_of_branch, groups.size());
            if (inserted)
                groups.push_back({position});
            else
                groups[it->second].push_back(position);
        }

        IQueryTreeNode::ReplacementMap fused_away;
        for (const auto & members : groups)
        {
            if (members.size() < 2)
                continue;

            auto plan = planFusion(branches, members, getContext());
            if (!plan)
                continue;

            /// From here on the tree is being changed, and every check has already passed.
            auto & base_branch = branches[members.front()]->as<QueryNode &>();
            base_branch.getWhere() = plan->where;
            base_branch.getProjection().getNodes() = std::move(plan->projection);
            base_branch.resolveProjectionColumns(std::move(plan->projection_columns));

            auto base_table_expression = std::static_pointer_cast<ITableExpressionNode>(branches[members.front()]);
            for (size_t member = 1; member < members.size(); ++member)
                fused_away.emplace(branches[members[member]]->asTableExpression(), base_table_expression);
        }

        if (fused_away.empty())
            return;

        QueryTreeNodes remaining_branches;
        for (const auto & branch : branches)
            if (!fused_away.contains(branch->asTableExpression()))
                remaining_branches.push_back(branch);

        /// Reassigning join_tree can drop the last reference to the node `join_types` belongs to.
        const auto remaining_join_type = join_types.front();
        const auto remaining_gaps = remaining_branches.size() - 1;

        if (remaining_branches.size() == 1)
            join_tree = remaining_branches.front();
        else
            join_tree = std::make_shared<CrossJoinNode>(
                std::move(remaining_branches), CrossJoinNode::JoinTypes(remaining_gaps, remaining_join_type));

        RepointSiblingReferencesVisitor visitor(fused_away);
        for (auto & child : node->getChildren())
            if (child)
                visitor.visit(child);
    }
};

}

void FuseSiblingAggregateSubqueriesPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FuseSiblingAggregateSubqueriesVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
