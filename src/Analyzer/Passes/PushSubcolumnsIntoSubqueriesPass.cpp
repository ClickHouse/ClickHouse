#include <Analyzer/Passes/PushSubcolumnsIntoSubqueriesPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <optional>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_push_subcolumns_into_subqueries;
}

namespace
{

/// Position of an expression relative to the aggregation step of the query.
enum class ClauseKind
{
    /// JOIN TREE (including JOIN ON and ARRAY JOIN expressions), WHERE, PREWHERE.
    /// Expressions here are evaluated directly on the rows exported by the subquery.
    PreAggregation,
    /// Projection, GROUP BY, HAVING, WINDOW, QUALIFY, ORDER BY, LIMIT BY.
    /// With GROUP BY or aggregate functions present, expressions here can be evaluated
    /// only over aggregation keys and aggregate function results.
    PostAggregation,
    /// INTERPOLATE expressions are evaluated over the output columns of the query
    /// during ORDER BY ... WITH FILL, replacing them with a column reference is not valid.
    Interpolate,
};

struct PushdownGroup
{
    /// The subquery exporting the column, an element of the JOIN TREE of the query.
    QueryTreeNodePtr source;
    String column_name;
    String subcolumn_path;
    /// Type of the column argument of getSubcolumn.
    DataTypePtr column_type;
    /// Result type of the getSubcolumn function.
    DataTypePtr subcolumn_type;
    /// False if at least one occurrence cannot be replaced. All occurrences of the same
    /// subcolumn are replaced together or not at all: replacing only some of them could
    /// desynchronize expressions that must stay equal, e.g. an aggregation key in the
    /// GROUP BY list and the same expression in the projection.
    bool viable = true;

    /// Set when the group is applied.
    String new_column_name;
    bool applied = false;
};

struct QueryProcessingState
{
    /// Subqueries and unions found while traversing the query, to be processed next.
    std::unordered_set<const IQueryTreeNode *> discovered_subqueries;
    QueryTreeNodes subqueries_to_visit;

    /// Table expressions of the JOIN TREE that are query nodes and are eligible
    /// as pushdown targets (in particular, not on a side of a JOIN that can be
    /// filled with default values for non-matched rows).
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> eligible_targets;

    std::vector<PushdownGroup> groups;

    bool collect_candidates = false;
    bool query_has_aggregation = false;
    QueryTreeNodes group_by_keys;

    void addSubqueryToVisit(const QueryTreeNodePtr & node)
    {
        if (discovered_subqueries.emplace(node.get()).second)
            subqueries_to_visit.push_back(node);
    }

    PushdownGroup * findGroup(const IQueryTreeNode * source, const String & column_name, const String & subcolumn_path)
    {
        for (auto & group : groups)
            if (group.source.get() == source && group.column_name == column_name && group.subcolumn_path == subcolumn_path)
                return &group;
        return nullptr;
    }
};

struct CandidateMatch
{
    ColumnNode * column_node;
    QueryTreeNodePtr column_source;
    String subcolumn_path;
};

/// Match `getSubcolumn(column, 'constant_path')` where the column comes from a query or union node.
std::optional<CandidateMatch> matchCandidate(FunctionNode & function_node)
{
    if (function_node.getFunctionName() != "getSubcolumn" || !function_node.isResolved())
        return {};

    auto & function_arguments = function_node.getArguments().getNodes();
    if (function_arguments.size() != 2)
        return {};

    auto * column_node = function_arguments[0]->as<ColumnNode>();
    if (!column_node)
        return {};

    auto column_source = column_node->getColumnSourceOrNull();
    if (!column_source || !isQueryOrUnionNode(column_source))
        return {};

    const auto * constant_node = function_arguments[1]->as<ConstantNode>();
    if (!constant_node)
        return {};

    auto constant_value = constant_node->getValue();
    if (constant_value.getType() != Field::Types::String)
        return {};

    auto subcolumn_path = constant_value.safeGet<String>();
    if (subcolumn_path.empty())
        return {};

    return CandidateMatch{column_node, std::move(column_source), std::move(subcolumn_path)};
}

/// Collect query and union table expressions of the JOIN TREE that can accept
/// additional projection columns. A table expression under LEFT/RIGHT/FULL/PASTE
/// JOIN can have its columns replaced with default values for non-matched rows,
/// and `getSubcolumn` of a default value is not always equal to the default value
/// of the subcolumn type (e.g. the `null` subcolumn of the default NULL value is 1,
/// while the default value of its UInt8 column is 0), so such table expressions
/// are not eligible.
void collectEligibleTargets(const QueryTreeNodePtr & join_tree_node, bool can_be_filled_with_defaults, QueryProcessingState & state)
{
    if (!join_tree_node)
        return;

    if (auto * join_node = join_tree_node->as<JoinNode>())
    {
        auto kind = join_node->getKind();
        bool left_defaultable = can_be_filled_with_defaults || kind == JoinKind::Right || kind == JoinKind::Full || kind == JoinKind::Paste;
        bool right_defaultable = can_be_filled_with_defaults || kind == JoinKind::Left || kind == JoinKind::Full || kind == JoinKind::Paste;

        collectEligibleTargets(join_node->getLeftTableExpressionNode(), left_defaultable, state);
        collectEligibleTargets(join_node->getRightTableExpressionNode(), right_defaultable, state);
        return;
    }

    if (auto * cross_join_node = join_tree_node->as<CrossJoinNode>())
    {
        for (const auto & table_expression : cross_join_node->getTableExpressions())
            collectEligibleTargets(table_expression, can_be_filled_with_defaults, state);
        return;
    }

    if (auto * array_join_node = join_tree_node->as<ArrayJoinNode>())
    {
        collectEligibleTargets(array_join_node->getTableExpressionNode(), can_be_filled_with_defaults, state);
        return;
    }

    if (join_tree_node->getNodeType() == QueryTreeNodeType::QUERY && !can_be_filled_with_defaults)
        state.eligible_targets.emplace(join_tree_node.get(), join_tree_node);
}

/// A projection column can be added to the subquery without changing its result:
/// - DISTINCT deduplicates over all projection columns;
/// - with GROUP BY or aggregate functions an additional non-aggregated column is not valid;
/// - ORDER BY ... WITH FILL and INTERPOLATE fill the added column with default values
///   in the filled rows, which is not always equal to `getSubcolumn` of the filled
///   original column.
bool canAddProjectionColumns(const QueryNode & subquery)
{
    if (subquery.isDistinct() || subquery.hasGroupBy() || subquery.hasInterpolate())
        return false;

    if (subquery.hasProjectionAliasesToOverride())
        return false;

    if (hasAggregateFunctionNodes(subquery.getProjectionNode())
        || (subquery.hasHaving() && hasAggregateFunctionNodes(subquery.getHaving()))
        || (subquery.hasOrderBy() && hasAggregateFunctionNodes(subquery.getOrderByNode())))
        return false;

    if (subquery.hasOrderBy())
    {
        for (const auto & sort_node : subquery.getOrderBy().getNodes())
        {
            if (auto * sort = sort_node->as<SortNode>(); sort && sort->withFill())
                return false;
        }
    }

    return true;
}

void collectCandidates(const QueryTreeNodePtr & node, ClauseKind clause_kind, bool inside_aggregate_function, QueryProcessingState & state)
{
    if (!node)
        return;

    if (isQueryOrUnionNode(node))
    {
        state.addSubqueryToVisit(node);
        return;
    }

    if (auto * function_node = node->as<FunctionNode>())
    {
        if (state.collect_candidates)
        {
            if (auto match = matchCandidate(*function_node))
            {
                auto target_it = state.eligible_targets.find(match->column_source.get());
                if (target_it != state.eligible_targets.end())
                {
                    const auto & column_name = match->column_node->getColumnName();
                    auto * group = state.findGroup(match->column_source.get(), column_name, match->subcolumn_path);
                    if (!group)
                    {
                        state.groups.push_back(PushdownGroup{
                            .source = target_it->second,
                            .column_name = column_name,
                            .subcolumn_path = match->subcolumn_path,
                            .column_type = match->column_node->getColumnType(),
                            .subcolumn_type = function_node->getResultType(),
                            .viable = true,
                            .new_column_name = {},
                            .applied = false});
                        group = &state.groups.back();
                    }

                    /// All occurrences must have the same types. Types can diverge e.g. when
                    /// group_by_use_nulls wraps an occurrence used as a GROUP BY key into Nullable.
                    if (!group->column_type->equals(*match->column_node->getColumnType())
                        || !group->subcolumn_type->equals(*function_node->getResultType()))
                        group->viable = false;

                    /// The occurrence can be replaced with a column reference when it is evaluated
                    /// directly over the rows exported by the subquery: anywhere if the query has no
                    /// aggregation, otherwise before the aggregation step (WHERE, JOIN TREE, arguments
                    /// of aggregate functions) or when the whole expression is an aggregation key.
                    bool replaceable = clause_kind != ClauseKind::Interpolate
                        && (!state.query_has_aggregation
                            || inside_aggregate_function
                            || clause_kind == ClauseKind::PreAggregation
                            || std::ranges::any_of(
                                state.group_by_keys,
                                [&](const auto & key) { return node->isEqual(*key, {.compare_aliases = false}); }));

                    if (!replaceable)
                        group->viable = false;
                }
            }
        }

        if (function_node->isAggregateFunction())
            inside_aggregate_function = true;
    }

    for (const auto & child : node->getChildren())
        collectCandidates(child, clause_kind, inside_aggregate_function, state);
}

/// Build the expression that reads the subcolumn inside the subquery, or nullptr if it cannot be built.
/// For a column read from a table, it is a direct reference to the subcolumn. For a column exported
/// by a deeper subquery, it is a `getSubcolumn` function that is pushed down further when that
/// subquery is processed.
QueryTreeNodePtr buildSubcolumnProjectionNode(const PushdownGroup & group, const QueryTreeNodePtr & inner_node, const ContextPtr & context)
{
    const auto * inner_column = inner_node->as<ColumnNode>();
    if (!inner_column || inner_column->hasExpression())
        return nullptr;

    auto inner_source = inner_column->getColumnSourceOrNull();
    if (!inner_source)
        return nullptr;

    auto * table_node = inner_source->as<TableNode>();
    auto * table_function_node = inner_source->as<TableFunctionNode>();

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
        if (!storage_snapshot->storage.supportsSubcolumns())
            return nullptr;

        auto subcolumn_full_name = inner_column->getColumnName() + "." + group.subcolumn_path;

        /// An ordinary column with the same name would shadow the subcolumn.
        if (storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All), subcolumn_full_name))
            return nullptr;

        auto subcolumn = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), subcolumn_full_name);
        if (!subcolumn || !subcolumn->type->equals(*group.subcolumn_type))
            return nullptr;

        return std::make_shared<ColumnNode>(NameAndTypePair{subcolumn_full_name, subcolumn->type}, inner_source);
    }

    if (isQueryOrUnionNode(inner_source))
    {
        auto function_node = std::make_shared<FunctionNode>("getSubcolumn");

        auto constant_value = ConstantValue{group.subcolumn_path, std::make_shared<DataTypeString>()};

        ColumnsWithTypeAndName argument_columns;
        argument_columns.push_back({nullptr, inner_column->getColumnType(), {}});
        argument_columns.push_back({constant_value.getColumn(), constant_value.getType(), {}});

        auto function = FunctionFactory::instance().get("getSubcolumn", context);
        auto function_base = function->build(argument_columns);
        if (!function_base->getResultType()->equals(*group.subcolumn_type))
            return nullptr;

        auto & function_arguments = function_node->getArguments().getNodes();
        function_arguments.push_back(inner_node->clone());
        function_arguments.push_back(std::make_shared<ConstantNode>(std::move(constant_value)));

        function_node->resolveAsFunction(std::move(function_base));
        return function_node;
    }

    return nullptr;
}

/// Add the subcolumn to the subquery projection. Returns false if the pushdown is not possible.
bool applyGroup(PushdownGroup & group, const ContextPtr & context)
{
    auto & subquery = group.source->as<QueryNode &>();

    const auto & projection_columns = subquery.getProjectionColumns();
    std::optional<size_t> projection_index;

    for (size_t i = 0; i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name == group.column_name)
        {
            /// The name must be unambiguous.
            if (projection_index)
                return false;
            projection_index = i;
        }
    }

    if (!projection_index)
        return false;

    /// The type of the column can differ from the type of the subquery projection column,
    /// e.g. when join_use_nulls wraps columns of the outer JOIN into Nullable.
    if (!projection_columns[*projection_index].type->equals(*group.column_type))
        return false;

    auto new_column_name = group.column_name + "." + group.subcolumn_path;
    for (const auto & projection_column : projection_columns)
    {
        if (projection_column.name == new_column_name)
            return false;
    }

    const auto & inner_node = subquery.getProjection().getNodes()[*projection_index];
    auto new_projection_node = buildSubcolumnProjectionNode(group, inner_node, context);
    if (!new_projection_node)
        return false;

    subquery.addProjectionColumn(std::move(new_projection_node), NameAndTypePair{new_column_name, group.subcolumn_type});

    group.new_column_name = std::move(new_column_name);
    group.applied = true;
    return true;
}

void replaceCandidates(QueryTreeNodePtr & node, QueryProcessingState & state)
{
    if (!node || isQueryOrUnionNode(node))
        return;

    if (auto * function_node = node->as<FunctionNode>())
    {
        if (auto match = matchCandidate(*function_node))
        {
            const auto * group = state.findGroup(match->column_source.get(), match->column_node->getColumnName(), match->subcolumn_path);
            if (group && group->applied)
            {
                node = std::make_shared<ColumnNode>(
                    NameAndTypePair{group->new_column_name, group->subcolumn_type},
                    std::static_pointer_cast<ITableExpressionNode>(group->source));
                return;
            }
        }
    }

    for (auto & child : node->getChildren())
        replaceCandidates(child, state);
}

void processQuery(QueryNode & query_node, QueryProcessingState & state)
{
    const auto & context = query_node.getContext();
    state.collect_candidates = context->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries];

    if (state.collect_candidates)
    {
        collectEligibleTargets(query_node.getJoinTreeNode(), false /*can_be_filled_with_defaults*/, state);

        for (auto it = state.eligible_targets.begin(); it != state.eligible_targets.end();)
        {
            if (canAddProjectionColumns(it->second->as<const QueryNode &>()))
                ++it;
            else
                it = state.eligible_targets.erase(it);
        }

        state.query_has_aggregation = query_node.hasGroupBy()
            || hasAggregateFunctionNodes(query_node.getProjectionNode())
            || (query_node.hasHaving() && hasAggregateFunctionNodes(query_node.getHaving()))
            || (query_node.hasOrderBy() && hasAggregateFunctionNodes(query_node.getOrderByNode()))
            || (query_node.hasQualify() && hasAggregateFunctionNodes(query_node.getQualify()));

        if (query_node.hasGroupBy())
        {
            for (const auto & key_node : query_node.getGroupBy().getNodes())
            {
                if (query_node.isGroupByWithGroupingSets())
                {
                    for (const auto & inner_key_node : key_node->as<ListNode &>().getNodes())
                        state.group_by_keys.push_back(inner_key_node);
                }
                else
                    state.group_by_keys.push_back(key_node);
            }
        }
    }

    const std::initializer_list<std::pair<QueryTreeNodePtr *, ClauseKind>> clauses = {
        {&query_node.getJoinTreeNode(), ClauseKind::PreAggregation},
        {&query_node.getPrewhere(), ClauseKind::PreAggregation},
        {&query_node.getWhere(), ClauseKind::PreAggregation},
        {&query_node.getWithNode(), ClauseKind::PostAggregation},
        {&query_node.getProjectionNode(), ClauseKind::PostAggregation},
        {&query_node.getGroupByNode(), ClauseKind::PostAggregation},
        {&query_node.getHaving(), ClauseKind::PostAggregation},
        {&query_node.getWindowNode(), ClauseKind::PostAggregation},
        {&query_node.getQualify(), ClauseKind::PostAggregation},
        {&query_node.getOrderByNode(), ClauseKind::PostAggregation},
        {&query_node.getInterpolate(), ClauseKind::Interpolate},
        {&query_node.getLimitByNode(), ClauseKind::PostAggregation},
        {&query_node.getLimitByLimit(), ClauseKind::PostAggregation},
        {&query_node.getLimitByOffset(), ClauseKind::PostAggregation},
        {&query_node.getLimit(), ClauseKind::PostAggregation},
        {&query_node.getOffset(), ClauseKind::PostAggregation},
    };

    for (const auto & [clause_node, clause_kind] : clauses)
        collectCandidates(*clause_node, clause_kind, false /*inside_aggregate_function*/, state);

    bool any_group_applied = false;
    for (auto & group : state.groups)
    {
        if (group.viable)
            any_group_applied |= applyGroup(group, context);
    }

    if (any_group_applied)
    {
        for (const auto & [clause_node, _] : clauses)
            replaceCandidates(*clause_node, state);
    }
}

}

void PushSubcolumnsIntoSubqueriesPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    QueryTreeNodes nodes_to_visit = {query_tree_node};
    std::unordered_set<const IQueryTreeNode *> visited_nodes;

    while (!nodes_to_visit.empty())
    {
        auto node_to_visit = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        if (!visited_nodes.emplace(node_to_visit.get()).second)
            continue;

        if (auto * union_node = node_to_visit->as<UnionNode>())
        {
            for (const auto & union_query_node : union_node->getQueries().getNodes())
                nodes_to_visit.push_back(union_query_node);
            continue;
        }

        auto * query_node = node_to_visit->as<QueryNode>();
        if (!query_node)
            continue;

        QueryProcessingState state;
        processQuery(*query_node, state);

        for (const auto & subquery_to_visit : state.subqueries_to_visit)
            nodes_to_visit.push_back(subquery_to_visit);
    }
}

}
