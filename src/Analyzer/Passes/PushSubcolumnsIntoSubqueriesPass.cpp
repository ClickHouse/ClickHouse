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
    /// Number of `getSubcolumn` occurrences matched into this group.
    size_t occurrences = 0;

    /// Set when the group is applied.
    String new_column_name;
    bool applied = false;
};

struct QueryProcessingState
{
    /// Table expressions of the JOIN TREE that are query nodes and are eligible
    /// as pushdown targets (in particular, not on a side of a JOIN that can be
    /// filled with default values for non-matched rows).
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> eligible_targets;

    /// Query nodes found on a side of a JOIN that can be filled with default values. An ordinary
    /// CTE referenced several times is a single shared query node, so the same node can also
    /// appear in an otherwise eligible position; such a node must stay ineligible everywhere.
    std::unordered_set<const IQueryTreeNode *> defaultable_targets;

    std::vector<PushdownGroup> groups;

    /// Number of references to each column of every query or union source, including the column
    /// arguments of matched `getSubcolumn` occurrences (each occurrence contributes one
    /// reference, compensated by `PushdownGroup::occurrences` when deciding to apply) and
    /// correlated columns of nested subqueries. Counted for all sources, not only the eligible
    /// targets: the counts of every query referencing a shared subquery are combined to decide
    /// which of its exported columns stay alive.
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> column_references;

    bool collect_candidates = false;
    bool query_has_aggregation = false;
    QueryTreeNodes group_by_keys;

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

    /// Only query and union sources are rewritten. In particular, a materialized CTE that is
    /// referenced more than once stays a TableNode over its temporary table (single-use ones
    /// are inlined and covered by the query branch). The temporary table serves all references
    /// of the CTE, so pruning the parent column there would require proving that no reference
    /// needs the whole column, and adding the subcolumn without removing the parent column
    /// would only make the materialized table bigger. Such references are deliberately left as is.
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

    if (join_tree_node->getNodeType() == QueryTreeNodeType::QUERY)
    {
        /// The same query node can occur several times in the JOIN TREE when it is a shared
        /// ordinary CTE. Eligibility is tracked per node, not per occurrence, so one occurrence
        /// in a defaultable position makes the node ineligible even for its other occurrences:
        /// the projection column added for an eligible occurrence would also be exported by the
        /// defaultable occurrence, where the JOIN fills it with default values of the subcolumn
        /// type for non-matched rows instead of `getSubcolumn` of the filled parent column.
        if (can_be_filled_with_defaults)
        {
            state.defaultable_targets.insert(join_tree_node.get());
            state.eligible_targets.erase(join_tree_node.get());
        }
        else if (!state.defaultable_targets.contains(join_tree_node.get()))
            state.eligible_targets.emplace(join_tree_node.get(), join_tree_node);
    }
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
        /// Nested subqueries are processed separately, but their correlated columns are uses
        /// of the columns of the enclosing queries. RemoveUnusedProjectionColumnsPass treats
        /// correlated columns as live uses of the outer query columns, so they are counted
        /// here as whole-column references: pushing a subcolumn of a column that a correlated
        /// subquery still needs would only add a projection column next to the surviving one.
        const auto * nested_query_node = node->as<QueryNode>();
        const auto & correlated_columns
            = nested_query_node ? nested_query_node->getCorrelatedColumns() : node->as<UnionNode &>().getCorrelatedColumns();

        for (const auto & correlated_column : correlated_columns.getNodes())
        {
            const auto * column_node = correlated_column->as<ColumnNode>();
            if (!column_node)
                continue;

            auto column_source = column_node->getColumnSourceOrNull();
            if (column_source && isQueryOrUnionNode(column_source))
                ++state.column_references[column_source.get()][column_node->getColumnName()];
        }

        return;
    }

    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto column_source = column_node->getColumnSourceOrNull();
        if (column_source && isQueryOrUnionNode(column_source))
            ++state.column_references[column_source.get()][column_node->getColumnName()];
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
                            .occurrences = 0,
                            .new_column_name = {},
                            .applied = false});
                        group = &state.groups.back();
                    }

                    /// The column argument of the matched occurrence is visited below as a child
                    /// and counted in column_references; the occurrence counter compensates it.
                    ++group->occurrences;

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
    auto * inner_column = inner_node->as<ColumnNode>();
    if (!inner_column)
        return nullptr;

    /// An exported ALIAS column whose body is just another column of the same table (possibly
    /// chained) is semantically the underlying storage column, so the subcolumn can be read
    /// directly from it. Non-trivial expressions (function calls, casts, ARRAY JOIN and
    /// JOIN USING columns) are rejected by resolveTrivialAliasChain.
    if (inner_column->hasExpression())
    {
        inner_column = resolveTrivialAliasChain(inner_column);
        if (!inner_column)
            return nullptr;
    }

    auto inner_source = inner_column->getColumnSourceOrNull();
    if (!inner_source)
        return nullptr;

    auto * table_node = inner_source->as<TableNode>();
    auto * table_function_node = inner_source->as<TableFunctionNode>();

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        /// Some storages expose subcolumns syntactically but opt out of rewriting reads of a column
        /// into direct reads of its subcolumns (e.g. StorageFile, StorageURL, StorageDistributed).
        if (!storage_snapshot->storage.supportsOptimizationToSubcolumns())
            return nullptr;

        if (storage_snapshot->metadata->isVirtualColumn(inner_column->getColumnName()))
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

/// Check that an existing projection node is the same expression as a built subcolumn
/// projection node. Column equality compares only names and types, so the sources of the
/// columns (the same in both nodes by construction of buildSubcolumnProjectionNode) are
/// compared explicitly: a column of an unrelated table could have the same name and type.
bool isSameSubcolumnProjection(const QueryTreeNodePtr & existing_node, const QueryTreeNodePtr & built_node)
{
    if (!existing_node->isEqual(*built_node, {.compare_aliases = false}))
        return false;

    const auto * existing_column = existing_node->as<ColumnNode>();
    const auto * built_column = built_node->as<ColumnNode>();

    if (const auto * built_function = built_node->as<FunctionNode>())
    {
        /// isEqual above guarantees the same structure: getSubcolumn with a column argument.
        existing_column = existing_node->as<FunctionNode>()->getArguments().getNodes()[0]->as<ColumnNode>();
        built_column = built_function->getArguments().getNodes()[0]->as<ColumnNode>();
    }

    if (!existing_column || !built_column)
        return false;

    return existing_column->getColumnSourceOrNull() == built_column->getColumnSourceOrNull();
}

/// Add the subcolumn to the subquery projection. Returns false if the pushdown is not possible.
bool applyGroup(PushdownGroup & group)
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

    const auto & projection_nodes = subquery.getProjection().getNodes();

    const auto & inner_node = projection_nodes[*projection_index];
    auto new_projection_node = buildSubcolumnProjectionNode(group, inner_node, subquery.getContext());
    if (!new_projection_node)
        return false;

    auto new_column_name = group.column_name + "." + group.subcolumn_path;
    for (size_t i = 0; i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name != new_column_name)
            continue;

        /// A projection column with the name of the subcolumn already exists. When it is the
        /// same subcolumn expression, e.g. it was pushed into this shared subquery (an ordinary
        /// CTE referenced several times) while processing another query referencing it, then it
        /// is reused. Otherwise the reference to the new column would be ambiguous.
        if (i < projection_nodes.size()
            && projection_columns[i].type->equals(*group.subcolumn_type)
            && isSameSubcolumnProjection(projection_nodes[i], new_projection_node))
        {
            group.new_column_name = std::move(new_column_name);
            group.applied = true;
            return true;
        }

        return false;
    }

    subquery.addProjectionColumn(std::move(new_projection_node), NameAndTypePair{new_column_name, group.subcolumn_type});

    group.new_column_name = std::move(new_column_name);
    group.applied = true;
    return true;
}

/// Identity of the underlying column of a trivial projection expression: the source
/// table expression and the column name in it.
using CanonicalColumn = std::pair<const IQueryTreeNode *, String>;

/// Map each exported column name of the subquery to the underlying column its projection
/// expression trivially resolves to. The same physical column can be exported under several
/// names: `SELECT tup AS x, tup FROM t`, or a trivial ALIAS storage column next to its base
/// column. Names whose projection expression is not a column (or is a non-trivial ALIAS)
/// are not mapped.
std::unordered_map<String, CanonicalColumn> collectCanonicalExports(const QueryNode & subquery)
{
    std::unordered_map<String, CanonicalColumn> result;

    const auto & projection_columns = subquery.getProjectionColumns();
    const auto & projection_nodes = subquery.getProjection().getNodes();

    for (size_t i = 0; i < projection_nodes.size() && i < projection_columns.size(); ++i)
    {
        auto * column = projection_nodes[i]->as<ColumnNode>();
        if (column && column->hasExpression())
            column = resolveTrivialAliasChain(column);
        if (!column)
            continue;

        auto column_source = column->getColumnSourceOrNull();
        if (column_source)
            result.emplace(projection_columns[i].name, CanonicalColumn{column_source.get(), column->getColumnName()});
    }

    return result;
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

void processQuery(
    QueryNode & query_node,
    QueryProcessingState & state,
    const std::unordered_set<String> * pruned_exports)
{
    const auto & context = query_node.getContext();
    state.collect_candidates = context->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries];

    if (state.collect_candidates)
    {
        collectEligibleTargets(query_node.getJoinTreeNode(), false /*can_be_filled_with_defaults*/, state);

        for (auto it = state.eligible_targets.begin(); it != state.eligible_targets.end();)
        {
            const auto & target_query = it->second->as<const QueryNode &>();

            /// The subquery can carry its own settings (SETTINGS clause, view definition),
            /// and disabling the setting there must protect that subquery from the rewrite.
            bool enabled_in_target = target_query.getContext()->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries];

            if (enabled_in_target && canAddProjectionColumns(target_query))
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

    /// Every clause of the query is traversed, even those that cannot reference columns of the
    /// JOIN TREE (e.g. LIMIT expressions must be constant): besides collecting candidates, the
    /// traversal counts column references, and clauses like LIMIT can contain scalar subqueries
    /// whose correlated columns are such references.
    /// Projection slots whose exported column became unused in the parent queries are skipped:
    /// they are removed by RemoveUnusedProjectionColumnsPass together with everything in them.
    auto for_each_clause = [&](auto && visit_clause)
    {
        visit_clause(query_node.getJoinTreeNode(), ClauseKind::PreAggregation);
        visit_clause(query_node.getPrewhere(), ClauseKind::PreAggregation);
        visit_clause(query_node.getWhere(), ClauseKind::PreAggregation);
        visit_clause(query_node.getWithNode(), ClauseKind::PostAggregation);

        const auto & projection_columns = query_node.getProjectionColumns();
        auto & projection_nodes = query_node.getProjection().getNodes();
        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            if (pruned_exports && i < projection_columns.size() && pruned_exports->contains(projection_columns[i].name))
                continue;
            visit_clause(projection_nodes[i], ClauseKind::PostAggregation);
        }

        visit_clause(query_node.getGroupByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getHaving(), ClauseKind::PostAggregation);
        visit_clause(query_node.getWindowNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getQualify(), ClauseKind::PostAggregation);
        visit_clause(query_node.getOrderByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getInterpolate(), ClauseKind::Interpolate);
        visit_clause(query_node.getLimitByNode(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimitByLimit(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimitByOffset(), ClauseKind::PostAggregation);
        visit_clause(query_node.getLimit(), ClauseKind::PostAggregation);
        visit_clause(query_node.getOffset(), ClauseKind::PostAggregation);
    };

    for_each_clause([&](QueryTreeNodePtr & clause_node, ClauseKind clause_kind)
    {
        collectCandidates(clause_node, clause_kind, false /*inside_aggregate_function*/, state);
    });

    bool any_group_applied = false;
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, CanonicalColumn>> canonical_exports_by_source;

    for (auto & group : state.groups)
    {
        if (!group.viable)
            continue;

        /// If the whole column is still referenced outside of the replaced occurrences (either
        /// directly or by an occurrence of a non-viable group), the parent projection column
        /// stays, and the subquery would read both the whole column and the subcolumn from the
        /// table. Extracting the subcolumn from the already read column is cheaper, so the
        /// group is not applied. The reference counts are keyed by exported names, but the same
        /// physical column can be exported under several names, so the counts of every name
        /// resolving to the same underlying column as the group's column are combined: while any
        /// of them stays alive, the whole column is read from the table anyway.
        auto [exports_it, exports_inserted] = canonical_exports_by_source.try_emplace(group.source.get());
        if (exports_inserted)
            exports_it->second = collectCanonicalExports(group.source->as<const QueryNode &>());
        const auto & canonical_exports = exports_it->second;

        auto group_canonical_it = canonical_exports.find(group.column_name);
        auto is_same_underlying_column = [&](const String & column_name)
        {
            if (column_name == group.column_name)
                return true;
            if (group_canonical_it == canonical_exports.end())
                return false;
            auto other_it = canonical_exports.find(column_name);
            return other_it != canonical_exports.end() && other_it->second == group_canonical_it->second;
        };

        size_t references = 0;
        for (const auto & [column_name, count] : state.column_references[group.source.get()])
        {
            if (is_same_underlying_column(column_name))
                references += count;
        }

        size_t replaced_references = 0;
        for (const auto & other_group : state.groups)
        {
            if (other_group.viable && other_group.source == group.source && is_same_underlying_column(other_group.column_name))
                replaced_references += other_group.occurrences;
        }

        if (references > replaced_references)
            continue;

        any_group_applied |= applyGroup(group);
    }

    if (any_group_applied)
    {
        for_each_clause([&](QueryTreeNodePtr & clause_node, ClauseKind)
        {
            replaceCandidates(clause_node, state);
        });
    }
}

}

void PushSubcolumnsIntoSubqueriesPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    /// An ordinary (non-materialized) CTE referenced several times is a single query node
    /// shared by all the referencing queries, so the query graph is a DAG rather than a tree.
    /// Queries are processed in topological order: a shared subquery is processed only after
    /// every query referencing it, so that it sees all the subcolumns pushed into it (and
    /// pushes them further down), and the liveness of its exported columns is fully known.

    /// Discovery: record the directly nested query and union nodes of every node.
    QueryTreeNodes discovery_order = {query_tree_node};
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodes> nested_subqueries;
    std::unordered_map<const IQueryTreeNode *, size_t> unprocessed_parents;

    {
        std::unordered_set<const IQueryTreeNode *> discovered = {query_tree_node.get()};

        for (size_t i = 0; i < discovery_order.size(); ++i)
        {
            auto current = discovery_order[i];
            auto & current_nested_subqueries = nested_subqueries[current.get()];

            std::unordered_set<const IQueryTreeNode *> unique_nested_subqueries;
            QueryTreeNodes stack = current->getChildren();

            while (!stack.empty())
            {
                auto node = std::move(stack.back());
                stack.pop_back();

                if (!node)
                    continue;

                if (isQueryOrUnionNode(node))
                {
                    if (unique_nested_subqueries.emplace(node.get()).second)
                    {
                        ++unprocessed_parents[node.get()];
                        current_nested_subqueries.push_back(node);

                        if (discovered.emplace(node.get()).second)
                            discovery_order.push_back(node);
                    }
                    continue;
                }

                for (const auto & child : node->getChildren())
                    stack.push_back(child);
            }
        }
    }

    /// Number of references to each exported column of a query or union node that stay in the
    /// referencing queries after the rewrite, and the number of replaced references, combined
    /// over all the processed queries. An exported column with some references replaced and no
    /// references remaining is removed by the subsequent RemoveUnusedProjectionColumnsPass, so
    /// when the subquery itself is processed, references inside such dead projection slots must
    /// not count as uses of the whole column (otherwise pushdown through several levels of
    /// subqueries would stop at the first level).
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> alive_references;
    std::unordered_map<const IQueryTreeNode *, std::unordered_map<String, size_t>> replaced_references;
    std::unordered_set<const IQueryTreeNode *> processed;

    auto process_node = [&](const QueryTreeNodePtr & node)
    {
        if (!processed.emplace(node.get()).second)
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        std::unordered_set<String> pruned_exports;
        if (auto replaced_it = replaced_references.find(node.get()); replaced_it != replaced_references.end())
        {
            const auto & alive_columns = alive_references[node.get()];
            for (const auto & [column_name, replaced] : replaced_it->second)
            {
                auto alive_it = alive_columns.find(column_name);
                if (replaced > 0 && (alive_it == alive_columns.end() || alive_it->second == 0))
                    pruned_exports.insert(column_name);
            }
        }

        QueryProcessingState state;
        processQuery(*query_node, state, pruned_exports.empty() ? nullptr : &pruned_exports);

        for (const auto & [source, columns] : state.column_references)
        {
            for (const auto & [column_name, references] : columns)
            {
                size_t replaced = 0;
                for (const auto & group : state.groups)
                {
                    if (group.applied && group.source.get() == source && group.column_name == column_name)
                        replaced += group.occurrences;
                }
                alive_references[source][column_name] += references - replaced;
                replaced_references[source][column_name] += replaced;
            }
        }
    };

    QueryTreeNodes ready = {query_tree_node};
    while (!ready.empty())
    {
        auto node = std::move(ready.back());
        ready.pop_back();

        process_node(node);

        for (const auto & subquery : nested_subqueries[node.get()])
        {
            if (--unprocessed_parents[subquery.get()] == 0)
                ready.push_back(subquery);
        }
    }

    /// Nodes whose number of unprocessed parents never reached zero are parts of reference
    /// cycles (e.g. recursive CTEs); they are processed in discovery order.
    for (const auto & node : discovery_order)
        process_node(node);
}

}
