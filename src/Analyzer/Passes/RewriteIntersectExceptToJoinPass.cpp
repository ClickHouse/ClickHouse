#include <Analyzer/Passes/RewriteIntersectExceptToJoinPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/traverseQueryTree.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableJoin.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool optimize_rewrite_intersect_except_to_join;
}

namespace
{

NamesAndTypes getProjectionColumns(const QueryTreeNodePtr & node)
{
    if (const auto * query_node = node->as<QueryNode>())
        return query_node->getProjectionColumns();
    return node->as<UnionNode &>().computeProjectionColumns();
}

bool columnNamesUnique(const NamesAndTypes & columns)
{
    std::unordered_set<std::string_view> names;
    for (const auto & column : columns)
        if (!names.insert(column.name).second)
            return false;
    return true;
}

QueryTreeNodePtr makeColumn(const NameAndTypePair & column, const QueryTreeNodePtr & source)
{
    return std::make_shared<ColumnNode>(column, std::static_pointer_cast<ITableExpressionNode>(source));
}

QueryTreeNodePtr makeFunction(const String & name, QueryTreeNodes arguments, const ContextPtr & context)
{
    auto function_node = std::make_shared<FunctionNode>(name);
    function_node->getArguments().getNodes() = std::move(arguments);
    resolveOrdinaryFunctionNodeByName(*function_node, name, context);
    return function_node;
}

/// The analyzer assigned its unique `__tableN` aliases before this pass, and the planner identifies columns by
/// the alias of their table expression, so every subquery created here needs an alias of its own.
struct SubqueryAliases
{
    size_t counter = 0;
    String next() { return "__intersect_except_" + std::to_string(++counter); }
};

QueryTreeNodePtr makeSubquery(
    QueryTreeNodePtr join_tree, QueryTreeNodes projection, const NamesAndTypes & projection_columns, SubqueryAliases & aliases, const ContextPtr & context)
{
    auto query_node = std::make_shared<QueryNode>(Context::createCopy(context));
    query_node->setIsSubquery(true);
    query_node->setAlias(aliases.next());
    query_node->getJoinTreeNode() = std::move(join_tree);
    query_node->getProjection().getNodes() = std::move(projection);
    query_node->resolveProjectionColumns(projection_columns);
    return query_node;
}

/// A join side: the table expression and the columns to reference in it.
struct JoinSide
{
    QueryTreeNodePtr node;
    NamesAndTypes columns;
};

/// The arm as a join side with the union's result types, converted once in a wrapping subquery when they differ.
JoinSide makeJoinSide(
    const QueryTreeNodePtr & arm, NamesAndTypes arm_columns, const NamesAndTypes & result_columns, SubqueryAliases & aliases, const ContextPtr & context)
{
    if (auto * query_node = arm->as<QueryNode>())
        query_node->setIsSubquery(true);
    else
        arm->as<UnionNode &>().setIsSubquery(true);
    if (!arm->hasAlias())
        arm->setAlias(aliases.next());

    const bool same_types = std::ranges::equal(
        arm_columns, result_columns, [](const auto & lhs, const auto & rhs) { return lhs.type->equals(*rhs.type); });
    if (same_types)
        return {arm, std::move(arm_columns)};

    QueryTreeNodes projection;
    projection.reserve(arm_columns.size());
    for (size_t i = 0; i < arm_columns.size(); ++i)
        projection.push_back(createCastFunction(makeColumn(arm_columns[i], arm), result_columns[i].type, context));

    return {makeSubquery(arm, std::move(projection), result_columns, aliases, context), result_columns};
}

QueryTreeNodePtr buildJoinQuery(const UnionNode & union_node, JoinStrictness strictness, SubqueryAliases & aliases, const ContextPtr & context)
{
    const auto & arms = union_node.getQueries().getNodes();
    const auto result_columns = union_node.computeProjectionColumns();

    for (const auto & column : result_columns)
        if (hasDynamicType(column.type))
            return nullptr;

    std::vector<NamesAndTypes> arm_columns;
    arm_columns.reserve(arms.size());
    for (const auto & arm : arms)
    {
        arm_columns.push_back(getProjectionColumns(arm));
        if (arm_columns.back().size() != result_columns.size() || !columnNamesUnique(arm_columns.back()))
            return nullptr;
    }

    /// Fold from the left: the result of the previous join is the left side of the next one.
    auto left = makeJoinSide(arms.front(), std::move(arm_columns.front()), result_columns, aliases, context);
    for (size_t arm_index = 1; arm_index < arms.size(); ++arm_index)
    {
        auto right = makeJoinSide(arms[arm_index], std::move(arm_columns[arm_index]), result_columns, aliases, context);

        QueryTreeNodes key_conditions;
        key_conditions.reserve(result_columns.size());
        for (size_t i = 0; i < result_columns.size(); ++i)
            key_conditions.push_back(makeFunction(
                "isNotDistinctFrom", {makeColumn(left.columns[i], left.node), makeColumn(right.columns[i], right.node)}, context));
        auto join_expression = key_conditions.size() == 1 ? key_conditions.front() : makeFunction("and", std::move(key_conditions), context);

        auto join_node = std::make_shared<JoinNode>(
            left.node, right.node, std::move(join_expression), JoinLocality::Unspecified, strictness, JoinKind::Left, /*is_using_join_expression_=*/ false);

        QueryTreeNodes projection;
        projection.reserve(result_columns.size());
        for (size_t i = 0; i < result_columns.size(); ++i)
            projection.push_back(makeColumn(left.columns[i], left.node));

        left = {makeSubquery(std::move(join_node), std::move(projection), result_columns, aliases, context), result_columns};
    }

    /// A semi or anti join never multiplies the left rows, so one DISTINCT over the last join deduplicates everything.
    auto & result_query = left.node->as<QueryNode &>();
    result_query.setIsDistinct(true);
    result_query.setIsSubquery(union_node.isSubquery());
    result_query.setIsCTE(union_node.isCTE());
    result_query.setCTEName(union_node.getCTEName());
    result_query.setIsMaterialized(union_node.isMaterialized());
    if (union_node.hasAlias())
        result_query.setAlias(union_node.getAlias());
    result_query.setOriginalAST(union_node.getOriginalAST());
    return left.node;
}

bool hasFloatType(const DataTypePtr & type)
{
    bool result = false;
    auto check = [&](const IDataType & nested) { result |= isFloat(nested); };
    check(*type);
    type->forEachChild(check);
    return result;
}

/// Whether one of the enabled join algorithms can execute a left join of two subqueries with this strictness.
///
/// A merge join compares its keys with `compareAt`, which equates `-0.0` with `0.0` and every `NaN` with every
/// other, while both the set operation and the hash join compare them bitwise. So the rewrite is only equivalent
/// for a float key when no algorithm that a merge join can be reached through is enabled: `PARTIAL_MERGE` and
/// `PREFER_PARTIAL_MERGE` run one directly, and `AUTO` switches to one once the right side outgrows the limits.
bool joinAlgorithmSupports(const Settings & settings, JoinStrictness strictness, bool has_float_key)
{
    const auto & algorithms = settings[Setting::join_algorithm].value;
    for (const auto algorithm : {JoinAlgorithm::HASH, JoinAlgorithm::PARALLEL_HASH, JoinAlgorithm::GRACE_HASH})
        if (TableJoin::isEnabledAlgorithm(algorithms, algorithm))
            return true;

    if (has_float_key)
        return false;

    if (TableJoin::isEnabledAlgorithm(algorithms, JoinAlgorithm::AUTO)
        || TableJoin::isEnabledAlgorithm(algorithms, JoinAlgorithm::PREFER_PARTIAL_MERGE))
        return true;

    /// The partial merge join executes semi joins but not anti joins.
    return strictness == JoinStrictness::Semi && TableJoin::isEnabledAlgorithm(algorithms, JoinAlgorithm::PARTIAL_MERGE);
}

/// Keyed by the replaced node, which the key itself keeps alive so that the columns of the outer queries
/// still sourced by it can be re-pointed.
using Replacements = std::unordered_map<QueryTreeNodePtr, QueryTreeNodePtr>;

class RewriteIntersectExceptToJoinVisitor : public InDepthQueryTreeVisitorWithContext<RewriteIntersectExceptToJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteIntersectExceptToJoinVisitor>;
    using Base::Base;

    Replacements replacements;

    /// Bottom-up, so that the arms of a set operation are already rewritten when it is.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        const auto * union_node = node->as<UnionNode>();
        if (!union_node || union_node->hasRecursiveCTETable() || union_node->isCorrelated())
            return;

        const auto union_mode = union_node->getUnionMode();
        if (union_mode != SelectUnionMode::INTERSECT_DISTINCT && union_mode != SelectUnionMode::EXCEPT_DISTINCT)
            return;

        const auto strictness = union_mode == SelectUnionMode::INTERSECT_DISTINCT ? JoinStrictness::Semi : JoinStrictness::Anti;
        if (!getSettings()[Setting::optimize_rewrite_intersect_except_to_join])
            return;

        const auto result_columns = union_node->computeProjectionColumns();
        const bool has_float_key = std::ranges::any_of(result_columns, [](const auto & column) { return hasFloatType(column.type); });
        if (!joinAlgorithmSupports(getSettings(), strictness, has_float_key))
            return;

        auto join_query = buildJoinQuery(*union_node, strictness, aliases, getContext());
        if (!join_query)
            return;

        /// A semi or anti join needs neither side deduplicated, so a rewritten arm can drop its own DISTINCT.
        for (const auto & arm : union_node->getQueries().getNodes())
            if (rewritten.contains(arm.get()))
                arm->as<QueryNode &>().setIsDistinct(false);

        rewritten.insert(join_query.get());
        replacements.emplace(node, join_query);
        node = std::move(join_query);
    }

private:
    SubqueryAliases aliases;
    std::unordered_set<const IQueryTreeNode *> rewritten;
};

}

void RewriteIntersectExceptToJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteIntersectExceptToJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);

    /// The in-place counterpart of what `IQueryTreeNode::cloneAndReplace` does for the column sources of a
    /// replaced node: the outer queries still point their columns at the union node this pass replaced.
    const auto & replacements = visitor.replacements;
    traverseQueryTree(query_tree_node, Everything{}, [&](const QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (auto it = replacements.find(column_node->getColumnSourceOrNull()); it != replacements.end())
            column_node->setColumnSource(std::static_pointer_cast<ITableExpressionNode>(it->second));
    });
}

}
