#include <Analyzer/Passes/RewriteIntersectExceptToJoinPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Interpreters/Context.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace Setting
{
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

QueryTreeNodePtr makeSubquery(QueryTreeNodePtr join_tree, QueryTreeNodes projection, const NamesAndTypes & projection_columns, const ContextPtr & context)
{
    auto query_node = std::make_shared<QueryNode>(Context::createCopy(context));
    query_node->setIsSubquery(true);
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
JoinSide makeJoinSide(const QueryTreeNodePtr & arm, NamesAndTypes arm_columns, const NamesAndTypes & result_columns, const ContextPtr & context)
{
    if (auto * query_node = arm->as<QueryNode>())
        query_node->setIsSubquery(true);
    else
        arm->as<UnionNode &>().setIsSubquery(true);

    bool same_types = true;
    for (size_t i = 0; i < arm_columns.size(); ++i)
        same_types &= arm_columns[i].type->equals(*result_columns[i].type);
    if (same_types)
        return {arm, std::move(arm_columns)};

    QueryTreeNodes projection;
    projection.reserve(arm_columns.size());
    for (size_t i = 0; i < arm_columns.size(); ++i)
        projection.push_back(createCastFunction(makeColumn(arm_columns[i], arm), result_columns[i].type, context));

    return {makeSubquery(arm, std::move(projection), result_columns, context), result_columns};
}

QueryTreeNodePtr buildJoinQuery(const UnionNode & union_node, const ContextPtr & context)
{
    const auto strictness = union_node.getUnionMode() == SelectUnionMode::INTERSECT_DISTINCT ? JoinStrictness::Semi : JoinStrictness::Anti;
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
    auto left = makeJoinSide(arms.front(), std::move(arm_columns.front()), result_columns, context);
    QueryTreeNodePtr result;
    for (size_t arm_index = 1; arm_index < arms.size(); ++arm_index)
    {
        auto right = makeJoinSide(arms[arm_index], std::move(arm_columns[arm_index]), result_columns, context);

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

        result = makeSubquery(std::move(join_node), std::move(projection), result_columns, context);
        left = {result, result_columns};
    }

    /// A semi or anti join never multiplies the left rows, so one DISTINCT over the last join deduplicates everything.
    auto & result_query = result->as<QueryNode &>();
    result_query.setIsDistinct(true);
    result_query.setIsSubquery(union_node.isSubquery());
    result_query.setAlias(union_node.getAlias());
    result_query.setOriginalAST(union_node.getOriginalAST());
    return result;
}

/// The replaced union nodes, kept alive so that the columns of the outer queries still sourced by them can be re-pointed.
using Replacements = std::unordered_map<const IQueryTreeNode *, std::pair<QueryTreeNodePtr, QueryTreeNodePtr>>;

class RewriteIntersectExceptToJoinVisitor : public InDepthQueryTreeVisitorWithContext<RewriteIntersectExceptToJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteIntersectExceptToJoinVisitor>;
    using Base::Base;

    Replacements replacements;

    /// Bottom-up, so that the arms of a set operation are already rewritten when it is.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_intersect_except_to_join])
            return;

        const auto * union_node = node->as<UnionNode>();
        if (!union_node || union_node->hasRecursiveCTETable())
            return;

        const auto union_mode = union_node->getUnionMode();
        if (union_mode != SelectUnionMode::INTERSECT_DISTINCT && union_mode != SelectUnionMode::EXCEPT_DISTINCT)
            return;

        auto join_query = buildJoinQuery(*union_node, getContext());
        if (!join_query)
            return;

        /// A semi or anti join needs neither side deduplicated, so a rewritten arm can drop its own DISTINCT.
        for (const auto & arm : union_node->getQueries().getNodes())
            if (rewritten.contains(arm.get()))
                arm->as<QueryNode &>().setIsDistinct(false);

        rewritten.insert(join_query.get());
        replacements.emplace(node.get(), std::pair{node, join_query});
        node = std::move(join_query);
    }

private:
    std::unordered_set<const IQueryTreeNode *> rewritten;
};

class ReplaceColumnSourcesVisitor : public InDepthQueryTreeVisitor<ReplaceColumnSourcesVisitor>
{
public:
    explicit ReplaceColumnSourcesVisitor(const Replacements & replacements_) : replacements(replacements_) {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return;

        auto it = replacements.find(source.get());
        if (it != replacements.end())
            column_node->setColumnSource(std::static_pointer_cast<ITableExpressionNode>(it->second.second));
    }

private:
    const Replacements & replacements;
};

}

void RewriteIntersectExceptToJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteIntersectExceptToJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);

    if (visitor.replacements.empty())
        return;

    ReplaceColumnSourcesVisitor replace_sources_visitor(visitor.replacements);
    replace_sources_visitor.visit(query_tree_node);
}

}
