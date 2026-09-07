#include <Planner/rewriteIntersectOrExceptToJoin.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Interpreters/Context.h>

#include <unordered_set>

namespace DB
{

namespace
{

NamesAndTypes getArmProjectionColumns(const QueryTreeNodePtr & arm)
{
    if (const auto * query_node = arm->as<QueryNode>())
        return query_node->getProjectionColumns();
    return arm->as<UnionNode &>().computeProjectionColumns();
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

/// The arm's column converted to the union's result type, as the set-operation step converts its inputs.
QueryTreeNodePtr makeResultColumn(const NameAndTypePair & column, const QueryTreeNodePtr & source, const DataTypePtr & result_type, const ContextPtr & context)
{
    auto column_node = makeColumn(column, source);
    if (column.type->equals(*result_type))
        return column_node;
    return makeFunction("_CAST", {std::move(column_node), std::make_shared<ConstantNode>(result_type->getName())}, context);
}

}

QueryTreeNodePtr rewriteIntersectOrExceptToJoin(const QueryTreeNodePtr & union_node, const ContextPtr & context)
{
    const auto & union_node_typed = union_node->as<UnionNode &>();
    const auto union_mode = union_node_typed.getUnionMode();
    chassert(union_mode == SelectUnionMode::INTERSECT_DISTINCT || union_mode == SelectUnionMode::EXCEPT_DISTINCT);
    const auto strictness = union_mode == SelectUnionMode::INTERSECT_DISTINCT ? JoinStrictness::Semi : JoinStrictness::Anti;

    const auto & arms = union_node_typed.getQueries().getNodes();
    const auto result_columns = union_node_typed.computeProjectionColumns();

    /// A join cannot use `Dynamic` keys.
    for (const auto & column : result_columns)
        if (hasDynamicType(column.type))
            return nullptr;

    std::vector<NamesAndTypes> arm_columns;
    arm_columns.reserve(arms.size());
    for (const auto & arm : arms)
    {
        arm_columns.push_back(getArmProjectionColumns(arm));
        if (arm_columns.back().size() != result_columns.size() || !columnNamesUnique(arm_columns.back()))
            return nullptr;
    }

    /// Fold from the left: the result of the previous join becomes the left arm of the next one.
    QueryTreeNodePtr left = arms.front();
    NamesAndTypes left_columns = arm_columns.front();
    for (size_t arm_index = 1; arm_index < arms.size(); ++arm_index)
    {
        const auto & right = arms[arm_index];
        const auto & right_columns = arm_columns[arm_index];

        QueryTreeNodes key_conditions;
        key_conditions.reserve(left_columns.size());
        for (size_t i = 0; i < left_columns.size(); ++i)
            key_conditions.push_back(makeFunction(
                "isNotDistinctFrom",
                {makeResultColumn(left_columns[i], left, result_columns[i].type, context),
                 makeResultColumn(right_columns[i], right, result_columns[i].type, context)},
                context));

        auto join_expression = key_conditions.size() == 1 ? key_conditions.front() : makeFunction("and", std::move(key_conditions), context);
        auto join_node = std::make_shared<JoinNode>(
            left, right, std::move(join_expression), JoinLocality::Unspecified, strictness, JoinKind::Left, /*is_using_join_expression_=*/ false);

        auto query_node = std::make_shared<QueryNode>(Context::createCopy(context));
        query_node->setIsDistinct(true);
        query_node->setIsSubquery(true);
        query_node->getJoinTreeNode() = std::move(join_node);
        for (size_t i = 0; i < left_columns.size(); ++i)
            query_node->getProjection().getNodes().push_back(makeResultColumn(left_columns[i], left, result_columns[i].type, context));
        query_node->resolveProjectionColumns(result_columns);

        left = std::move(query_node);
        left_columns = result_columns;
    }

    auto & result = left->as<QueryNode &>();
    result.setIsSubquery(union_node_typed.isSubquery());
    result.setAlias(union_node_typed.getAlias());
    return left;
}

}
