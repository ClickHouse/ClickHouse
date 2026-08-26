#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexBasic.h>

#include <Core/Block.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

/// KeyDescription expects either a single expression or a tuple(...) function.
static ASTPtr wrapInTupleIfNeeded(ASTPtr expr_list)
{
    const auto * list = expr_list->as<ASTExpressionList>();
    if (!list || list->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Projection index expression list must not be empty");

    /// Single element or not a list — use the first child directly
    if (list->children.size() == 1)
        return list->children[0]->clone();

    /// Otherwise wrap everything into tuple function
    auto tuple_func = make_intrusive<ASTFunction>();
    tuple_func->name = "tuple";
    tuple_func->arguments = expr_list->clone();
    tuple_func->children.push_back(tuple_func->arguments);
    return tuple_func;
}

void ProjectionIndexBasic::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * index_expr,
    const ColumnsDescription & columns,
    const KeyDescription * partition_key,
    const ContextPtr & query_context) const
{
    auto select_query = make_intrusive<ASTProjectionSelectQuery>();
    auto select_expr_list = make_intrusive<ASTExpressionList>();
    select_expr_list->children.push_back(make_intrusive<ASTIdentifier>("_part_offset"));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expr_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, wrapInTupleIfNeeded(index_expr->clone()));

    ProjectionDescription::fillProjectionDescriptionByQuery(result, *select_query, columns, partition_key, query_context);
}

Block ProjectionIndexBasic::calculate(
    const ProjectionDescription & projection_desc,
    const Block & block,
    UInt64 starting_offset,
    ContextPtr context,
    const IColumnPermutation * perm_ptr) const
{
    return projection_desc.calculateByQuery(block, starting_offset, context, perm_ptr);
}

}
