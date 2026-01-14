#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexBasic.h>

#include <Core/Block.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{

void ProjectionIndexBasic::fillProjectionDescription(
    ProjectionDescription & result, const IAST * index_expr, const ColumnsDescription & columns, ContextPtr query_context) const
{
    auto select_query = std::make_shared<ASTProjectionSelectQuery>();
    auto select_expr_list = std::make_shared<ASTExpressionList>();
    select_expr_list->children.push_back(std::make_shared<ASTIdentifier>("_part_offset"));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expr_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, index_expr->clone());

    ProjectionDescription::fillProjectionDescriptionByQuery(result, *select_query, columns, query_context);
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
