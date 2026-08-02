#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexCommitOrder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/ProjectionsDescription.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTProjectionSelectQuery.h>

#include <Core/Block.h>

namespace DB
{

void ProjectionIndexCommitOrder::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * index_expr,
    const ColumnsDescription & columns,
    const KeyDescription * partition_key,
    const ContextPtr & query_context) const
{
    auto select_query = make_intrusive<ASTProjectionSelectQuery>();

    /// Projection list = columns the user wants to store + _block_number/_block_offset.
    auto select_expr_list = index_expr->clone();
    select_expr_list->children.push_back(make_intrusive<ASTIdentifier>(BlockNumberColumn::name));
    select_expr_list->children.push_back(make_intrusive<ASTIdentifier>(BlockOffsetColumn::name));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expr_list));

    /// ORDER BY (_block_number, _block_offset)
    auto order_args = make_intrusive<ASTExpressionList>();
    order_args->children.push_back(make_intrusive<ASTIdentifier>(BlockNumberColumn::name));
    order_args->children.push_back(make_intrusive<ASTIdentifier>(BlockOffsetColumn::name));
    auto order_expr_list = make_intrusive<ASTFunction>();
    order_expr_list->name = "tuple";
    order_expr_list->arguments = std::move(order_args);
    order_expr_list->children.push_back(order_expr_list->arguments);
    select_query->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, std::move(order_expr_list));

    ProjectionDescription::fillProjectionDescriptionByQuery(result, *select_query, columns, partition_key, query_context);
}

Block ProjectionIndexCommitOrder::calculate(
    const ProjectionDescription & projection_desc,
    const Block & block,
    UInt64 starting_offset,
    ContextPtr context,
    const IColumnPermutation * perm_ptr) const
{
    return projection_desc.calculateByQuery(block, starting_offset, context, perm_ptr);
}

}
