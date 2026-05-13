#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/RewriteCountDistinctVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

void RewriteCountDistinctFunctionMatcher::visit(ASTPtr & ast, Data & data)
{
    auto * selectq = ast->as<ASTSelectQuery>();
    if (!selectq || !selectq->tables() || selectq->tables()->children.size() != 1)
        return;
    auto expr_list = selectq->select();
    if (!expr_list || expr_list->children.size() != 1)
        return;
    auto * func = expr_list->children[0]->as<ASTFunction>();
    if (!func || (Poco::toLower(func->name) != "countdistinct" && Poco::toLower(func->name) != "uniqexact"))
        return;
    auto arg = func->arguments->children;
    if (arg.size() != 1)
        return;
    if (!arg[0]->as<ASTIdentifier>())
        return;
    if (selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children.size() != 1)
        return;
    auto * table_expr = selectq->tables()->as<ASTTablesInSelectQuery>()->children[0]->as<ASTTablesInSelectQueryElement>()->children[0]->as<ASTTableExpression>();
    if (!table_expr || table_expr->size() != 1 || !table_expr->database_and_table_name)
        return;

    /// Apply the same gates as the new-analyzer `CountDistinctPass`:
    /// the rewrite must skip remote tables (`Distributed`, `remote(...)`) and
    /// fixed-size numeric columns, where `uniqExact` already beats the GROUP BY
    /// rewrite. If the storage cannot be resolved (e.g. the table is being
    /// created within the same query), skip the rewrite to fail closed.
    if (!data.context)
        return;
    auto table_id = data.context->tryResolveStorageID(table_expr->database_and_table_name);
    if (!table_id)
        return;
    auto storage = DatabaseCatalog::instance().tryGetTable(table_id, data.context);
    if (!storage || storage->isRemote())
        return;

    const auto column_name = arg[0]->as<ASTIdentifier>()->name();
    auto metadata_snapshot = storage->getInMemoryMetadataPtr(data.context, false);
    if (!metadata_snapshot)
        return;
    auto physical_column = metadata_snapshot->getColumns().tryGetPhysical(column_name);
    if (!physical_column)
        return;
    if (removeNullableOrLowCardinalityNullable(physical_column->type)->isValueRepresentedByNumber())
        return;

    // Check done, we now rewrite the AST
    auto cloned_select_query = selectq->clone();
    expr_list->children[0] = makeASTFunction("count");

    table_expr->children.clear();
    table_expr->children.emplace_back(make_intrusive<ASTSubquery>());
    table_expr->database_and_table_name = nullptr;
    table_expr->table_function = nullptr;
    table_expr->subquery = table_expr->children[0];

    // Form AST for subquery
    {
        auto * select_ptr = cloned_select_query->as<ASTSelectQuery>();
        select_ptr->refSelect()->children.clear();
        select_ptr->refSelect()->children.emplace_back(make_intrusive<ASTIdentifier>(column_name));
        auto exprlist = make_intrusive<ASTExpressionList>();
        exprlist->children.emplace_back(make_intrusive<ASTIdentifier>(column_name));
        cloned_select_query->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, exprlist);

        auto expr = make_intrusive<ASTExpressionList>();
        expr->children.emplace_back(cloned_select_query);
        auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union->union_mode = SelectUnionMode::UNION_DEFAULT;
        select_with_union->is_normalized = false;
        select_with_union->list_of_modes.clear();
        select_with_union->set_of_modes.clear();
        select_with_union->children.emplace_back(expr);
        select_with_union->list_of_selects = expr;
        table_expr->children[0]->as<ASTSubquery>()->children.emplace_back(select_with_union);
    }
}

}
