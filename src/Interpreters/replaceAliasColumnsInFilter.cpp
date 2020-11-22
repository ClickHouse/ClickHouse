#include <Interpreters/replaceAliasColumnsInFilter.h>
#include <Interpreters/ColumnAliasesVisitor.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

ASTPtr replaceAliasColumnsInFilter(ASTPtr && ast, const ColumnsDescription & columns)
{
    auto & temp_select = ast->as<ASTSelectQuery &>();
    ColumnAliasesVisitor::Data aliase_column_data(columns);
    ColumnAliasesVisitor aliase_column_visitor(aliase_column_data);
    if (temp_select.where())
        aliase_column_visitor.visit(temp_select.refWhere());
    if (temp_select.prewhere())
        aliase_column_visitor.visit(temp_select.refPrewhere());

    return std::move(ast);
}

}
