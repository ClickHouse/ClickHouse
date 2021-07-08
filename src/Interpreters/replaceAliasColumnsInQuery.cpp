#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/ColumnAliasesVisitor.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

void replaceAliasColumnsInQuery(ASTPtr & ast, const ColumnsDescription & columns, const NameSet & forbidden_columns, ContextPtr context)
{
    ColumnAliasesVisitor::Data aliase_column_data(columns, forbidden_columns, context);
    ColumnAliasesVisitor aliase_column_visitor(aliase_column_data);
    aliase_column_visitor.visit(ast);
}

}
