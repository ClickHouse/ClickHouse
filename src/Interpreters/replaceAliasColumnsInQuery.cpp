#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/ColumnAliasesVisitor.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

void replaceAliasColumnsInQuery(ASTPtr & ast, const ColumnsDescription & columns, const NameSet & forbidden_columns, ContextConstPtr context)
{
    ColumnAliasesVisitor::Data aliases_column_data(columns, forbidden_columns, context);
    ColumnAliasesVisitor aliases_column_visitor(aliases_column_data);
    aliases_column_visitor.visit(ast);
}

}
