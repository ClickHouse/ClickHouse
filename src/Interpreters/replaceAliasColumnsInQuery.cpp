#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/ColumnAliasesVisitor.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

bool replaceAliasColumnsInQuery(
        ASTPtr & ast,
        const ColumnsDescription & columns,
        const NameToNameMap & array_join_result_to_source,
        ContextPtr context,
        const std::unordered_set<IAST *> & excluded_nodes)
{
    ColumnAliasesVisitor::Data aliases_column_data(columns, array_join_result_to_source, context, excluded_nodes);
    ColumnAliasesVisitor aliases_column_visitor(aliases_column_data);
    aliases_column_visitor.visit(ast);
    return aliases_column_data.changed;
}

}
