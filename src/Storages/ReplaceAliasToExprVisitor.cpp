#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ReplaceAliasToExprVisitor.h>
#include <Common/typeid_cast.h>

namespace DB
{

void ReplaceAliasToExprMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        visit(*identifier, ast, data);
    }
}

void ReplaceAliasToExprMatcher::visit(const ASTIdentifier & column, ASTPtr & ast, Data & data)
{
    const auto & column_name = column.name();
    if (data.columns.hasAlias(column_name))
    {
        /// Alias expr is saved in default expr.
        if (auto col_default = data.columns.getDefault(column_name))
        {
            ast = col_default->expression->clone();
        }
    }
}

}
