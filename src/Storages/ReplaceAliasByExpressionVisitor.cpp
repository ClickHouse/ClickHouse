#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Common/typeid_cast.h>

namespace DB
{

bool ReplaceAliasByExpressionMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    /// A lambda visits its own body itself (see the ASTFunction overload).
    if (const auto * function = node->as<ASTFunction>())
        return function->name != "lambda";
    return true;
}

void ReplaceAliasByExpressionMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, ast, data);
    else if (const auto * identifier = ast->as<ASTIdentifier>())
        visit(*identifier, ast, data);
}

void ReplaceAliasByExpressionMatcher::visit(const ASTFunction & function, ASTPtr &, Data & data)
{
    if (function.name != "lambda")
        return;

    /// Mask the lambda parameters so a lambda-local name that shadows an ALIAS column is not expanded.
    Names local_aliases;
    for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(function))
        if (data.private_aliases.insert(name).second)
            local_aliases.push_back(name);

    Visitor(data).visit(function.arguments->children[1]);

    for (const auto & name : local_aliases)
        data.private_aliases.erase(name);
}

void ReplaceAliasByExpressionMatcher::visit(const ASTIdentifier & column, ASTPtr & ast, Data & data)
{
    const auto & column_name = column.name();

    if (data.private_aliases.contains(column_name))
        return;

    if (data.columns.hasAlias(column_name))
    {
        /// Alias expr is saved in default expr.
        if (auto col_default = data.columns.getDefault(column_name))
        {
            ast = col_default->expression->clone();
        }
    }
}

void replaceAliasColumnsWithExpressions(ASTPtr & ast, const ColumnsDescription & columns)
{
    ReplaceAliasByExpressionMatcher::Data data{columns, {}};
    ReplaceAliasByExpressionMatcher::Visitor(data).visit(ast);
}

}
