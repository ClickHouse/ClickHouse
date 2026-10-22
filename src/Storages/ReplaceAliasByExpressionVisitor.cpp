#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Common/typeid_cast.h>

namespace DB
{

bool ReplaceAliasByExpressionMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    /// A lambda visits its own body (see the ASTFunction overload) to mask its parameters.
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

    /// Mask lambda parameters so a name shadowing an ALIAS column is not expanded.
    Names local_aliases;
    for (const auto & name : getASTLambdaArgumentNames(function))
        if (data.private_aliases.insert(name).second)
            local_aliases.push_back(name);

    Visitor(data).visit(function.arguments->children[1]);

    for (const auto & name : local_aliases)
        data.private_aliases.erase(name);
}

void ReplaceAliasByExpressionMatcher::visit(const ASTIdentifier & column, ASTPtr & ast, Data & data)
{
    const String & column_name = column.name();

    if (data.private_aliases.contains(column_name))
        return;

    if (data.columns.hasAlias(column_name))
    {
        /// Alias expr is saved in default expr.
        if (auto col_default = data.columns.getDefault(column_name))
        {
            /// Expand the ALIAS chain (a -> b -> c) before deciding on capture. The inserted expression was
            /// written at table scope, so its identifiers refer to table columns even when they match a
            /// lambda parameter name, and only the fully expanded result can be captured.
            /// The ALIAS body may contain column matchers (e.g. `COLUMNS('^msg$')`), which downstream
            /// consumers such as text-index transforms pass to TreeRewriter as-is, so expand them here.
            ASTPtr expanded = cloneAndExpandColumnDefaultExpression(*col_default, data.columns);
            Data table_scope{data.columns, {}, data.reject_lambda_capture};
            Visitor(table_scope).visit(expanded);

            /// Reject an ALIAS whose free identifiers would be captured by an enclosing lambda parameter.
            if (data.reject_lambda_capture && !data.private_aliases.empty())
                validateAliasExpansionNotCapturedByLambda(column_name, expanded, data.private_aliases);

            ast = std::move(expanded);
        }
    }
}

}
