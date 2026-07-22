#include <Storages/ReplaceAliasByExpressionVisitor.h>

#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(function))
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
            /// Reject an ALIAS whose free identifiers would be captured by an enclosing lambda parameter.
            /// Parameters of lambdas inside the ALIAS body shadow their own scope, so drop them from `bound`.
            auto check_alias_not_captured_by_lambda = [&column_name](this auto && self, const ASTPtr & sub_ast, NameSet bound) -> void
            {
                if (const auto * func = sub_ast->as<ASTFunction>(); func && func->name == "lambda")
                {
                    for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*func))
                        bound.erase(name);
                }
                else if (const auto * identifier = sub_ast->as<ASTIdentifier>(); identifier && bound.contains(identifier->name()))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "ALIAS column '{}' cannot be expanded inside a lambda: its expression references '{}', "
                        "which is bound by a lambda parameter of the same name", column_name, identifier->name());

                for (const auto & child : sub_ast->children)
                    self(child, bound);
            };

            if (!data.private_aliases.empty())
                check_alias_not_captured_by_lambda(col_default->expression, data.private_aliases);

            ast = col_default->expression->clone();

            /// Revisit the result to expand chained ALIASes (a -> b -> c).
            Visitor(data).visit(ast);
        }
    }
}

}
