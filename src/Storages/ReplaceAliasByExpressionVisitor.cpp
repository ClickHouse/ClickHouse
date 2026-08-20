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
                else if (const auto * identifier = sub_ast->as<ASTIdentifier>())
                {
                    /// A compound identifier like `t.v` is captured when its root `t` is a lambda parameter.
                    const String & root = identifier->name_parts.empty() ? identifier->name() : identifier->name_parts.front();
                    if (bound.contains(root))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "ALIAS column '{}' cannot be expanded inside a lambda: its expression references '{}', "
                            "which is bound by the lambda parameter '{}'", column_name, identifier->name(), root);
                }

                for (const auto & child : sub_ast->children)
                    self(child, bound);
            };

            /// Expand the ALIAS chain (a -> b -> c) before deciding on capture. The inserted expression was
            /// written at table scope, so its identifiers refer to table columns even when they match a
            /// lambda parameter name, and only the fully expanded result can be captured.
            ASTPtr expanded = col_default->expression->clone();
            Data table_scope{data.columns, {}, data.reject_lambda_capture};
            Visitor(table_scope).visit(expanded);

            if (data.reject_lambda_capture && !data.private_aliases.empty())
                check_alias_not_captured_by_lambda(expanded, data.private_aliases);

            ast = std::move(expanded);
        }
    }
}

}
