#include <Interpreters/RewriteArrayExistsFunctionVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
void RewriteArrayExistsFunctionMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
    {
        if (func->is_window_function)
            return;

        visit(*func, ast, data);
    }
}

void RewriteArrayExistsFunctionMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data &)
{
    if (func.name != "arrayExists" || !func.arguments)
        return;

    auto & array_exists_arguments = func.arguments->children;
    if (array_exists_arguments.size() != 2)
        return;

    /// lambda function must be like: x -> x = elem
    const auto * lambda_func = array_exists_arguments[0]->as<ASTFunction>();
    if (!lambda_func || !lambda_func->is_lambda_function)
        return;

    const auto & lambda_func_arguments = lambda_func->arguments->children;
    if (lambda_func_arguments.size() != 2)
        return;

    const auto * tuple_func = lambda_func_arguments[0]->as<ASTFunction>();
    if (!tuple_func || tuple_func->name != "tuple")
        return;

    const auto & tuple_arguments = tuple_func->arguments->children;
    if (tuple_arguments.size() != 1)
        return;

    const auto * id = tuple_arguments[0]->as<ASTIdentifier>();
    if (!id)
        return;

    const auto * filter_func = lambda_func_arguments[1]->as<ASTFunction>();
    if (!filter_func || filter_func->name != "equals")
        return;

    auto & filter_arguments = filter_func->arguments->children;
    if (filter_arguments.size() != 2)
        return;

    const ASTIdentifier * filter_id = nullptr;
    if ((filter_id = filter_arguments[0]->as<ASTIdentifier>()) && filter_arguments[1]->as<ASTLiteral>()
        && filter_id->full_name == id->full_name)
    {
        /// arrayExists(x -> x = elem, arr) -> has(arr, elem)
        auto new_func = makeASTFunction("has", std::move(array_exists_arguments[1]), std::move(filter_arguments[1]));
        new_func->setAlias(func.alias);
        ast = std::move(new_func);
        return;
    }
    else if (
        (filter_id = filter_arguments[1]->as<ASTIdentifier>()) && filter_arguments[0]->as<ASTLiteral>()
        && filter_id->full_name == id->full_name)
    {
        /// arrayExists(x -> elem = x, arr) -> has(arr, elem)
        auto new_func = makeASTFunction("has", std::move(array_exists_arguments[1]), std::move(filter_arguments[0]));
        new_func->setAlias(func.alias);
        ast = std::move(new_func);
        return;
    }
}

}
