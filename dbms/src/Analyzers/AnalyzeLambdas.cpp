#include <vector>
#include <Analyzers/AnalyzeLambdas.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_LAMBDA;
    extern const int RESERVED_IDENTIFIER_NAME;
}


AnalyzeLambdas::LambdaParameters AnalyzeLambdas::extractLambdaParameters(ASTPtr & ast)
{
    /// Lambda parameters could be specified in AST in two forms:
    /// - just as single parameter: x -> x + 1
    /// - parameters in tuple: (x, y) -> x + 1

#define LAMBDA_ERROR_MESSAGE " There are two valid forms of lambda expressions: x -> ... and (x, y...) -> ..."

    if (!ast->tryGetAlias().empty())
        throw Exception("Lambda parameters cannot have aliases."
            LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

    if (const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        return { identifier->name };
    }
    else if (const ASTFunction * function = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (function->name != "tuple")
            throw Exception("Left hand side of '->' or first argument of 'lambda' is a function, but this function is not tuple."
                LAMBDA_ERROR_MESSAGE " Found function '" + function->name + "' instead.", ErrorCodes::BAD_LAMBDA);

        if (!function->arguments || function->arguments->children.empty())
            throw Exception("Left hand side of '->' or first argument of 'lambda' is empty tuple."
                LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

        LambdaParameters res;
        res.reserve(function->arguments->children.size());

        for (const ASTPtr & arg : function->arguments->children)
        {
            const ASTIdentifier * arg_identifier = typeid_cast<const ASTIdentifier *>(arg.get());

            if (!arg_identifier)
                throw Exception("Left hand side of '->' or first argument of 'lambda' contains something that is not just identifier."
                    LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

            if (!arg_identifier->children.empty())
                throw Exception("Left hand side of '->' or first argument of 'lambda' contains compound identifier."
                    LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

            if (!arg_identifier->alias.empty())
                throw Exception("Lambda parameters cannot have aliases."
                    LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

            res.emplace_back(arg_identifier->name);
        }

        return res;

    }
    else
        throw Exception("Unexpected left hand side of '->' or first argument of 'lambda'."
            LAMBDA_ERROR_MESSAGE, ErrorCodes::BAD_LAMBDA);

#undef LAMBDA_ERROR_MESSAGE
}


namespace
{


/// Currently visible parameters in all scopes of lambda expressions.
/// Lambda expressions could be nested: arrayMap(x -> arrayMap(y -> x[y], x), [[1], [2, 3]])
using LambdaScopes = std::vector<AnalyzeLambdas::LambdaParameters>;

void processIdentifier(ASTPtr & ast, LambdaScopes & lambda_scopes)
{
    ASTIdentifier & identifier = static_cast<ASTIdentifier &>(*ast);

    if (identifier.children.empty())
    {
        bool found = false;

        /// From most inner scope towards outer scopes.
        for (ssize_t num_scopes = lambda_scopes.size(), scope_idx = num_scopes - 1; scope_idx >= 0; --scope_idx)
        {
            for (size_t arg_idx = 0, num_args = lambda_scopes[scope_idx].size(); arg_idx < num_args; ++arg_idx)
            {
                if (lambda_scopes[scope_idx][arg_idx] == identifier.name)
                {
                    identifier.name = "_lambda" + toString(scope_idx) + "_arg" + toString(arg_idx);

                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }

        if (!found && startsWith(identifier.name, "_lambda"))
            throw Exception("Identifier names starting with '_lambda' are reserved for parameters of lambda expressions.",
                ErrorCodes::RESERVED_IDENTIFIER_NAME);
    }
}


void processImpl(
    ASTPtr & ast,
    LambdaScopes & lambda_scopes,
    const ASTPtr & parent_function_for_this_argument,
    AnalyzeLambdas::HigherOrderFunctions & higher_order_functions)
{
    /// Don't go into subqueries and table-like expressions.
    if (typeid_cast<const ASTSelectQuery *>(ast.get())
        || typeid_cast<const ASTTableExpression *>(ast.get()))
    {
        return;
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        /** We must memoize parameters from left hand side (x, y) and then analyze right hand side.
          */
        if (func->name == "lambda")
        {
            auto num_arguments = func->arguments->children.size();
            if (num_arguments != 2)
                throw Exception("Lambda expression ('->' or 'lambda' function) must have exactly two arguments."
                    " Found " + toString(num_arguments) + " instead.", ErrorCodes::BAD_LAMBDA);

            lambda_scopes.emplace_back(AnalyzeLambdas::extractLambdaParameters(func->arguments->children[0]));
            for (size_t i = 0; i < num_arguments; ++i)
                processImpl(func->arguments->children[i], lambda_scopes, nullptr, higher_order_functions);
            lambda_scopes.pop_back();

            if (!parent_function_for_this_argument)
                throw Exception("Lambda expression ('->' or 'lambda' function) must be presented as an argument of higher-order function."
                    " Found standalone lambda expression instead.", ErrorCodes::BAD_LAMBDA);

            higher_order_functions.emplace_back(parent_function_for_this_argument);
        }
        else
        {
            /// When diving into function arguments, pass current ast node.
            if (func->arguments)
                for (auto & child : func->arguments->children)
                    processImpl(child, lambda_scopes, ast, higher_order_functions);

            if (func->parameters)
                for (auto & child : func->parameters->children)
                    processImpl(child, lambda_scopes, nullptr, higher_order_functions);
        }

        return;
    }
    else if (typeid_cast<ASTIdentifier *>(ast.get()))
    {
        processIdentifier(ast, lambda_scopes);
        return;
    }

    for (auto & child : ast->children)
        processImpl(child, lambda_scopes, nullptr, higher_order_functions);
}

}


void AnalyzeLambdas::process(ASTPtr & ast)
{
    LambdaScopes lambda_scopes;
    for (auto & child : ast->children)
        processImpl(child, lambda_scopes, nullptr, higher_order_functions);
}


void AnalyzeLambdas::dump(WriteBuffer & out) const
{
    for (const auto & ast : higher_order_functions)
    {
        writeString(ast->getColumnName(), out);
        writeChar('\n', out);
    }
}


}
