#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/OptimizeIfChains.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

void OptimizeIfChainsVisitor::visit(ASTPtr & current_ast)
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        /// Fallthrough cases

        const auto * function_node = child->as<ASTFunction>();
        if (!function_node || function_node->name != "if" || !function_node->arguments)
        {
            visit(child);
            continue;
        }

        const auto * function_args = function_node->arguments->as<ASTExpressionList>();
        if (!function_args || function_args->children.size() != 3 || !*std::next(function_args->children.begin(), 2))
        {
            visit(child);
            continue;
        }

        const auto * else_arg = (*std::next(function_args->children.begin(), 2))->as<ASTFunction>();
        if (!else_arg || else_arg->name != "if")
        {
            visit(child);
            continue;
        }

        /// The case of:
        /// if(cond, a, if(...))

        auto chain = ifChain(child);
        std::reverse(chain.begin(), chain.end());
        child->as<ASTFunction>()->name = "multiIf";
        child->as<ASTFunction>()->arguments->children = std::move(chain);
    }
}

ASTList OptimizeIfChainsVisitor::ifChain(const ASTPtr & child)
{
    const auto * function_node = child->as<ASTFunction>();
    if (!function_node || !function_node->arguments)
        throw Exception("Unexpected AST for function 'if'", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    const auto * function_args = function_node->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 3)
        throw Exception("Wrong number of arguments for function 'if' (" + toString(function_args->children.size()) + " instead of 3)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * else_arg = function_args->children.back()->as<ASTFunction>();

    /// Recursively collect arguments from the innermost if ("head-recursion").
    /// Arguments will be returned in reverse order.

    if (else_arg && else_arg->name == "if")
    {
        auto cur = ifChain(function_node->arguments->children.back());
        cur.push_back(*++function_node->arguments->children.begin());
        cur.push_back(function_node->arguments->children.front());
        return cur;
    }
    else
    {
        ASTList end;
        end.push_back(function_node->arguments->children.back());
        end.push_back(*++function_node->arguments->children.begin());
        end.push_back(function_node->arguments->children.front());
        return end;
    }
}

}
