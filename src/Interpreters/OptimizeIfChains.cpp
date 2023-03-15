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
        if (!function_args || function_args->children.size() != 3 || !function_args->children[2])
        {
            visit(child);
            continue;
        }

        const auto * else_arg = function_args->children[2]->as<ASTFunction>();
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

ASTs OptimizeIfChainsVisitor::ifChain(const ASTPtr & child)
{
    const auto * function_node = child->as<ASTFunction>();
    if (!function_node || !function_node->arguments)
        throw Exception("Unexpected AST for function 'if'", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    const auto * function_args = function_node->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 3)
        throw Exception("Wrong number of arguments for function 'if' (" + toString(function_args->children.size()) + " instead of 3)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * else_arg = function_args->children[2]->as<ASTFunction>();

    /// Recursively collect arguments from the innermost if ("head-recursion").
    /// Arguments will be returned in reverse order.

    if (else_arg && else_arg->name == "if")
    {
        auto cur = ifChain(function_node->arguments->children[2]);
        cur.push_back(function_node->arguments->children[1]);
        cur.push_back(function_node->arguments->children[0]);
        return cur;
    }
    else
    {
        ASTs end;
        end.reserve(3);
        end.push_back(function_node->arguments->children[2]);
        end.push_back(function_node->arguments->children[1]);
        end.push_back(function_node->arguments->children[0]);
        return end;
    }
}

}
