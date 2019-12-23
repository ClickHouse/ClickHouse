#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/OptimizeIfChains.h>
#include <IO/WriteHelpers.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

    void OptimizeIfChainsVisitor::visit(ASTPtr & current_ast) {
        if (!current_ast) {
            return;
        }
        for (ASTPtr & child : current_ast->children)
        {
            auto * function_node = child->as<ASTFunction>();
            if (!function_node || function_node->name != "if" ||
                (!function_node->arguments->as<ASTExpressionList>()->children[2]->as<ASTFunction>() ||
                 function_node->arguments->as<ASTExpressionList>()->children[2]->as<ASTFunction>()->name != "if"))
            {
                visit(child);
                continue;
            }

            auto chain = if_chain(child);
            reverse(chain.begin(), chain.end());
            child = makeASTFunction("multiIf", std::move(chain));
        }
    }

    ASTs OptimizeIfChainsVisitor::if_chain (ASTPtr & child) {
        auto * function_node = child->as<ASTFunction>();

        const auto * args = function_node->arguments->as<ASTExpressionList>();

        if (args->children.size() != 3)
            throw Exception("Wrong number of arguments for function 'if' (" + toString(args->children.size()) + " instead of 3)",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (args->children[2]->as<ASTFunction>() && args->children[2]->as<ASTFunction>()->name == "if" ) {
            auto cur = if_chain(function_node->arguments->children[2]);
            cur.push_back(function_node->arguments->children[1]);
            cur.push_back(function_node->arguments->children[0]);
            return cur;
        } else {
            ASTs end;
            end.push_back(function_node->arguments->children[2]);
            end.push_back(function_node->arguments->children[1]);
            end.push_back(function_node->arguments->children[0]);
            return end;
        }
    }
}