#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/OptimizeIfWithConstantConditionVisitor.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static bool tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value)
{
    /// numeric constant in condition
    if (const auto * literal = condition->as<ASTLiteral>())
    {
        if (literal->value.getType() == Field::Types::Int64 ||
            literal->value.getType() == Field::Types::UInt64)
        {
            value = literal->value.get<Int64>();
            return true;
        }
    }

    /// cast of numeric constant in condition to UInt8
    if (const auto * function = condition->as<ASTFunction>())
    {
        if (function->name == "CAST")
        {
            if (const auto * expr_list = function->arguments->as<ASTExpressionList>())
            {
                if (expr_list->children.size() != 2)
                    throw Exception("Function CAST must have exactly two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                const ASTPtr & type_ast = expr_list->children.at(1);
                if (const auto * type_literal = type_ast->as<ASTLiteral>())
                {
                    if (type_literal->value.getType() == Field::Types::String &&
                        type_literal->value.get<std::string>() == "UInt8")
                        return tryExtractConstValueFromCondition(expr_list->children.at(0), value);
                }
            }
        }
    }

    return false;
}

void OptimizeIfWithConstantConditionVisitor::visit(ASTPtr & current_ast)
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        auto * function_node = child->as<ASTFunction>();
        if (!function_node || function_node->name != "if")
        {
            visit(child);
            continue;
        }

        visit(function_node->arguments);
        const auto * args = function_node->arguments->as<ASTExpressionList>();

        if (args->children.size() != 3)
            throw Exception("Wrong number of arguments for function 'if' (" + toString(args->children.size()) + " instead of 3)",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTPtr condition_expr = args->children[0];
        ASTPtr then_expr = args->children[1];
        ASTPtr else_expr = args->children[2];

        bool condition;
        if (tryExtractConstValueFromCondition(condition_expr, condition))
        {
            ASTPtr replace_ast = condition ? then_expr : else_expr;
            ASTPtr child_copy = child;
            String replace_alias = replace_ast->tryGetAlias();
            String if_alias = child->tryGetAlias();

            if (replace_alias.empty())
            {
                replace_ast->setAlias(if_alias);
                child = replace_ast;
            }
            else
            {
                /// Only copy of one node is required here.
                /// But IAST has only method for deep copy of subtree.
                /// This can be a reason of performance degradation in case of deep queries.
                ASTPtr replace_ast_deep_copy = replace_ast->clone();
                replace_ast_deep_copy->setAlias(if_alias);
                child = replace_ast_deep_copy;
            }

            if (!if_alias.empty())
            {
                auto alias_it = aliases.find(if_alias);
                if (alias_it != aliases.end() && alias_it->second.get() == child_copy.get())
                    alias_it->second = child;
            }
        }
    }
}

}
