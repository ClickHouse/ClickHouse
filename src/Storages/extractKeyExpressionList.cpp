#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }

    ASTPtr extractKeyExpressionList(const ASTPtr & node)
    {
        if (!node)
            return std::make_shared<ASTExpressionList>();

        const auto * expr_func = node->as<ASTFunction>();

        if (expr_func && expr_func->name == "tuple")
        {
            if (expr_func->arguments)
                /// Primary key is specified in tuple, extract its arguments.
                return expr_func->arguments->clone();
            else
                return std::make_shared<ASTExpressionList>();
        }
        else
        {
            /// Primary key consists of one column.
            auto res = std::make_shared<ASTExpressionList>();
            res->children.push_back(node);
            return res;
        }
    }

    ASTPtr wrapExpressionListToKeyAST(const ASTPtr & node)
    {
        if (!node)
            return makeASTFunction("tuple");

        const auto * expression_list = node->as<ASTExpressionList>();
        if (!expression_list)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ASTExpressionList");

        if (expression_list->children.size() == 1)
            return expression_list->children.front()->clone();

        auto res = std::make_shared<ASTFunction>();
        res->name = "tuple";
        res->arguments = expression_list->clone();
        res->children.push_back(res->arguments);
        return res;
    }
}
