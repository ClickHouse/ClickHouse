#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node)
    {
        if (!node)
            return make_intrusive<ASTExpressionList>();

        const auto * expr_func = node->as<ASTFunction>();

        if (expr_func && expr_func->name == "tuple")
        {
            if (expr_func->arguments)
                /// Primary key is specified in tuple, extract its arguments.
                return expr_func->arguments->clone();
            return make_intrusive<ASTExpressionList>();
        }

        /// Primary key consists of one column.
        auto res = make_intrusive<ASTExpressionList>();
        res->children.push_back(node->clone());
        return res;
    }
}
