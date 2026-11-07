#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Common/checkStackSize.h>
#include <Common/Exception.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
    }

    void checkExpressionDoesntContainSubqueries(const IAST & ast)
    {
        checkStackSize();

        if (ast.as<ASTSubquery>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key expressions cannot contain subqueries");

        for (const auto & child : ast.children)
            checkExpressionDoesntContainSubqueries(*child);
    }

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
