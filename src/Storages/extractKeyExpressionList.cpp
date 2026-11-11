#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/misc.h>
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

        /// An `IN` operator whose right-hand side is a table reference (e.g. `x IN table`) builds a
        /// FutureSet that nobody fills outside of a SELECT pipeline, so evaluating the key during INSERT
        /// aborts with a "Not-ready Set" LOGICAL_ERROR. The subquery form above is already rejected;
        /// the table-identifier form has the same defect and no practical use case, so forbid it too.
        if (const auto * func = ast.as<ASTFunction>(); func && functionIsInOrGlobalInOperator(func->name))
        {
            const auto * args = func->arguments ? func->arguments->as<ASTExpressionList>() : nullptr;
            if (args && args->children.size() == 2)
            {
                const auto & rhs = args->children[1];
                if (rhs->as<ASTIdentifier>() || rhs->as<ASTTableIdentifier>())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key expressions cannot contain a table in the 'IN' operator");
            }
        }

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
