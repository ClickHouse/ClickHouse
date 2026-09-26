#include <Interpreters/ExpandedASTBudget.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_BIG_AST;
}

ASTPtr ExpandedASTBudget::clone(const ASTPtr & ast)
{
    if (max_elements)
    {
        /// Counting before cloning keeps the work proportional to the limit: `checkSize` stops at the
        /// first subtree over the limit, and nothing is copied once the budget is spent.
        used_elements += ast->checkSize(max_elements);
        if (used_elements > max_elements)
            throw Exception(ErrorCodes::TOO_BIG_AST,
                "AST is too big after the expansion of WITH elements. Maximum: {}", max_elements);
    }
    return ast->clone();
}

}
