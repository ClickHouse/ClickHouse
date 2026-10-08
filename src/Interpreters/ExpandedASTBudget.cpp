#include <Interpreters/ExpandedASTBudget.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_BIG_AST;
}

void ExpandedASTBudget::charge(const IAST & ast, size_t refunded_elements)
{
    if (!root_counted)
    {
        used_elements += root.checkSize(max_elements);
        root_counted = true;
    }

    /// Counting before cloning keeps the work proportional to the limit: `checkSize` stops at the
    /// first subtree over the limit, and nothing is copied once the budget is spent.
    used_elements += ast.checkSize(max_elements);
    used_elements -= std::min(refunded_elements, used_elements);
    if (used_elements > max_elements)
        throw Exception(ErrorCodes::TOO_BIG_AST,
            "AST is too big after the expansion of WITH elements. Maximum: {}", max_elements);
}

ASTPtr ExpandedASTBudget::clone(const ASTPtr & ast)
{
    if (max_elements)
        charge(*ast, 0);
    return ast->clone();
}

ASTPtr ExpandedASTBudget::replace(const ASTPtr & old_node, const ASTPtr & ast)
{
    if (max_elements)
    {
        /// `old_node` belongs to the query, which `charge` has counted or is about to count, so it is within the limit.
        size_t old_elements = old_node ? old_node->checkSize(max_elements) : 0;
        charge(*ast, old_elements);
    }
    return ast->clone();
}

}
