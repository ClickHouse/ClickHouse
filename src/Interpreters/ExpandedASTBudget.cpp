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

void ExpandedASTBudget::charge(size_t added_elements, size_t refunded_elements)
{
    if (!root_counted)
    {
        used_elements += root.checkSize(max_elements);
        root_counted = true;
    }

    used_elements += added_elements;
    used_elements -= std::min(refunded_elements, used_elements);
    if (used_elements > max_elements)
        throw Exception(ErrorCodes::TOO_BIG_AST,
            "AST is too big after the expansion of WITH elements. Maximum: {}", max_elements);
}

ASTPtr ExpandedASTBudget::clone(const ASTPtr & ast)
{
    /// Counting before cloning keeps the work proportional to the limit: `checkSize` stops at the
    /// first subtree over the limit, and nothing is copied once the budget is spent.
    if (max_elements)
        charge(ast->checkSize(max_elements), 0);
    return ast->clone();
}

ASTPtr ExpandedASTBudget::replace(const ASTPtr & old_node, const ASTPtr & ast)
{
    if (max_elements)
    {
        /// `old_node` belongs to the query, which `charge` has counted or is about to count, so it is within the limit.
        size_t old_elements = old_node ? old_node->checkSize(max_elements) : 0;
        charge(ast->checkSize(max_elements), old_elements);
    }
    return ast->clone();
}

void ExpandedASTBudget::addNode()
{
    if (max_elements)
        charge(1, 0);
}

}
