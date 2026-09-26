#pragma once

#include <Parsers/IAST_fwd.h>

#include <cstddef>


namespace DB
{

/// Counts the AST elements that the propagation of `WITH` elements copies into a query and throws
/// `TOO_BIG_AST` once they exceed `max_expanded_ast_elements`. A copied element can itself carry copies
/// of the elements declared before it, so a `WITH` list whose elements refer to the preceding ones, or a
/// chain of common table expressions each reading the previous one twice, grows exponentially.
/// Zero means no limit.
class ExpandedASTBudget
{
public:
    explicit ExpandedASTBudget(size_t max_elements_) : max_elements(max_elements_) {}

    ASTPtr clone(const ASTPtr & ast);

private:
    const size_t max_elements;
    size_t used_elements = 0;
};

}
