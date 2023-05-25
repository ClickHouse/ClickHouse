#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite 'sum(if())' and 'sumIf' functions to counIf.
/// sumIf(1, cond) -> countIf(1, cond)
/// sumIf(123, cond) -> 123 * countIf(1, cond)
/// sum(if(cond, 1, 0)) -> countIf(cond)
/// sum(if(cond, 123, 0)) -> 123 * countIf(cond)
/// sum(if(cond, 0, 1)) -> countIf(not(cond))
/// sum(if(cond, 0, 123)) -> 123 * countIf(not(cond))
class RewriteSumIfFunctionMatcher
{
public:
    struct Data
    {
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTFunction &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteSumIfFunctionVisitor = InDepthNodeVisitor<RewriteSumIfFunctionMatcher, false>;
}
