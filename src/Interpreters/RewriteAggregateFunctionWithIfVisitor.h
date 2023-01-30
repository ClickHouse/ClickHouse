#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite '<aggregate-function>(if())' to '<aggregate-function>If[OrNull]()'
/// sum(if(cond, a, 0)) -> sumIf[OrNull](a, cond)
/// sum(if(cond, a, null)) -> sumIf[OrNull](a, cond)
/// avg(if(cond, a, null)) -> avgIf[OrNull](a, cond)
/// ...
class RewriteAggregateFunctionWithIfMatcher
{
public:
    struct Data
    {
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTFunction &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteAggregateFunctionWithIfVisitor = InDepthNodeVisitor<RewriteAggregateFunctionWithIfMatcher, false>;
}
