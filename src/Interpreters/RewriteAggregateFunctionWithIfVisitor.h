#pragma once


#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{

class ASTFunction;

/// Rewrite 'sum(if())' to 'sumIf()'
/// sum(if(cond, a, 0)) -> sumIf(a, cond)
/// sum(if(cond, a, null)) -> sumIf(a, cond)
/// avg(if(cond, a, null)) -> avgIf(a, cond)
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
