#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class ASTFunction;

/// Really simple rewrite 'select countDistinct(a) from t' to 'select count(1) from (select a from t groupBy a)'
class RewriteCountDistinctFunctionMatcher
{
public:
    struct Data
    {
        ContextPtr context;
    };
    static void visit(ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteCountDistinctFunctionVisitor = InDepthNodeVisitor<RewriteCountDistinctFunctionMatcher, true>;
}
