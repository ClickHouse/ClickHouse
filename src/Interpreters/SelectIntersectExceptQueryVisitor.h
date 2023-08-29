#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class ASTFunction;
class ASTSelectWithUnionQuery;

class SelectIntersectExceptQueryMatcher
{
public:
    struct Data {};

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
};

/// Visit children first.
using SelectIntersectExceptQueryVisitor
    = InDepthNodeVisitor<SelectIntersectExceptQueryMatcher, false>;
}
