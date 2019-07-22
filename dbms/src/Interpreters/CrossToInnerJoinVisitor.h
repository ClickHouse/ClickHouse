#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;

/// AST transformer. It replaces cross joins with equivalented inner join if possible.
class CrossToInnerJoinMatcher
{
public:
    struct Data
    {
        bool done = false;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);
};

using CrossToInnerJoinVisitor = InDepthNodeVisitor<CrossToInnerJoinMatcher, true>;

}
