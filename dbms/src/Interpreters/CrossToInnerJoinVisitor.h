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

    static constexpr const char * label = "JoinToSubqueryTransform";

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }
    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);
};

using CrossToInnerJoinVisitor = InDepthNodeVisitor<CrossToInnerJoinMatcher, true>;

}
