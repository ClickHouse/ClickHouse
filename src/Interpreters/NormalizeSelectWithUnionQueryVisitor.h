#pragma once

#include <unordered_set>

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

#include <Core/Settings.h>

namespace DB
{

class ASTFunction;
class ASTSelectWithUnionQuery;

class NormalizeSelectWithUnionQueryMatcher
{
public:
    struct Data
    {
        const UnionMode & union_default_mode;
    };

    static void getSelectsFromUnionListNode(ASTPtr ast_select, ASTList & selects);

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

/// We need normalize children first, so we should visit AST tree bottom up
using NormalizeSelectWithUnionQueryVisitor
    = InDepthNodeVisitor<NormalizeSelectWithUnionQueryMatcher, false>;
}
