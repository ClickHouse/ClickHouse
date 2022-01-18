#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Core/Settings.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

class ASTFunction;

class NormalizeSelectWithUnionQueryMatcher
{
public:
    struct Data
    {
        const UnionMode & union_default_mode;
    };

    static void getSelectsFromUnionListNode(ASTPtr ast_select, ASTs & selects);

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

/// We need normalize children first, so we should visit AST tree bottom up
using NormalizeSelectWithUnionQueryVisitor
    = InDepthNodeVisitor<NormalizeSelectWithUnionQueryMatcher, false>;
}
