#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

/// Visits AST node to collect aliases.
class QueryAliasesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<QueryAliasesMatcher, false>;

    struct Data
    {
        Aliases & aliases;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static void visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using QueryAliasesVisitor = QueryAliasesMatcher::Visitor;

}
