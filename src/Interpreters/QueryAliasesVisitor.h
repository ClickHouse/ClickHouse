#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

struct QueryAliasesWithSubqueries
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

struct QueryAliasesNoSubqueries
{
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};

/// Visits AST node to collect aliases.
template <typename Helper>
class QueryAliasesMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<QueryAliasesMatcher, false>;

    using Data = Aliases;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child) { return Helper::needChildVisit(node, child); }

private:
    static void visit(const ASTSelectQuery & select, const ASTPtr & ast, Data & data);
    static void visit(const ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static void visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using QueryAliasesVisitor = QueryAliasesMatcher<QueryAliasesWithSubqueries>::Visitor;
using QueryAliasesNoSubqueriesVisitor = QueryAliasesMatcher<QueryAliasesNoSubqueries>::Visitor;

}
