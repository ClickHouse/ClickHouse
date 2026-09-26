#pragma once

#include <map>

#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTFunction;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;
struct ASTTableExpression;
class ASTWithElement;

class ApplyWithSubqueryVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
        std::map<String, ASTPtr> literals;
        /// Expression aliases declared with `enable_scopes_for_with_statement` disabled. They reach every
        /// nested select, including through one that does not resolve them itself, so they are kept apart
        /// from the aliases visible in the current scope.
        std::map<String, ASTPtr> exported_literals;
        /// When set, each subquery's own settings are applied while descending, so that an inherited
        /// element is not substituted into a subquery whose settings hide it.
        ContextPtr context;
    };

    static void visit(ASTPtr & ast) { visit(ast, Data{}); }
    static void visit(ASTPtr & ast, ContextPtr context)
    {
        Data data;
        data.context = std::move(context);
        visit(ast, data);
    }
    static void visit(ASTSelectQuery & select) { visit(select, {}); }
    static void visit(ASTSelectWithUnionQuery & select) { visit(select, {}); }

    /// The branches of a recursive element's body: the branches of a `UNION`, or the operands of an
    /// `INTERSECT` / `EXCEPT`, either reached through any number of single-branch wrappers. This is
    /// the rule `QueryTreeBuilder` applies, so a body that it takes for a recursive element is taken
    /// for one here too. Null when the body is a single `SELECT`, which is an ordinary CTE within a
    /// `WITH RECURSIVE` list. The first branch is the seed, the ones after it are the recursive members.
    static ASTs * getRecursiveBodyBranches(const ASTPtr & subquery);

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visitRecursiveWithElement(ASTWithElement & with_element, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
