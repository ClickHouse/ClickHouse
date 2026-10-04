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

class ApplyWithSubqueryVisitor
{
public:
    struct Data
    {
        /// A CTE body together with whether its list was `WITH RECURSIVE`, kept in one entry so that
        /// re-declaring the name in a nested list replaces both at once.
        struct Subquery
        {
            ASTPtr ast;
            bool recursive_with = false;
        };

        std::map<String, Subquery> subqueries;
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

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
