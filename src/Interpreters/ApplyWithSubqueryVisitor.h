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
    /// Replaces references to plain CTEs by copies of their bodies. References to `MATERIALIZED` CTEs stay
    /// identifiers: the analyzer resolves them from the `WITH` list and materializes the CTE once. With a
    /// context, each subquery's own settings decide which inherited names it sees.
    static void visit(ASTPtr & ast, ContextPtr context = nullptr);
    static void visit(ASTSelectQuery & select, ContextPtr context = nullptr);
    static void visit(ASTSelectWithUnionQuery & select, ContextPtr context = nullptr);

private:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
        std::map<String, ASTPtr> literals;
        /// When set, each subquery's own settings are applied while descending, so that an inherited
        /// `subqueries` element is not substituted into a subquery whose settings hide it. Inherited
        /// `literals` are substituted either way.
        ContextPtr context;
    };

    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
