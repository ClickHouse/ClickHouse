#pragma once

#include <map>
#include <memory>
#include <set>
#include <unordered_set>

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
        std::map<String, ASTPtr> subqueries;
        std::map<String, ASTPtr> literals;
        /// When set, each subquery's own settings are applied while descending, so that an inherited
        /// `subqueries` element is not substituted into a subquery whose settings hide it. Inherited
        /// `literals` are substituted either way.
        ContextPtr context;
        /// Stored view definitions: `MATERIALIZED` CTEs are not expanded; their names are scoped like
        /// `subqueries`, and the identifiers left in place are reported through `kept_cte_references` when set.
        bool keep_materialized_cte = false;
        std::set<String> materialized_ctes;
        std::unordered_set<const IAST *> * kept_cte_references = nullptr;
        /// Keep mode: the scope each plain CTE's body was visited with, to classify its expansion copies.
        std::map<String, std::shared_ptr<const Data>> cte_declaration_scopes;
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

    /// Expands plain CTE references and leaves references to `MATERIALIZED` CTEs as identifiers for the
    /// analyzer to resolve when the stored query runs. Returns the identifier nodes of the resulting AST.
    static std::unordered_set<const IAST *> visitKeepingMaterializedCTEs(ASTSelectWithUnionQuery & select);

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
