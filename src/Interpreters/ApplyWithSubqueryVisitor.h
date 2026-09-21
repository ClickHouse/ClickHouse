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
    using KeptCTEReferences = std::unordered_set<const IAST *>;

    /// Replaces references to plain CTEs by copies of their bodies. References to `MATERIALIZED` CTEs stay
    /// identifiers for the analyzer, which materializes the CTE once; they are returned so that
    /// `AddDefaultDatabaseVisitor` leaves them unqualified. With a context, each subquery's own settings
    /// decide which inherited names it sees.
    static KeptCTEReferences visit(ASTPtr & ast, ContextPtr context = nullptr);
    static KeptCTEReferences visit(ASTSelectQuery & select, ContextPtr context = nullptr);
    static KeptCTEReferences visit(ASTSelectWithUnionQuery & select, ContextPtr context = nullptr);

private:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
        std::map<String, ASTPtr> literals;
        /// When set, each subquery's own settings are applied while descending, so that an inherited
        /// `subqueries` element is not substituted into a subquery whose settings hide it. Inherited
        /// `literals` are substituted either way.
        ContextPtr context;
        std::set<String> materialized_ctes;
        /// Set during the second, read-only pass: the identifiers of the final tree that name a visible `MATERIALIZED` CTE.
        KeptCTEReferences * kept_cte_references = nullptr;
        /// The scope each plain CTE's body was visited with, to classify its expansion copies.
        std::map<String, std::shared_ptr<const Data>> cte_declaration_scopes;
    };

    template <typename T>
    static KeptCTEReferences visitTwice(T & ast, ContextPtr context);
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
