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
class ExpandedASTBudget;

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
        ExpandedASTBudget * budget = nullptr;
    };

    /// Each overload throws `TOO_BIG_AST` when the substituted copies exceed `max_expanded_ast_elements`
    /// (zero means no limit). The overload with a context takes the limit from its settings.
    static void visit(ASTPtr & ast, size_t max_expanded_ast_elements);
    static void visit(ASTPtr & ast, ContextPtr context);
    static void visit(ASTSelectQuery & select, size_t max_expanded_ast_elements);
    static void visit(ASTSelectWithUnionQuery & select, size_t max_expanded_ast_elements);

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

}
