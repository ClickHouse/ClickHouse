#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;
class ASTIdentifier;


struct ExpressionInfoMatcher
{
    struct Data : public WithContext
    {
        const TablesWithColumns & tables;

        bool is_array_join = false;
        bool is_stateful_function = false;
        bool is_aggregate_function = false;
        bool is_window_function = false;
        bool is_deterministic_function = true;
        std::unordered_set<size_t> unique_reference_tables_pos = {};
    };

    static void visit(const ASTPtr & ast, Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);

    static void visit(const ASTFunction & ast_function, const ASTPtr &, Data & data);

    static void visit(const ASTIdentifier & identifier, const ASTPtr &, Data & data);
};

using ExpressionInfoVisitor = ConstInDepthNodeVisitor<ExpressionInfoMatcher, true>;

bool hasNonRewritableFunction(const ASTPtr & node, ContextPtr context);

}
