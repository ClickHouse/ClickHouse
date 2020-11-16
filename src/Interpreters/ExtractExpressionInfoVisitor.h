#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

class Context;

struct ExpressionInfoMatcher
{
    struct Data
    {
        const Context & context;
        const TablesWithColumns & tables;

        bool is_array_join = false;
        bool is_stateful_function = false;
        bool is_aggregate_function = false;
        std::unordered_set<size_t> unique_reference_tables_pos = {};
    };

    static void visit(const ASTPtr & ast, Data & data);

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &);

    static void visit(const ASTFunction & ast_function, const ASTPtr &, Data & data);

    static void visit(const ASTIdentifier & identifier, const ASTPtr &, Data & data);
};

using ExpressionInfoVisitor = ConstInDepthNodeVisitor<ExpressionInfoMatcher, true>;

bool hasStatefulFunction(const ASTPtr & node, const Context & context);

}
