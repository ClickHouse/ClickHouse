#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <unordered_set>

namespace DB
{

/// Matcher that collects all table names in a SELECT query:
/// from FROM / JOIN clauses and also table-function invocations.
struct CollectTablesMatcher
{
    struct Data
    {
        std::unordered_set<String> tables;
        std::unordered_set<String> seen;
    };

    /// Called for each AST node
    static void visit(ASTPtr & node, Data & data)
    {
        // 1) If root is ASTSelectQuery, pull explicit table list
        if (auto * select = typeid_cast<ASTSelectQuery *>(node.get()))
        {
            if (select->tables())
            {
                for (auto & child : select->tables()->children)
                {
                    // e.g. FROM table
                    if (auto * table_expr = typeid_cast<ASTTableExpression *>(child.get()))
                    {
                        if (table_expr->database_and_table_name)
                        {
                            String tbl = table_expr->database_and_table_name->getColumnName();
                            if (data.seen.insert(tbl).second)
                                data.tables.insert(tbl);
                        }
                    }
                    // e.g. JOIN table
                    else if (auto * join = typeid_cast<ASTTableJoin *>(child.get()))
                    {
                        for (auto join_child :join->children)
                            visit(join_child, data);
                    }
                }
            }
        }
        // 2) Also catch any standalone ASTTableExpression deeper in AST
        else if (auto * table_expr = typeid_cast<ASTTableExpression *>(node.get()))
        {
            if (table_expr->database_and_table_name)
            {
                String tbl = table_expr->database_and_table_name->getColumnName();
                if (data.seen.insert(tbl).second)
                    data.tables.insert(tbl);
            }
        }
    }

    /// Always recurse into children
    static bool needChildVisit(const ASTPtr & /*node*/, const ASTPtr & /*child*/) { return true; }
};

/// Wrapper to apply the matcher
inline void collectTables(ASTPtr & ast, CollectTablesMatcher::Data & data)
{
    InDepthNodeVisitor<CollectTablesMatcher, /*checkFirst*/ false> visitor{data, nullptr};
    visitor.visit(ast);
}

} // namespace DB
