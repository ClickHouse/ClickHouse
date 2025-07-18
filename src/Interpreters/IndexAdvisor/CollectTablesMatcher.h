#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <unordered_set>

namespace DB
{

struct CollectTablesMatcher
{
    struct Data
    {
        std::unordered_set<String> tables;
    };

    static void visit(ASTPtr & node, Data & data)
    {
        if (auto * table_expr = node->as<ASTTableExpression>())
        {
            if (table_expr->database_and_table_name)
            {
                auto table_name = table_expr->database_and_table_name->getColumnName();
                data.tables.insert(table_name);
            }
        }
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/) {
        if (node->as<ASTTableExpression>())
            return false;
        return true;
    }
};

inline void collectTables(ASTPtr & ast, CollectTablesMatcher::Data & data)
{
    InDepthNodeVisitor<CollectTablesMatcher, /*checkFirst*/ false> visitor{data, nullptr};
    visitor.visit(ast);
}

}
