#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>

namespace DB
{

class ASTSelectQuery;

/// AST transformer. It replaces cross joins with equivalented inner join if possible.
class JoinReorderingMatcher
{
public:
    struct Data
    {
        const std::vector<StoragePtr> & storages;
        TablesWithColumns & tables; /// This will be reordered after visit() is called.
        const Aliases & aliases;
        const Settings & settings;
        const String current_database;
        Names pre_reorder_names = {};
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);
};

using JoinReorderingVisitor = InDepthNodeVisitor<JoinReorderingMatcher, true>;

}
