#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Aliases.h>

namespace DB
{

class ASTSelectQuery;
struct TableWithColumnNamesAndTypes;

/// AST transformer. It replaces cross joins with equivalented inner join if possible.
class CrossToInnerJoinMatcher
{
public:
    struct Data
    {
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns;
        const Aliases & aliases;
        const String current_database;
        bool done = false;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);
};

using CrossToInnerJoinVisitor = InDepthNodeVisitor<CrossToInnerJoinMatcher, true>;

}
