#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Aliases.h>

namespace DB
{

class ASTSelectQuery;
struct TableWithColumnNamesAndTypes;

/// AST transformer. It replaces outer joins with equivalented inner join if possible.
/// If the right table has predicates that can filter null values, then the [full outer join]/[left join] can be rewritten as [right outer join]/[inner join].
/// If the left table has predicates that can filter null values, then the [full outer join]/[right join] can be rewritten as [left outer join]/[inner join].
class OuterToInnerJoinMatcher
{
public:
    struct Data
    {
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns;
        const Aliases & aliases;
        const String current_database;
        UInt8 outer_to_inner_join_rewrite = 1;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);

private:
    static void visit(ASTSelectQuery & select, ASTPtr & ast, Data & data);
};

using OuterToInnerJoinVisitor = InDepthNodeVisitor<OuterToInnerJoinMatcher, true>;
}
