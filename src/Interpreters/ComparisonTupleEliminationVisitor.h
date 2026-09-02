#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Aliases.h>

namespace DB
{

class ASTSelectQuery;
struct TableWithColumnNamesAndTypes;

/** Replaces tuple comparisons with multiple comparisons.
  *
  * Example: SELECT id FROM test_table WHERE (id, value) = (1, 'Value');
  * Result: SELECT id FROM test_table WHERE id = 1 AND value = 'Value';
  */
class ComparisonTupleEliminationMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & ast, Data & data);
};

using ComparisonTupleEliminationVisitor = InDepthNodeVisitor<ComparisonTupleEliminationMatcher, true>;

}
