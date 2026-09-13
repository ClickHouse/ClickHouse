#include <gtest/gtest.h>

#include <Common/QueryFuzzer.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <algorithm>

using namespace DB;

namespace
{
bool isInChildren(const ASTTablesInSelectQueryElement & element, const ASTPtr & member)
{
    return std::find(element.children.begin(), element.children.end(), member) != element.children.end();
}

/// Count sub-nodes an ASTTablesInSelectQueryElement reaches through a typed member but does not
/// list in `children`. A generic AST walk visits only `children`, so such a member is invisible to
/// every visitor, while the formatter and the analyzer read the typed members and still use it.
size_t countMembersMissingFromChildren(const IAST * ast)
{
    size_t missing = 0;
    if (const auto * element = typeid_cast<const ASTTablesInSelectQueryElement *>(ast))
    {
        for (const ASTPtr & member : {element->table_join, element->table_expression, element->array_join})
            if (member && !isInChildren(*element, member))
                ++missing;
    }
    for (const auto & child : ast->children)
        missing += countMembersMissingFromChildren(child.get());
    return missing;
}

/// Number of FROM-list elements carrying a JOIN. The seed below has a single table, so the parser
/// produces exactly one element and its `table_join` is null: a non-null one can only come from
/// QueryFuzzer::addJoinClause().
size_t countJoinElements(const IAST * ast)
{
    size_t joins = 0;
    if (const auto * element = typeid_cast<const ASTTablesInSelectQueryElement *>(ast))
        if (element->table_join)
            ++joins;
    for (const auto & child : ast->children)
        joins += countJoinElements(child.get());
    return joins;
}
}

/// QueryFuzzer must not produce an ASTTablesInSelectQueryElement whose `table_join` or
/// `table_expression` is missing from `children`: a consumer that walks `children` then recurses
/// through an empty vector and never sees the joined relation, while the query still executes with
/// it. Deterministic over a fixed range of seeds.
TEST(QueryFuzzer, AddedJoinElementKeepsChildrenConsistent)
{
    const String sql = "SELECT a, b FROM t1 WHERE a > 5";

    size_t joins_added = 0;
    for (UInt64 seed = 0; seed < 500; ++seed)
    {
        ParserQuery parser(sql.data() + sql.size());
        ASTPtr base = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);
        QueryFuzzer fuzzer{pcg64(seed)};

        /// Feed the same query repeatedly so the persistent fuzzer accumulates the table and
        /// column fragments addJoinClause() needs before it can add a join at all.
        for (int step = 0; step < 8; ++step)
        {
            ASTPtr fuzzed = base->clone();
            try
            {
                fuzzer.fuzzMain(fuzzed);
            }
            catch (...)
            {
                /// The fuzzer can build queries that throw here; that is fine, it does not
                /// affect the invariant we assert on the produced tree.
                continue;
            }
            ASSERT_EQ(countMembersMissingFromChildren(fuzzed.get()), 0u) << "seed=" << seed << " step=" << step;
            joins_added += countJoinElements(fuzzed.get());
        }
    }

    /// Anti-vacuity: a fuzzer that stopped adding joins would leave the assertion above green.
    ASSERT_GT(joins_added, 0u);
}
