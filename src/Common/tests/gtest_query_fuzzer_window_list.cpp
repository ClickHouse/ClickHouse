#include <gtest/gtest.h>

#include <Common/QueryFuzzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{
/// Count WINDOW-clause children that are not ASTWindowListElement. A non-WLE window child
/// means QueryFuzzer replaced a window list element with an arbitrary expression, which breaks
/// the node-type invariant the analyzer and query tree builder rely on (both cast every
/// window() child to ASTWindowListElement).
size_t countNonWindowListElementWindowChildren(const IAST * ast)
{
    size_t bad = 0;
    if (const auto * select = typeid_cast<const ASTSelectQuery *>(ast))
    {
        if (ASTPtr window = select->window())
        {
            for (const auto & child : window->children)
                if (!typeid_cast<const ASTWindowListElement *>(child.get()))
                    ++bad;
        }
    }
    for (const auto & child : ast->children)
        bad += countNonWindowListElementWindowChildren(child.get());
    return bad;
}
}

/// QueryFuzzer must never replace an ASTWindowListElement in a WINDOW list with a plain
/// expression. It may still recurse into the element to fuzz its window definition.
/// Deterministic over a fixed range of seeds; fails if the fuzzExpressionList guard is removed.
TEST(QueryFuzzer, DoesNotReplaceWindowListElement)
{
    const String sql
        = "SELECT n, sum(n) OVER w1, count() OVER w2 FROM numbers(10) "
          "WINDOW w1 AS (PARTITION BY n ORDER BY n), w2 AS (ORDER BY n), w3 AS ()";

    for (UInt64 seed = 0; seed < 500; ++seed)
    {
        ParserQuery parser(sql.data() + sql.size());
        ASTPtr base = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);
        QueryFuzzer fuzzer{pcg64(seed)};

        /// Feed the same query repeatedly so the persistent fuzzer accumulates window
        /// fragments and exercises the WINDOW-list path across several mutation rounds.
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
            ASSERT_EQ(countNonWindowListElementWindowChildren(fuzzed.get()), 0u) << "seed=" << seed << " step=" << step;
        }
    }
}
