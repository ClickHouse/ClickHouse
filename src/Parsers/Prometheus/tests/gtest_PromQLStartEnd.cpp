#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>

using namespace DB;

namespace
{
    void expectStartEndRoundTrip(std::string_view input, std::string_view expected)
    {
        PrometheusQueryTree query_tree{input};
        const auto serialized = query_tree.toString();
        EXPECT_EQ(serialized, expected) << input;

        PrometheusQueryTree reparsed;
        String error_message;
        size_t error_pos = 0;
        ASSERT_TRUE(reparsed.tryParse(serialized, 3, &error_message, &error_pos))
            << input << ": " << error_message << " at position " << error_pos;
        EXPECT_EQ(reparsed.getResultType(), query_tree.getResultType()) << input;
        EXPECT_EQ(reparsed.dumpTree(), query_tree.dumpTree()) << input;
    }
}


TEST(PromQLParser, StandaloneStartEndFunctions)
{
    for (const auto * query :
         {"start()", "end()", "end() - start()", "start\n()", "end # comment\r()", "start # comment\n()"})
    {
        PrometheusQueryTree tree{query};
        EXPECT_EQ(tree.getResultType(), PrometheusQueryTree::ResultType::SCALAR) << query;
        expectStartEndRoundTrip(query, tree.toString());
    }

    /// These names remain valid metric names when they are not called as functions.
    for (const auto * query : {"start", "end", R"(start{job="x"})", R"(end{job="x"})", R"(up{start="x",end="y"})"})
    {
        PrometheusQueryTree tree{query};
        EXPECT_EQ(tree.getResultType(), PrometheusQueryTree::ResultType::INSTANT_VECTOR) << query;
        expectStartEndRoundTrip(query, tree.toString());
    }

    /// The case-insensitive @ modifiers are unchanged.
    expectStartEndRoundTrip("up @ start()", "up @ start()");
    expectStartEndRoundTrip("up @ end()", "up @ end()");
    expectStartEndRoundTrip("up @ START()", "up @ start()");
    expectStartEndRoundTrip("up @ END()", "up @ end()");
}


TEST(PromQLParser, InvalidStandaloneStartEndFunctions)
{
    for (const auto * query : {"start(1)", "end(1)", "start(1, 2)", "end(1, 2)", "START()", "END()", "Start()", "End()", "vector(START())"})
    {
        PrometheusQueryTree tree;
        String error_message;
        size_t error_pos = 0;
        EXPECT_FALSE(tree.tryParse(query, 3, &error_message, &error_pos)) << query;
        EXPECT_FALSE(error_message.empty()) << query;
    }
}
