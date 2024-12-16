#include <Interpreters/ComparisonGraph.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/FieldVisitorToString.h>

#include <gtest/gtest.h>

using namespace DB;

static ComparisonGraph<ASTPtr> getGraph(const String & query)
{
    ParserExpressionList parser(false);
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    return ComparisonGraph<ASTPtr>(ast->children);
}

TEST(ComparisonGraph, Bounds)
{
    String query = "x <= 1, 1 < c, 3 < c, c < d, d < e, e < 7, e < 10, 10 <= y";
    auto graph = getGraph(query);

    auto d = std::make_shared<ASTIdentifier>("d");

    {
        auto res = graph.getConstLowerBound(d);
        ASSERT_TRUE(res.has_value());

        const auto & [lower, strict] = *res;

        ASSERT_EQ(lower.safeGet<UInt64>(), 3);
        ASSERT_TRUE(strict);
    }

    {
        auto res = graph.getConstUpperBound(d);
        ASSERT_TRUE(res.has_value());

        const auto & [upper, strict] = *res;

        ASSERT_EQ(upper.safeGet<UInt64>(), 7);
        ASSERT_TRUE(strict);
    }

    {
        auto x = std::make_shared<ASTIdentifier>("x");
        auto y = std::make_shared<ASTIdentifier>("y");

        ASSERT_EQ(graph.compare(x, y), ComparisonGraphCompareResult::LESS);
        ASSERT_EQ(graph.compare(y, x), ComparisonGraphCompareResult::GREATER);
    }
}

using Components = std::set<std::set<String>>;

static std::set<String> componentToStrings(const ASTs & comp)
{
    std::set<String> res;
    for (const auto & ast : comp)
        res.insert(ast->getColumnName());
    return res;
}

static void checkComponents(const String & query, const Components & expected)
{
    auto graph = getGraph(query);

    size_t num_components = graph.getNumOfComponents();
    ASSERT_EQ(num_components, expected.size());

    Components res;
    for (size_t i = 0; i < num_components; ++i)
        res.insert(componentToStrings(graph.getComponent(i)));

    ASSERT_EQ(res, expected);
}

TEST(ComparisonGraph, Components)
{
    {
        String query = "a >= b, b >= c, c >= d, d >= b, d >= e, a >= e";
        Components expected = {{"a"}, {"b", "c", "d"}, {"e"}};
        checkComponents(query, expected);
    }

    {
        String query = "a >= b, b >= a, b >= c, c >= d, d >= c";
        Components expected = {{"a", "b"}, {"c", "d"}};
        checkComponents(query, expected);
    }
}

TEST(ComparisonGraph, Compare)
{
    using enum ComparisonGraphCompareResult;

    {
        String query = "a >= b, c >= b";
        auto graph = getGraph(query);

        auto a = std::make_shared<ASTIdentifier>("a");
        auto c = std::make_shared<ASTIdentifier>("c");

        ASSERT_EQ(graph.compare(a, c), UNKNOWN);
    }

    {
        String query = "a >= b, b > c";
        auto graph = getGraph(query);

        auto a = std::make_shared<ASTIdentifier>("a");
        auto b = std::make_shared<ASTIdentifier>("b");
        auto c = std::make_shared<ASTIdentifier>("c");

        ASSERT_EQ(graph.compare(a, c), GREATER);
        ASSERT_EQ(graph.compare(a, b), GREATER_OR_EQUAL);
        ASSERT_EQ(graph.compare(b, c), GREATER);
    }

    {
        String query = "a != b, c < a";
        auto graph = getGraph(query);

        auto a = std::make_shared<ASTIdentifier>("a");
        auto b = std::make_shared<ASTIdentifier>("b");
        auto c = std::make_shared<ASTIdentifier>("c");

        ASSERT_EQ(graph.compare(a, b), NOT_EQUAL);
        ASSERT_EQ(graph.compare(a, c), GREATER);
        ASSERT_EQ(graph.compare(b, c), UNKNOWN);
    }

    {
        /// These constraints are inconsistent.
        String query = "a >= b, b >= a, a != b";
        ASSERT_THROW(getGraph(query), Exception);
    }

    {
        /// These constraints are inconsistent.
        String query = "a > b, b > c, c > a";
        ASSERT_THROW(getGraph(query), Exception);
    }

    {
        String query = "a >= 3, b > a, c >= 3, d >= c";
        auto graph = getGraph(query);

        auto a = std::make_shared<ASTIdentifier>("a");
        auto b = std::make_shared<ASTIdentifier>("b");
        auto d = std::make_shared<ASTIdentifier>("d");
        auto lit_2 = std::make_shared<ASTLiteral>(2u);
        auto lit_3 = std::make_shared<ASTLiteral>(3u);
        auto lit_4 = std::make_shared<ASTLiteral>(4u);

        ASSERT_EQ(graph.compare(lit_3, a), LESS_OR_EQUAL);
        ASSERT_FALSE(graph.isAlwaysCompare(LESS, lit_3, a));
        ASSERT_TRUE(graph.isAlwaysCompare(LESS, lit_2, a));

        ASSERT_EQ(graph.compare(b, lit_2), GREATER);
        ASSERT_EQ(graph.compare(b, lit_3), GREATER);
        ASSERT_EQ(graph.compare(b, lit_4), UNKNOWN);

        ASSERT_EQ(graph.compare(d, lit_2), GREATER);
        ASSERT_EQ(graph.compare(d, lit_3), GREATER_OR_EQUAL);
        ASSERT_EQ(graph.compare(d, lit_4), UNKNOWN);
    }

    {
        String query = "a >= 5, a <= 10";
        auto graph = getGraph(query);

        auto a = std::make_shared<ASTIdentifier>("a");
        auto lit_8 = std::make_shared<ASTLiteral>(8);
        auto lit_3 = std::make_shared<ASTLiteral>(3);
        auto lit_15 = std::make_shared<ASTLiteral>(15);

        ASSERT_EQ(graph.compare(a, lit_8), UNKNOWN);
        ASSERT_EQ(graph.compare(a, lit_3), GREATER);
        ASSERT_EQ(graph.compare(a, lit_15), LESS);
    }
}
