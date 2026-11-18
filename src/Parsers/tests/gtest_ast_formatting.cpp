#include <Parsers/ASTFormatting.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>


using namespace DB;

namespace
{

String formatWrapped(const String & query, size_t width)
{
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    return formatASTWithWrapping(*ast, IAST::FormatSettings(/*one_line=*/false), width);
}

}

TEST(ASTFormatting, FittingQueryIsNotChanged)
{
    EXPECT_EQ(formatWrapped("SELECT f(a, b) FROM t", 0), "SELECT f(a, b)\nFROM t");
    EXPECT_EQ(formatWrapped("SELECT f(a, b) FROM t", 14), "SELECT f(a, b)\nFROM t");
}

TEST(ASTFormatting, ArgumentsStartOnNewLines)
{
    EXPECT_EQ(
        formatWrapped("SELECT f(g(first_argument), g(second_argument))", 30),
        "SELECT f(\n    g(first_argument),\n    g(second_argument)\n)");
}

TEST(ASTFormatting, PlainValuesArePacked)
{
    EXPECT_EQ(
        formatWrapped("SELECT f(first, second, third, fourth, fifth)", 25),
        "SELECT f(\n    first, second, third,\n    fourth, fifth\n)");
    EXPECT_EQ(
        formatWrapped("SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9]]", 24),
        "SELECT [\n    [1, 2, 3],\n    [4, 5, 6],\n    [7, 8, 9]\n]");
}

TEST(ASTFormatting, NestedChainIsIndentedDeeper)
{
    EXPECT_EQ(
        formatWrapped("SELECT 1 WHERE (first = 1 AND third = 3) OR fourth = 4", 24),
        "SELECT 1\nWHERE ((first = 1)\n        AND (third = 3))\n    OR (fourth = 4)");
}

TEST(ASTFormatting, SubqueryInWrappedArgumentsIsIndented)
{
    EXPECT_EQ(
        formatWrapped("SELECT f((SELECT 1), second_long_argument)", 20),
        "SELECT f(\n    (\n        SELECT 1\n    ),\n    second_long_argument\n)");
}

TEST(ASTFormatting, IndentationIsLimitedByWidth)
{
    /// Wrapping stops when the next level would be indented by the whole width.
    const String query = "SELECT " + String(100, '[') + "1" + String(100, ']');
    EXPECT_EQ(formatWrapped(query, 12), "SELECT [\n    [\n        " + String(98, '[') + "1" + String(98, ']') + "\n    ]\n]");
}
