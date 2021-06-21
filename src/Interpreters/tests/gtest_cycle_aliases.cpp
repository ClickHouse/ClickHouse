#include <gtest/gtest.h>

#include <Interpreters/QueryNormalizer.h>
#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

using namespace DB;

TEST(QueryNormalizer, SimpleCycleAlias)
{
    String query = "a as b, b as a";
    ParserExpressionList parser(false);
    ASTPtr ast = parseQuery(parser, query, 0, 0);

    Aliases aliases;
    aliases["a"] = parseQuery(parser, "b as a", 0, 0)->children[0];
    aliases["b"] = parseQuery(parser, "a as b", 0, 0)->children[0];

    Settings settings;
    QueryNormalizer::Data normalizer_data(aliases, settings);
    EXPECT_THROW(QueryNormalizer(normalizer_data).visit(ast), Exception);
}
