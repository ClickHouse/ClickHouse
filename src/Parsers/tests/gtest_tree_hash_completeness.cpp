#include <gtest/gtest.h>

#include <Parsers/ASTFromJSON.h>
#include <Parsers/ASTToJSON.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

/// Members that live outside `children` must still be significant for `getTreeHash`, and a
/// JSON round trip of the same definition must hash identically to the parsed original.

namespace
{

ASTPtr parse(const std::string & query)
{
    ParserQuery parser(query.data() + query.size(), false, false);
    return parseQuery(parser, query, 0, 0, 0);
}

IASTHash hashOf(const std::string & query)
{
    return parse(query)->getTreeHash(/*ignore_aliases=*/ false);
}

IASTHash hashOfJSONRoundTrip(const std::string & query)
{
    auto ast = parse(query);
    auto restored = IAST::createFromJSON(serializeASTToJSON(*ast), /*max_depth=*/ 1000, /*max_elements=*/ 100000);
    return restored->getTreeHash(/*ignore_aliases=*/ false);
}

}

TEST(TreeHashCompleteness, ColumnCollationIsSignificant)
{
    /// `ASTCollation` keeps the collation name out of `children`.
    const std::string case_sensitive = "CREATE TABLE t (c String COLLATE utf8_cs) ENGINE = Memory";
    const std::string case_insensitive = "CREATE TABLE t (c String COLLATE utf8_ci) ENGINE = Memory";

    EXPECT_NE(hashOf(case_sensitive), hashOf(case_insensitive));
    EXPECT_EQ(hashOf(case_sensitive), hashOf(case_sensitive));
}

TEST(TreeHashCompleteness, ColumnCollationJSONRoundTripHashesEqual)
{
    /// `readJSON` must reproduce the parser's shape, where the collation name is not a child.
    const std::string query = "CREATE TABLE t (c String COLLATE utf8_cs) ENGINE = Memory";
    EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query));
}

TEST(TreeHashCompleteness, WithElementJSONRoundTripHashesEqual)
{
    /// `ASTWithElement::updateTreeHashImpl` hashes `aliases` explicitly because the parser keeps
    /// them out of `children`; a JSON-built copy must not hash them a second time through
    /// `children`.
    const std::string query = "WITH x (r) AS (SELECT 1 AS q) SELECT r FROM x";
    EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query));

    const std::string no_aliases = "WITH x AS (SELECT 1 AS q) SELECT q FROM x";
    EXPECT_NE(hashOf(query), hashOf(no_aliases));
    EXPECT_EQ(hashOfJSONRoundTrip(no_aliases), hashOf(no_aliases));
}

TEST(TreeHashCompleteness, StreamSettingsAreSignificant)
{
    /// The cursor tree and the watermark column/idle timeout are not children.
    const std::string cursor_ten = "SELECT * FROM t STREAM CURSOR {'all': {'block_number': 10}}";
    const std::string cursor_eleven = "SELECT * FROM t STREAM CURSOR {'all': {'block_number': 11}}";

    EXPECT_NE(hashOf(cursor_ten), hashOf(cursor_eleven));
    EXPECT_EQ(hashOf(cursor_ten), hashOf(cursor_ten));

    const std::string watermark_a = "SELECT * FROM t STREAM WATERMARK FOR a AS a - 1";
    const std::string watermark_b = "SELECT * FROM t STREAM WATERMARK FOR b AS a - 1";
    EXPECT_NE(hashOf(watermark_a), hashOf(watermark_b));

    const std::string watermark_timeout = "SELECT * FROM t STREAM WATERMARK FOR a AS a - 1 IDLE TIMEOUT INTERVAL 5 SECOND";
    EXPECT_NE(hashOf(watermark_a), hashOf(watermark_timeout));
}

TEST(TreeHashCompleteness, GroupingSetsFlagIsSignificant)
{
    /// `group_by_with_grouping_sets` is not a child, and the SQL parser always pairs it with a
    /// nested-list `GROUP BY`, so no pair of queries differs by the flag alone. `readJSON` accepts
    /// the flag independently of that shape, so a JSON-built AST can.
    const std::string query = "SELECT a FROM t GROUP BY GROUPING SETS ((a))";
    const String json = serializeASTToJSON(*parse(query));

    const String key = "\"group_by_with_grouping_sets\":true,";
    const auto pos = json.find(key);
    ASSERT_NE(pos, String::npos);
    String without_flag = json;
    without_flag.erase(pos, key.size());

    auto restored = IAST::createFromJSON(without_flag, /*max_depth=*/ 1000, /*max_elements=*/ 100000);
    EXPECT_NE(restored->formatWithSecretsOneLine(), parse(query)->formatWithSecretsOneLine());
    EXPECT_NE(restored->getTreeHash(/*ignore_aliases=*/ false), hashOf(query));

    EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query));
}
