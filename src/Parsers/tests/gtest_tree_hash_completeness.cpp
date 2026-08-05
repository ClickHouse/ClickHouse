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

TEST(TreeHashCompleteness, SetQueryStandaloneFlagIsSignificant)
{
    /// `is_standalone` is not a child, and the SQL parser fixes it per position, so no pair of
    /// queries differs by the flag alone. `readJSON` accepts it independently of the position, so a
    /// JSON-built AST can. Every position that reads an embedded `ASTSetQuery` is a carrier.
    const std::string queries[] = {
        "CREATE TABLE t (a UInt64, PROJECTION p (SELECT a) WITH SETTINGS (x = 1)) ENGINE = MergeTree ORDER BY a",
        "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1",
        "CREATE TABLE t (a UInt64 SETTINGS (max_compress_block_size = 1)) ENGINE = MergeTree ORDER BY a",
        "SELECT 1 SETTINGS max_threads = 1",
    };

    for (const auto & query : queries)
    {
        const String json = serializeASTToJSON(*parse(query));

        /// `writeJSON` omits the flag for an embedded node, so the carrier injects it.
        const String key = R"("type":"SetQuery",)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos) << query;
        String with_flag = json;
        with_flag.insert(pos + key.size(), "\"is_standalone\":true,");

        auto restored = IAST::createFromJSON(with_flag, /*max_depth=*/ 1000, /*max_elements=*/ 100000);
        EXPECT_NE(restored->formatWithSecretsOneLine(), parse(query)->formatWithSecretsOneLine()) << query;
        EXPECT_NE(restored->getTreeHash(/*ignore_aliases=*/ false), hashOf(query)) << query;

        EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query)) << query;
    }

    EXPECT_NE(hashOf("SET max_threads = 1"), hashOf("SELECT 1 SETTINGS max_threads = 1"));
}

TEST(TreeHashCompleteness, OutputOptionFlagsAreSignificant)
{
    /// `APPEND` / `TRUNCATE` / `AND STDOUT` live in `ASTQueryWithOutput`'s flags, not in `children`,
    /// and every query with an output suffix inherits them.
    const std::string plain = "SELECT 1 INTO OUTFILE 'x'";
    EXPECT_NE(hashOf(plain), hashOf("SELECT 1 INTO OUTFILE 'x' APPEND"));
    EXPECT_NE(hashOf(plain), hashOf("SELECT 1 INTO OUTFILE 'x' TRUNCATE"));
    EXPECT_NE(hashOf(plain), hashOf("SELECT 1 INTO OUTFILE 'x' AND STDOUT"));
    EXPECT_NE(hashOf("SELECT 1 INTO OUTFILE 'x' APPEND"), hashOf("SELECT 1 INTO OUTFILE 'x' TRUNCATE"));

    /// Roots other than `SELECT` reach the same flags through their own parsers.
    EXPECT_NE(hashOf("SHOW TABLES INTO OUTFILE 'x'"), hashOf("SHOW TABLES INTO OUTFILE 'x' APPEND"));
    EXPECT_NE(hashOf("EXPLAIN SELECT 1 INTO OUTFILE 'x'"), hashOf("EXPLAIN SELECT 1 INTO OUTFILE 'x' TRUNCATE"));
    EXPECT_NE(hashOf("CHECK TABLE t INTO OUTFILE 'x'"), hashOf("CHECK TABLE t INTO OUTFILE 'x' AND STDOUT"));

    EXPECT_EQ(hashOfJSONRoundTrip("SELECT 1 INTO OUTFILE 'x' APPEND"), hashOf("SELECT 1 INTO OUTFILE 'x' APPEND"));
}

TEST(TreeHashCompleteness, TemporaryFlagIsSignificant)
{
    /// `TEMPORARY` lives in `ASTQueryWithTableAndOutput`'s flags, so it is not a child either, and it
    /// names a different object rather than formatting the same one differently.
    EXPECT_NE(hashOf("CREATE TABLE t (a UInt64) ENGINE = Memory"),
              hashOf("CREATE TEMPORARY TABLE t (a UInt64) ENGINE = Memory"));
    EXPECT_NE(hashOf("DROP TABLE t"), hashOf("DROP TEMPORARY TABLE t"));
    EXPECT_NE(hashOf("EXISTS TABLE t"), hashOf("EXISTS TEMPORARY TABLE t"));

    EXPECT_EQ(hashOfJSONRoundTrip("CREATE TEMPORARY TABLE t (a UInt64) ENGINE = Memory"),
              hashOf("CREATE TEMPORARY TABLE t (a UInt64) ENGINE = Memory"));
}

TEST(TreeHashCompleteness, ExplicitUuidIsSignificant)
{
    /// `uuid` is a plain member of `ASTQueryWithTableAndOutput`, and the `database` / `table`
    /// children are plain identifiers, so nothing else brings it into the hash.
    const std::string one = "CREATE TABLE db.t UUID '00000000-0000-0000-0000-000000000001' (a UInt64) ENGINE = Memory";
    const std::string two = "CREATE TABLE db.t UUID '00000000-0000-0000-0000-000000000002' (a UInt64) ENGINE = Memory";
    const std::string none = "CREATE TABLE db.t (a UInt64) ENGINE = Memory";

    EXPECT_NE(hashOf(one), hashOf(two));
    EXPECT_NE(hashOf(one), hashOf(none));
}
