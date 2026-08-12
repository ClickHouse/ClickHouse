#include <gtest/gtest.h>

#include <Parsers/ASTFromJSON.h>
#include <Parsers/ASTToJSON.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserCreateQuery.h>
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

TEST(TreeHashCompleteness, ColumnDeclarationMembersAreSignificant)
{
    /// `default_specifier`, `null_modifier`, `primary_key_specifier` and the role each child plays
    /// live outside `children`.
    EXPECT_NE(hashOf("CREATE TABLE t (x UInt8 DEFAULT 1) ENGINE = Memory"),
              hashOf("CREATE TABLE t (x UInt8 MATERIALIZED 1) ENGINE = Memory"));
    EXPECT_NE(hashOf("CREATE TABLE t (x UInt8 DEFAULT 1) ENGINE = Memory"),
              hashOf("CREATE TABLE t (x UInt8 ALIAS 1) ENGINE = Memory"));

    EXPECT_NE(hashOf("CREATE TABLE t (x UInt8 NULL) ENGINE = Memory"),
              hashOf("CREATE TABLE t (x UInt8 NOT NULL) ENGINE = Memory"));
    EXPECT_NE(hashOf("CREATE TABLE t (x UInt8) ENGINE = Memory"),
              hashOf("CREATE TABLE t (x UInt8 NULL) ENGINE = Memory"));

    EXPECT_NE(hashOf("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x"),
              hashOf("CREATE TABLE t (x UInt64 PRIMARY KEY) ENGINE = MergeTree"));

    /// The same literal as the only child besides the type, in a different role.
    EXPECT_NE(hashOf("CREATE TABLE t (x String COMMENT 'a') ENGINE = MergeTree ORDER BY x"),
              hashOf("CREATE TABLE t (x String TTL 'a') ENGINE = MergeTree ORDER BY x"));
}

TEST(TreeHashCompleteness, ColumnDeclarationCloneAndJSONRoundTripHashEqual)
{
    /// The parser inserts the column `SETTINGS` child right after the codec, before the TTL and the
    /// collation; `clone` and `readJSON` must reproduce that order, otherwise the copy has a
    /// different shape - and a different hash.
    const std::string column = "x String TTL now() + INTERVAL 1 DAY SETTINGS (max_compress_block_size = 1)";
    ParserColumnDeclaration column_parser(/*require_type_=*/ true, /*allow_null_modifiers_=*/ true);
    ASTPtr ast = parseQuery(column_parser, column, 0, 0, 0);
    const auto hash = ast->getTreeHash(/*ignore_aliases=*/ false);

    EXPECT_EQ(ast->clone()->getTreeHash(/*ignore_aliases=*/ false), hash);

    auto restored = IAST::createFromJSON(serializeASTToJSON(*ast), /*max_depth=*/ 1000, /*max_elements=*/ 100000);
    EXPECT_EQ(restored->getTreeHash(/*ignore_aliases=*/ false), hash);
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

TEST(TreeHashCompleteness, CloneHashesEqual)
{
    /// `ParserQueryWithOutput` appends the output-option children after the query-specific ones,
    /// and most parsers add the database/table children first; every `clone` must rebuild
    /// `children` in the same order, or the copy hashes differently than the original.
    const std::string queries[] = {
        "SHOW CREATE TABLE db.t INTO OUTFILE 'x'",
        "CHECK TABLE db.t PARTITION 1 FORMAT JSONEachRow",
        "CHECK DATABASE db FORMAT JSONEachRow",
        "WATCH db.t LIMIT 5 FORMAT JSONEachRow",
        "OPTIMIZE TABLE db.t PARTITION 1 FINAL DEDUPLICATE BY a, b",
        "ALTER TABLE db.t ADD COLUMN x UInt64 FORMAT JSONEachRow",
        "DROP TABLE db.t",
        "DROP TABLE t1, t2",
        "UNDROP TABLE db.t",
        "EXISTS TABLE db.t INTO OUTFILE 'x'",
        "DESCRIBE TABLE db.t FORMAT JSONEachRow",
        "CREATE TABLE db.t (a UInt64) ENGINE = MergeTree ORDER BY a",
        "KILL QUERY WHERE query_id = 'x' FORMAT JSONEachRow",
        "SELECT 1 INTO OUTFILE 'x' FORMAT JSONEachRow",
    };

    for (const auto & query : queries)
    {
        ASTPtr ast = parse(query);
        EXPECT_EQ(ast->clone()->getTreeHash(/*ignore_aliases=*/ false), ast->getTreeHash(/*ignore_aliases=*/ false)) << query;
    }
}

TEST(TreeHashCompleteness, FormatRoundTripHashesEqual)
{
    /// The debug build verifies for every incoming query that formatting the AST and parsing it
    /// back yields the same tree hash, so every member this file makes significant must survive a
    /// format+parse round trip.
    const std::string queries[] = {
        /// Per-column PRIMARY KEY: the parser moves it into the storage definition and must clear
        /// `primary_key_specifier`, which formatting does not reproduce per column in CREATE.
        "CREATE TABLE t (a UInt8 PRIMARY KEY, b String PRIMARY KEY) ENGINE = MergeTree",
        /// In ALTER there is no storage definition to move the specifier into, so formatting must
        /// print it.
        "ALTER TABLE t ADD COLUMN x UInt64 PRIMARY KEY",
        "SELECT 1 UNION DISTINCT SELECT 2 UNION ALL SELECT 3",
        "SELECT 1 INTERSECT SELECT 2 EXCEPT SELECT 3",
        "FROM numbers(10) SELECT number ORDER BY number DESC WITH FILL FROM 10 TO 5 STEP -1 LIMIT 3 OFFSET 2",
        "SELECT count() OVER (PARTITION BY number ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(3)",
        "SELECT count() OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(3)",
        "SELECT count() OVER w FROM numbers(3) WINDOW w AS (ORDER BY number)",
        "WITH x (r) AS MATERIALIZED (SELECT 1 AS q) SELECT r FROM x",
        "SET max_threads = DEFAULT, max_block_size = 1",
        "CREATE TABLE t (a UInt64, b String TTL now() + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY a "
            "TTL now() + INTERVAL 1 MONTH GROUP BY a SET b = max(b), now() + INTERVAL 2 MONTH RECOMPRESS CODEC(ZSTD(3)), "
            "now() + INTERVAL 3 MONTH TO DISK 'd'",
        "CREATE TABLE t (a UInt64, INDEX i a TYPE minmax GRANULARITY 4, CONSTRAINT c CHECK a > 0, CONSTRAINT d ASSUME a < 10, "
            "PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a",
        "SELECT 1 INTO OUTFILE 'x' APPEND AND STDOUT",
    };

    for (const auto & query : queries)
    {
        ASTPtr ast = parse(query);
        const String formatted = ast->formatWithSecretsOneLine();
        ASTPtr ast2 = parse(formatted);
        EXPECT_EQ(ast2->getTreeHash(/*ignore_aliases=*/ false), ast->getTreeHash(/*ignore_aliases=*/ false))
            << query << "\nformatted: " << formatted;
        /// Formatting must also be a fixed point of the round trip.
        EXPECT_EQ(ast2->formatWithSecretsOneLine(), formatted) << query;
    }

    /// The specifier must not be silently dropped from a formatted ALTER.
    EXPECT_TRUE(parse("ALTER TABLE t ADD COLUMN x UInt64 PRIMARY KEY")->formatWithSecretsOneLine().contains("PRIMARY KEY"));
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
