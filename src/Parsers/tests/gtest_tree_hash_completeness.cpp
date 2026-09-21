#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFromJSON.h>
#include <Parsers/ASTQueryWithOutput.h>
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

void expectJSONRejected(const String & json)
{
    EXPECT_THROW(IAST::createFromJSON(json, /*max_depth=*/ 1000, /*max_elements=*/ 100000), Exception);
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

    /// `readOutputOptionsJSON` must rebuild the output-option children in the canonical order of
    /// `output_option_members` - `COMPRESSION` and `LEVEL` come before `FORMAT` and `SETTINGS` -
    /// or the restored AST hashes differently than the parsed one.
    for (const std::string query : {
             "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz'",
             "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' LEVEL 5",
             "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' LEVEL 5 FORMAT JSONEachRow",
             "SELECT 1 INTO OUTFILE 'x' TRUNCATE COMPRESSION 'gz' FORMAT JSONEachRow SETTINGS max_threads = 1",
         })
        EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query)) << query;
}

TEST(TreeHashCompleteness, ResetOutputASTClearsTheFlags)
{
    /// `resetOutputASTIfExist` is used to normalize an AST before hashing (e.g. the query result
    /// cache strips the output options so they do not affect the cache key); now that the
    /// `APPEND` / `TRUNCATE` / `AND STDOUT` flags are part of the hash, it must clear them too.
    const auto stripped_hash = [](const std::string & query)
    {
        ASTPtr ast = parse(query);
        EXPECT_TRUE(ASTQueryWithOutput::resetOutputASTIfExist(*ast)) << query;
        return ast->getTreeHash(/*ignore_aliases=*/ false);
    };

    const auto plain = hashOf("SELECT 1");
    EXPECT_EQ(stripped_hash("SELECT 1 INTO OUTFILE 'x'"), plain);
    EXPECT_EQ(stripped_hash("SELECT 1 INTO OUTFILE 'x' APPEND"), plain);
    EXPECT_EQ(stripped_hash("SELECT 1 INTO OUTFILE 'x' TRUNCATE"), plain);
    EXPECT_EQ(stripped_hash("SELECT 1 INTO OUTFILE 'x' AND STDOUT COMPRESSION 'gz' LEVEL 5"), plain);
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
        "OPTIMIZE TABLE db.t PARTITION 1 FINAL DEDUPLICATE BY a, b",
        "ALTER TABLE db.t ADD COLUMN x UInt64 FORMAT JSONEachRow",
        "DROP TABLE db.t",
        "DROP TABLE t1, t2",
        "UNDROP TABLE db.t",
        "EXISTS TABLE db.t INTO OUTFILE 'x'",
        "DESCRIBE TABLE db.t FORMAT JSONEachRow",
        "CREATE TABLE db.t (a UInt64) ENGINE = MergeTree ORDER BY a",
        "CREATE TABLE t (a UInt64) ENGINE = Memory COMMENT 'c' AS SELECT 1",
        "CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR COMMENT 'c' AS SELECT 1",
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
        /// Without an explicit ENGINE the parser synthesizes the storage definition for the moved
        /// PRIMARY KEY; it must land in `children` before the comment, where a fresh parse of the
        /// formatted query (which spells the storage-level PRIMARY KEY) puts it. Found by the AST
        /// fuzzer.
        "CREATE TEMPORARY TABLE t (a UInt64 NOT NULL PRIMARY KEY) COMMENT 'c'",
        "CREATE TABLE t (a UInt64, PRIMARY KEY a) COMMENT 'c'",
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

TEST(TreeHashCompleteness, MemberOnlyClausesAreSignificant)
{
    /// `ASTCheckTableQuery::partition` / `part_name` and `ASTOptimizeQuery::deduplicate_by_columns`
    /// are kept outside `children`,
    /// so `CloneHashesEqual` above would pass for them even if a clone dropped them.
    EXPECT_NE(hashOf("CHECK TABLE t"), hashOf("CHECK TABLE t PARTITION 1"));
    EXPECT_NE(hashOf("CHECK TABLE t PARTITION 1"), hashOf("CHECK TABLE t PARTITION 2"));
    EXPECT_NE(hashOf("CHECK TABLE t"), hashOf("CHECK TABLE t PART 'all_1_1_0'"));
    EXPECT_NE(hashOf("CHECK TABLE t PART 'all_1_1_0'"), hashOf("CHECK TABLE t PART 'all_2_2_0'"));

    EXPECT_NE(hashOf("OPTIMIZE TABLE t DEDUPLICATE"), hashOf("OPTIMIZE TABLE t DEDUPLICATE BY a"));
    EXPECT_NE(hashOf("OPTIMIZE TABLE t DEDUPLICATE BY a"), hashOf("OPTIMIZE TABLE t DEDUPLICATE BY b"));

    for (const std::string query : {
             "CHECK TABLE t PARTITION 1",
             "OPTIMIZE TABLE t PARTITION 1",
             "OPTIMIZE TABLE t DRY RUN PARTS 'p1'",
             "OPTIMIZE TABLE t DEDUPLICATE BY a",
         })
        EXPECT_EQ(hashOfJSONRoundTrip(query), hashOf(query)) << query;

    /// Every one of them is printed, so it survives a format+parse round trip and a clone.
    for (const std::string query : {
             "CHECK TABLE t PARTITION 1",
             "CHECK TABLE t PART 'all_1_1_0'",
             "OPTIMIZE TABLE t DEDUPLICATE BY a, b",
         })
    {
        ASTPtr ast = parse(query);
        const auto hash = ast->getTreeHash(/*ignore_aliases=*/ false);
        EXPECT_EQ(ast->clone()->getTreeHash(/*ignore_aliases=*/ false), hash) << query;
        EXPECT_EQ(hashOf(ast->formatWithSecretsOneLine()), hash) << query;
    }
}

TEST(TreeHashCompleteness, CreateDropAndShowMembersAreSignificant)
{
    EXPECT_NE(hashOf("CREATE TABLE t (a UInt8) ENGINE = Memory"),
              hashOf("CREATE TABLE IF NOT EXISTS t (a UInt8) ENGINE = Memory"));
    EXPECT_NE(hashOf("CREATE TABLE dst AS db1.src"), hashOf("CREATE TABLE dst AS db2.src"));
    EXPECT_NE(hashOf("CREATE TABLE dst AS a.bc"), hashOf("CREATE TABLE dst AS ab.c"));
    EXPECT_NE(hashOf("CREATE VIEW v AS SELECT 1"), hashOf("CREATE OR REPLACE VIEW v AS SELECT 1"));

    EXPECT_NE(hashOf("DROP TABLE t"), hashOf("DROP VIEW t"));
    EXPECT_NE(hashOf("DETACH TABLE t"), hashOf("DETACH TABLE t PERMANENTLY"));
    EXPECT_NE(hashOf("TRUNCATE TABLES FROM db LIKE 'x%'"), hashOf("TRUNCATE TABLES FROM db LIKE 'y%'"));
    EXPECT_NE(hashOf("TRUNCATE TABLES FROM db"), hashOf("TRUNCATE TABLES FROM db LIKE ''"));
    EXPECT_NE(hashOf("TRUNCATE TABLES FROM db LIKE ''"), hashOf("TRUNCATE TABLES FROM db NOT LIKE ''"));
    EXPECT_NE(hashOf("TRUNCATE TABLES FROM db LIKE ''"), hashOf("TRUNCATE TABLES FROM db ILIKE ''"));

    std::string like_with_flag_byte = "TRUNCATE TABLES FROM db LIKE 'x";
    like_with_flag_byte += '\x01';
    like_with_flag_byte += "'";
    EXPECT_NE(hashOf(like_with_flag_byte), hashOf("TRUNCATE TABLES FROM db NOT LIKE 'x'"));

    EXPECT_NE(hashOf("SHOW COLUMNS FROM t"), hashOf("SHOW COLUMNS FROM u"));
    EXPECT_NE(hashOf("SHOW COLUMNS FROM t"), hashOf("SHOW COLUMNS FROM t LIMIT 1"));
    EXPECT_NE(hashOf("SHOW COLUMNS FROM t"), hashOf("SHOW COLUMNS FROM t LIKE ''"));
    EXPECT_NE(hashOf("SHOW COLUMNS FROM t LIKE ''"), hashOf("SHOW COLUMNS FROM t ILIKE ''"));
    EXPECT_NE(hashOf("SHOW COLUMNS FROM bc FROM a"), hashOf("SHOW COLUMNS FROM c FROM ab"));
    EXPECT_NE(hashOf("SHOW INDEXES FROM bc FROM a"), hashOf("SHOW INDEXES FROM c FROM ab"));
    EXPECT_NE(hashOf("SHOW INDEXES FROM t"), hashOf("SHOW COLUMNS FROM t"));
    EXPECT_EQ(hashOfJSONRoundTrip("SHOW COLUMNS FROM t LIMIT 1"), hashOf("SHOW COLUMNS FROM t LIMIT 1"));
    EXPECT_EQ(hashOfJSONRoundTrip("SHOW INDEXES FROM db.t WHERE name = 'i'"),
              hashOf("SHOW INDEXES FROM db.t WHERE name = 'i'"));
    EXPECT_EQ(hashOfJSONRoundTrip("TRUNCATE TABLES FROM db LIKE ''"), hashOf("TRUNCATE TABLES FROM db LIKE ''"));
    EXPECT_EQ(hashOfJSONRoundTrip("SHOW COLUMNS FROM t LIKE ''"), hashOf("SHOW COLUMNS FROM t LIKE ''"));

    for (const std::string query : {
             "CREATE OR REPLACE VIEW v AS SELECT 1",
             "DETACH TABLE t PERMANENTLY",
             "SHOW COLUMNS FROM t LIMIT 1",
             "TRUNCATE TABLES FROM db NOT LIKE ''",
             "SHOW COLUMNS FROM t ILIKE ''",
             "SHOW INDEXES FROM t WHERE name = 'i'",
         })
    {
        ASTPtr ast = parse(query);
        const auto hash = ast->getTreeHash(/*ignore_aliases=*/ false);
        EXPECT_EQ(ast->clone()->getTreeHash(/*ignore_aliases=*/ false), hash) << query;
        EXPECT_EQ(hashOf(ast->formatWithSecretsOneLine()), hash) << query;
    }
}

TEST(TreeHashCompleteness, OptimizeClusterIsSignificant)
{
    EXPECT_NE(hashOf("OPTIMIZE TABLE t ON CLUSTER c1"), hashOf("OPTIMIZE TABLE t ON CLUSTER c2"));
}

TEST(TreeHashCompleteness, AlterAndUndropMembersAreSignificant)
{
    /// The root cluster and command flags / source names are member-only, while each command's
    /// expressions are children.
    EXPECT_NE(hashOf("ALTER TABLE t ON CLUSTER c1 ADD COLUMN x UInt64"),
              hashOf("ALTER TABLE t ON CLUSTER c2 ADD COLUMN x UInt64"));
    EXPECT_NE(hashOf("ALTER TABLE t ADD COLUMN x UInt64"),
              hashOf("ALTER TABLE t ADD COLUMN x UInt64 FIRST"));
    EXPECT_NE(hashOf("ALTER TABLE t DROP PARTITION 1"),
              hashOf("ALTER TABLE t DETACH PARTITION 1"));
    EXPECT_NE(hashOf("ALTER TABLE t REPLACE PARTITION 1 FROM db1.src"),
              hashOf("ALTER TABLE t REPLACE PARTITION 1 FROM db2.src"));

    const std::string materialize_statistics = "ALTER TABLE tab MATERIALIZE STATISTICS no_such_column";
    EXPECT_EQ(hashOf(materialize_statistics), hashOf(parse(materialize_statistics)->formatWithSecretsOneLine()));

    EXPECT_NE(hashOf("UNDROP TABLE t ON CLUSTER c1"),
              hashOf("UNDROP TABLE t ON CLUSTER c2"));
}

TEST(TreeHashCompleteness, AlterSnapshotDescriptionCloneHashEqual)
{
    const std::string query = "ALTER TABLE t UNLOCK SNAPSHOT 'snapshot' FROM S3('https://example.com/backup')";
    ASTPtr ast = parse(query);
    ASTPtr cloned = ast->clone();

    EXPECT_EQ(cloned->getTreeHash(/*ignore_aliases=*/ false), ast->getTreeHash(/*ignore_aliases=*/ false));
    EXPECT_EQ(cloned->formatWithSecretsOneLine(), ast->formatWithSecretsOneLine());
}

TEST(TreeHashCompleteness, JSONRejectsInternalAndHiddenExecutionState)
{
    {
        String json = serializeASTToJSON(*parse("CREATE TABLE t (x UInt8) ENGINE = Memory"));
        EXPECT_EQ(json.find("\"attach_short_syntax\""), String::npos);
        const String key = R"("type":"CreateQuery",)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.insert(pos + key.size(), R"("attach_short_syntax":false,)");
        expectJSONRejected(json);
    }

    {
        String json = serializeASTToJSON(*parse("DROP TABLE t"));
        EXPECT_EQ(json.find("\"no_ddl_lock\""), String::npos);
        const String key = R"("type":"DropQuery",)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.insert(pos + key.size(), R"("no_ddl_lock":false,)");
        expectJSONRejected(json);
    }

    {
        String json = serializeASTToJSON(*parse("SELECT 1 ORDER BY 1"));
        const String key = R"("nulls_direction":1)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("nulls_direction":-1)");
        expectJSONRejected(json);
    }

    const String query = "SELECT sum(n) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t";
    {
        String json = serializeASTToJSON(*parse(query));
        const String key = R"("frame_begin_preceding":true)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("frame_begin_preceding":false)");
        expectJSONRejected(json);
    }

    {
        String json = serializeASTToJSON(*parse(query));
        const String key = R"("frame_end_preceding":false)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("frame_end_preceding":true)");
        expectJSONRejected(json);
    }
}

TEST(TreeHashCompleteness, ViewsRejectAPrimaryKeyTheyCannotFormat)
{
    /// A plain view has no storage definition, and a materialized view with `TO [db].[table]` must
    /// not have one, so a PRIMARY KEY in the column list used to be normalized into a synthesized
    /// storage definition that formatting printed as a table-level PRIMARY KEY - which no longer
    /// parsed back (and would have made the view unloadable from its metadata). Found by the AST
    /// fuzzer.
    for (const std::string query : {
             "CREATE VIEW v (a UInt8 PRIMARY KEY) AS SELECT 1 AS a",
             "CREATE VIEW v (a UInt8, PRIMARY KEY a) AS SELECT 1 AS a",
             "CREATE MATERIALIZED VIEW v TO t (a UInt8 PRIMARY KEY) AS SELECT 1 AS a",
             "CREATE MATERIALIZED VIEW v TO t (a UInt8, PRIMARY KEY a) AS SELECT 1 AS a",
         })
        EXPECT_THROW(parse(query), Exception) << query;

    /// A materialized view with an inner table keeps it: there the storage definition is its own.
    for (const std::string query : {
             "CREATE MATERIALIZED VIEW v (a UInt8 PRIMARY KEY) ENGINE = MergeTree AS SELECT 1 AS a",
             "CREATE MATERIALIZED VIEW v (a UInt8 PRIMARY KEY) AS SELECT 1 AS a",
         })
    {
        ASTPtr ast = parse(query);
        EXPECT_EQ(hashOf(ast->formatWithSecretsOneLine()), ast->getTreeHash(/*ignore_aliases=*/ false)) << query;
    }
}

TEST(TreeHashCompleteness, TableFunctionsRejectAPrimaryKeyTheyCannotFormat)
{
    /// A table created from a table function has no storage definition of its own, so a PRIMARY KEY
    /// in the column list used to be normalized into a synthesized storage definition that
    /// formatting printed after the table function - a position that no longer parsed back (and
    /// would have made the table unloadable from its metadata). Found by the AST fuzzer.
    for (const std::string query : {
             "CREATE TABLE t (a UInt8 PRIMARY KEY) AS numbers(5)",
             "CREATE TABLE t (a UInt8, PRIMARY KEY a) AS numbers(5)",
             "ATTACH TABLE t (a UInt8 PRIMARY KEY) AS numbers(5)",
         })
        EXPECT_THROW(parse(query), Exception) << query;

    /// Without a PRIMARY KEY the table function keeps its column list; with an explicit ENGINE the
    /// storage definition is the table's own.
    for (const std::string query : {
             "CREATE TABLE t (a UInt8) AS numbers(5)",
             "CREATE TABLE t (a UInt8 PRIMARY KEY) ENGINE = MergeTree",
         })
    {
        ASTPtr ast = parse(query);
        EXPECT_EQ(hashOf(ast->formatWithSecretsOneLine()), ast->getTreeHash(/*ignore_aliases=*/ false)) << query;
    }
}

TEST(TreeHashCompleteness, ExplicitNilUuidClausesAreRejected)
{
    /// Both clauses used to retain presence state that formatting could not represent. Reject them
    /// at parsing instead of allowing a tree that changes meaning when formatted and reparsed.
    EXPECT_THROW(parse("ATTACH TABLE t UUID '00000000-0000-0000-0000-000000000000'"), Exception);
    EXPECT_THROW(
        parse("CREATE TABLE t TO INNER UUID '00000000-0000-0000-0000-000000000000' "
              "(x UInt32) ENGINE = SharedSet('/z', 'r')"),
        Exception);

    String json = serializeASTToJSON(*parse("ATTACH TABLE t"));
    const String key = R"("has_uuid_clause":false)";
    const auto pos = json.find(key);
    ASSERT_NE(pos, String::npos);
    json.replace(pos, key.size(), R"("has_uuid_clause":true)");
    expectJSONRejected(json);
}

TEST(TreeHashCompleteness, ShortAttachRejectsAToInnerUuidItCannotFormat)
{
    /// The short `ATTACH` form has nowhere to keep the parsed inner UUID: it builds no `targets`,
    /// which is what formatting prints the clause from, so only the presence flag would survive and
    /// the clause would disappear on format. Reject it at parsing instead.
    EXPECT_THROW(parse("ATTACH TABLE t TO INNER UUID '00000000-0000-0000-0000-000000000001'"), Exception);
    EXPECT_THROW(parse("ATTACH TABLE t TO INNER UUID '00000000-0000-0000-0000-000000000000'"), Exception);

    /// The long form keeps it in `targets` and formats it back.
    const std::string query = "ATTACH TABLE t TO INNER UUID '00000000-0000-0000-0000-000000000001' "
                              "(x UInt32) ENGINE = SharedSet('/z', 'r')";
    ASTPtr ast = parse(query);
    EXPECT_TRUE(ast->formatWithSecretsOneLine().contains("TO INNER UUID"));
    EXPECT_EQ(hashOf(ast->formatWithSecretsOneLine()), ast->getTreeHash(/*ignore_aliases=*/ false));
}

TEST(TreeHashCompleteness, CreateQueryUpdatesEveryDirectChildPointer)
{
    /// `forEachPointerToChild` must name every member that also lives in `children`. A visitor that
    /// replaces a child - `ReplaceQueryParameterVisitor` is the one in the server - swaps the entry
    /// in `children` and then asks the node to update its member pointer; a member missing here
    /// keeps pointing at the node that was just released.
    const std::string query =
        "CREATE MATERIALIZED VIEW v ENGINE = Memory DEFINER = CURRENT_USER SQL SECURITY DEFINER "
        "COMMENT 'c' AS SELECT count(a) FROM t";
    ASTPtr ast = parse(query);
    auto & create = ast->as<ASTCreateQuery &>();
    ASSERT_TRUE(create.sql_security);
    ASSERT_TRUE(create.comment);

    for (IAST ** member : {&create.sql_security, &create.comment})
    {
        ASTPtr replacement = (*member)->clone();
        ast->updatePointerToChild(*member, replacement);
        EXPECT_EQ(*member, replacement.get());
    }
}

TEST(TreeHashCompleteness, JSONRejectsPopulateAndEmptyTheParserCannotProduce)
{
    /// `POPULATE` / `EMPTY` decide whether the initial `INSERT SELECT` runs, and formatting prints
    /// them unconditionally, so a JSON payload must not be able to reach a combination that the SQL
    /// parser rejects: it would both format into unparsable SQL and change what execution does.
    const auto reject_with_flag = [](const std::string & query, const String & flag)
    {
        String json = serializeASTToJSON(*parse(query));
        const String key = "\"" + flag + "\":false";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos) << query;
        json.replace(pos, key.size(), "\"" + flag + "\":true");
        expectJSONRejected(json);
    };

    /// A plain table and an ordinary view never carry `POPULATE`.
    reject_with_flag("CREATE TABLE t ENGINE = Memory EMPTY AS SELECT 1 AS x", "is_populate");
    reject_with_flag("CREATE VIEW v AS SELECT 1", "is_populate");
    /// An ordinary view never carries `EMPTY` either.
    reject_with_flag("CREATE VIEW v AS SELECT 1", "is_create_empty");
    /// The first refresh of a refreshable materialized view already fills it.
    reject_with_flag("CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR TO t AS SELECT 1", "is_populate");
    /// With an external target and no refresh strategy there is no initial load for `EMPTY` to skip.
    reject_with_flag("CREATE MATERIALIZED VIEW v TO t AS SELECT 1", "is_create_empty");

    /// The two are mutually exclusive.
    {
        String json = serializeASTToJSON(*parse("CREATE MATERIALIZED VIEW v ENGINE = Memory POPULATE AS SELECT 1"));
        const String key = R"("is_create_empty":false)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("is_create_empty":true)");
        expectJSONRejected(json);
    }

    /// Nothing to fill from: formatting would emit a trailing `EMPTY` that cannot be parsed back.
    {
        String json = serializeASTToJSON(*parse("CREATE TABLE t (x UInt8) ENGINE = Memory"));
        const String key = R"("is_create_empty":false)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("is_create_empty":true)");
        expectJSONRejected(json);
    }
}

TEST(TreeHashCompleteness, JSONRejectsColumnPrimaryKeyMultiTargetDetachAndOrphanedWatermark)
{
    /// A column-level PRIMARY KEY never survives into a final CREATE column list: the parser
    /// normalizes it into the storage definition (or rejects it for view shapes) and clears the
    /// flag. Execution ignores the flag while formatting prints it, so a JSON payload carrying it
    /// would execute one definition and persist another.
    {
        String json = serializeASTToJSON(*parse("CREATE TABLE t (a UInt8) ENGINE = Memory"));
        const String key = R"("primary_key_specifier":false)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("primary_key_specifier":true)");
        expectJSONRejected(json);
    }

    /// The SQL parser allows a multi-entry target list only for DROP, but the interpreter would
    /// execute each entry, so JSON must not express a multi-table DETACH / TRUNCATE.
    for (const String kind : {"Detach", "Truncate"})
    {
        String json = serializeASTToJSON(*parse("DROP TABLE t1, t2"));
        const String key = R"("kind":"Drop")";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.replace(pos, key.size(), R"("kind":")" + kind + R"(")");
        expectJSONRejected(json);
    }

    /// The single-target list form stays accepted for every kind.
    EXPECT_EQ(hashOfJSONRoundTrip("DETACH TABLE t"), hashOf("DETACH TABLE t"));
    EXPECT_EQ(hashOfJSONRoundTrip("TRUNCATE TABLE t"), hashOf("TRUNCATE TABLE t"));
    EXPECT_EQ(hashOfJSONRoundTrip("DROP TABLE t1, t2"), hashOf("DROP TABLE t1, t2"));

    /// A table created from a table function has no storage definition of its own, so the parser
    /// accepts only one of the two clauses. A payload carrying both formats to a definition that
    /// no longer parses back, which the metadata written for such a table could not be loaded from.
    {
        String json = serializeASTToJSON(*parse("CREATE TABLE t (a UInt8) AS numbers(5)"));
        const String key = R"("as_table_function":)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.insert(pos, R"("storage":{"type":"Storage","engine":{"type":"Function","name":"MergeTree","no_empty_args":true,"kind":"TABLE_ENGINE"}},)");
        expectJSONRejected(json);
    }

    /// Either clause on its own stays accepted.
    EXPECT_EQ(hashOfJSONRoundTrip("CREATE TABLE t (a UInt8) AS numbers(5)"), hashOf("CREATE TABLE t (a UInt8) AS numbers(5)"));
    EXPECT_EQ(hashOfJSONRoundTrip("CREATE TABLE t (a UInt8) ENGINE = MergeTree"), hashOf("CREATE TABLE t (a UInt8) ENGINE = MergeTree"));

    /// The parser produces the watermark fields only together with the column; orphaned fields
    /// would previously be dropped silently, hashing and executing as an unwatermarked stream.
    {
        String json = serializeASTToJSON(*parse("SELECT * FROM t STREAM WATERMARK FOR a AS a - 1 IDLE TIMEOUT INTERVAL 5 SECOND"));
        const String key = R"("watermark_column":"a",)";
        const auto pos = json.find(key);
        ASSERT_NE(pos, String::npos);
        json.erase(pos, key.size());
        expectJSONRejected(json);
    }
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
