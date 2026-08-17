#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIQueryValidation.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_register.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{

ASTPtr parse(const String & query)
{
    /// The validation distinguishes builtin functions from (server-side) user-defined ones
    /// through the factories, which the client populates on startup.
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const char * begin = query.data();
    const char * end = begin + query.size();
    ParserQuery parser(end, false, false);
    return parseQuery(parser, begin, end, "", 0, 1000, 100000);
}

bool isAllowed(const String & query)
{
    try
    {
        validateReadOnlyQueryForAIAgent(*parse(query));
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

bool isAllowedWithoutSchemaAccess(const String & query)
{
    try
    {
        validateReadOnlyQueryForAIAgent(*parse(query), /*allow_schema_access=*/ false);
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

bool isReadOnlyStatement(const String & query)
{
    return isReadOnlyStatementForAIAgent(*parse(query));
}

bool changesSettings(const String & query)
{
    return changesSettingsForAIAgent(*parse(query));
}

}

TEST(AIQueryValidation, AllowsReadOnlyStatements)
{
    EXPECT_TRUE(isAllowed("SELECT count() FROM system.tables"));
    EXPECT_FALSE(isAllowed("SELECT * FROM default.events"));
    EXPECT_FALSE(isAllowed("SELECT * FROM v"));
    EXPECT_TRUE(isAllowed("WITH 1 AS x SELECT x"));
    EXPECT_TRUE(isAllowed("SELECT 1 UNION ALL SELECT 2"));
    EXPECT_TRUE(isAllowed("SHOW TABLES FROM system"));
    EXPECT_TRUE(isAllowed("SHOW DATABASES"));
    EXPECT_TRUE(isAllowed("SHOW CREATE TABLE system.tables"));
    EXPECT_FALSE(isAllowed("SHOW CREATE TABLE default.events"));
    EXPECT_FALSE(isAllowed("EXISTS TABLE default.events"));
    EXPECT_TRUE(isAllowed("SHOW PROCESSLIST"));
    EXPECT_TRUE(isAllowed("DESCRIBE TABLE system.tables"));
    EXPECT_TRUE(isAllowed("EXPLAIN SELECT 1"));
    EXPECT_TRUE(isAllowed("EXPLAIN PIPELINE SELECT number FROM numbers(10)"));
    EXPECT_TRUE(isAllowed("EXISTS TABLE system.tables"));
    EXPECT_FALSE(isAllowed("CHECK TABLE t"));
    EXPECT_TRUE(isAllowed("SHOW GRANTS"));
}

TEST(AIQueryValidation, DisablingSchemaAccessBlocksAutonomousSchemaExploration)
{
    EXPECT_TRUE(isAllowedWithoutSchemaAccess("SELECT 1"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("DESCRIBE TABLE system.tables"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SHOW TABLES"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT name FROM system.tables"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT name FROM system.columns"));
}

TEST(AIQueryValidation, RejectsWritesAndDDL)
{
    EXPECT_FALSE(isAllowed("INSERT INTO t VALUES (1)"));
    EXPECT_FALSE(isAllowed("INSERT INTO t SELECT * FROM s"));
    EXPECT_FALSE(isAllowed("CREATE TABLE t (x UInt8) ENGINE = Memory"));
    EXPECT_FALSE(isAllowed("DROP TABLE t"));
    EXPECT_FALSE(isAllowed("TRUNCATE TABLE t"));
    EXPECT_FALSE(isAllowed("ALTER TABLE t DELETE WHERE 1"));
    EXPECT_FALSE(isAllowed("OPTIMIZE TABLE t FINAL"));
    EXPECT_FALSE(isAllowed("RENAME TABLE a TO b"));
    EXPECT_FALSE(isAllowed("SET max_threads = 1"));
    EXPECT_FALSE(isAllowed("KILL QUERY WHERE 1"));
    EXPECT_FALSE(isAllowed("USE system"));
    EXPECT_FALSE(isAllowed("GRANT SELECT ON *.* TO nobody"));
    EXPECT_FALSE(isAllowed("SYSTEM FLUSH LOGS"));
    EXPECT_FALSE(isAllowed("DELETE FROM t WHERE 1"));
}

TEST(AIQueryValidation, RejectsOutfile)
{
    EXPECT_FALSE(isAllowed("SELECT 1 INTO OUTFILE '/tmp/x'"));
}

TEST(AIQueryValidation, RejectsSandboxSettingOverrides)
{
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS readonly = 0"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS max_execution_time = 100500"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS max_memory_usage = 0"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS max_memory_usage_for_user = 0"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS max_threads = 1, readonly = 2"));

    /// `SETTINGS name = DEFAULT` resets a limit through `default_settings`, not `changes`.
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS max_execution_time = DEFAULT"));

    /// `profile` and `compatibility` expand into changes of many other settings on the server,
    /// so they could redefine the protected ones indirectly.
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS profile = 'default'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS compatibility = '20.3'"));

    /// Harmless settings are allowed.
    EXPECT_TRUE(isAllowed("SELECT 1 SETTINGS max_threads = 1"));
    EXPECT_TRUE(isAllowed("SELECT 1 SETTINGS max_threads = DEFAULT"));
}

TEST(AIQueryValidation, RejectsExternalAccess)
{
    /// `readonly = 1` only blocks writes: a SELECT over an external table function can still
    /// read files or the network, so it must go through the confirmed run_query tool.
    EXPECT_FALSE(isAllowed("SELECT * FROM file('/etc/passwd', 'LineAsString')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM url('http://127.0.0.1/', 'CSV', 'x String')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM s3('http://bucket.example.com/x', 'CSV')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM remote('127.0.0.1', system.one)"));
    EXPECT_FALSE(isAllowed("SELECT * FROM cluster('default', system.one)"));
    EXPECT_FALSE(isAllowed("SELECT * FROM executable('script.sh', 'CSV', 'x UInt8')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM mysql('127.0.0.1:3306', 'db', 't', 'user', 'password')"));

    /// The names of some of these functions are registered case-insensitively.
    EXPECT_FALSE(isAllowed("SELECT * FROM FILE('/etc/passwd', 'LineAsString')"));

    /// Nested and non-obvious positions.
    EXPECT_FALSE(isAllowed("SELECT (SELECT count() FROM file('x', 'CSV'))"));
    EXPECT_FALSE(isAllowed("SELECT 1 FROM (SELECT * FROM url('http://x', 'CSV', 'x String'))"));
    EXPECT_FALSE(isAllowed("WITH t AS (SELECT * FROM file('x', 'CSV')) SELECT * FROM t"));
    EXPECT_FALSE(isAllowed("SELECT 1 WHERE 1 IN remote('127.0.0.1', system.one)"));
    EXPECT_FALSE(isAllowed("SELECT 1 UNION ALL SELECT * FROM file('x', 'CSV')"));
    EXPECT_FALSE(isAllowed("EXPLAIN SELECT * FROM file('x', 'CSV')"));
    /// Schema inference of DESCRIBE opens the resource.
    EXPECT_FALSE(isAllowed("DESCRIBE file('x', 'CSV')"));

    /// The scalar functions reading external resources.
    EXPECT_FALSE(isAllowed("SELECT file('/etc/passwd')"));
    EXPECT_FALSE(isAllowed("SELECT catboostEvaluate('/path/model.bin', 1, 2)"));
    /// Dictionaries may fetch from HTTP, MySQL, ClickHouse, or another external source.
    EXPECT_FALSE(isAllowed("SELECT dictGet('dictionary', 'value', toUInt64(1))"));
    EXPECT_FALSE(isAllowed("SELECT dictHas('dictionary', toUInt64(1))"));
    EXPECT_FALSE(isAllowed("SELECT dictIsIn('dictionary', toUInt64(1))"));

    /// Table functions generating data locally are allowed.
    EXPECT_TRUE(isAllowed("SELECT * FROM numbers(10)"));
    EXPECT_TRUE(isAllowed("SELECT number FROM numbers_mt(10)"));
    EXPECT_TRUE(isAllowed("SELECT * FROM generateSeries(1, 10)"));
    EXPECT_TRUE(isAllowed("SELECT * FROM generateRandom('x UInt8') LIMIT 1"));
    EXPECT_TRUE(isAllowed("SELECT * FROM values('x UInt8', 1, 2)"));
    EXPECT_TRUE(isAllowed("SELECT 1 IN (1, 2, 3)"));
    EXPECT_TRUE(isAllowed("SELECT 1 IN (SELECT 1)"));
    EXPECT_TRUE(isAllowed("SELECT number IN numbers(3) FROM numbers(5)"));
}

TEST(AIQueryValidation, RejectsUnknownFunctions)
{
    /// The validation sees the raw AST before the server expands SQL user-defined functions,
    /// so a UDF like `CREATE FUNCTION read_secret AS path -> file(path)` could hide an external
    /// table function. Functions the client does not know are conservatively rejected.
    EXPECT_FALSE(isAllowed("SELECT read_secret('/etc/passwd')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM numbers(10) WHERE read_secret('x') = ''"));

    /// Builtins, including aggregate/window functions, combinators and AST-level constructs.
    EXPECT_TRUE(isAllowed("SELECT count(), sum(number), sumIf(number, number > 0) FROM numbers(10)"));
    EXPECT_TRUE(isAllowed("SELECT row_number() OVER () FROM numbers(3)"));
    EXPECT_TRUE(isAllowed("SELECT arrayMap(x -> x + 1, [1, 2])"));
    EXPECT_TRUE(isAllowed("SELECT EXISTS(SELECT 1)"));
    EXPECT_TRUE(isAllowed("SELECT number, grouping(number) FROM numbers(2) GROUP BY GROUPING SETS ((number))"));
    EXPECT_TRUE(isAllowed("SELECT untuple((1, 2))"));
    EXPECT_TRUE(isAllowed("SELECT CAST(1 AS String), toString(42), if(1, 2, 3)"));
}

TEST(AIQueryValidation, RejectsAIFunctions)
{
    /// The AI functions send the query data to an external AI provider and incur cost,
    /// which must not happen without the user's confirmation.
    EXPECT_FALSE(isAllowed("SELECT aiGenerate('ping')"));
    EXPECT_FALSE(isAllowed("SELECT aiGenerate('ping') SETTINGS allow_experimental_ai_functions = 1"));
    EXPECT_FALSE(isAllowed("SELECT aiClassify('text', ['a', 'b'])"));
    EXPECT_FALSE(isAllowed("SELECT aiExtract('text', 'names')"));
    EXPECT_FALSE(isAllowed("SELECT aiTranslate('text', 'French')"));
    EXPECT_FALSE(isAllowed("SELECT aiEmbed('text')"));
    EXPECT_FALSE(isAllowed("SELECT aiSimilarity('a', 'b')"));
    EXPECT_FALSE(isAllowed("SELECT aiFilter('text', 'filter')"));
    EXPECT_FALSE(isAllowed("SELECT aiRedact('text', 'redact')"));
    EXPECT_FALSE(isAllowed("SELECT * FROM numbers(3) WHERE aiGenerate('x') = ''"));
}

TEST(AIQueryValidation, RejectsLogCommentOverride)
{
    /// `log_comment` marks the queries of the agent in the query log, and the `read_query_log`
    /// tool filters the marked ones out. A query that redefines it would let the model make its
    /// own queries look like the user's ones.
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS log_comment = 'typed by the user'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS log_comment = DEFAULT"));
}

TEST(AIQueryValidation, RejectsFormatSchemaSettings)
{
    /// With `format_schema_source = 'query'`, `FormatSchemaInfo` executes the query from
    /// `format_schema` after this validation, and the schema-source modes write cached schema
    /// files - side effects outside of the validated AST.
    EXPECT_FALSE(isAllowed("SELECT 1 FORMAT Protobuf SETTINGS format_schema_source = 'query', format_schema = 'SELECT 1'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS format_schema = 'x.proto:Message'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS format_schema_source = 'string'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS format_schema_message_name = 'Message'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS output_format_schema = 'x.proto'"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS format_schema = DEFAULT"));
    EXPECT_FALSE(isAllowed("SELECT 1 FORMAT Template SETTINGS format_template_resultset = 'resultset.tpl'"));
    EXPECT_FALSE(isAllowed("SELECT 1 FORMAT Template SETTINGS format_template_row = 'row.tpl'"));
}

TEST(AIQueryValidation, ReadOnlyStatementTypes)
{
    /// The statement types a session with `readonly = 1` accepts. Unlike the validation of the
    /// read-only tool, this says nothing about what a read-only statement reads or writes outside
    /// of the server's tables: such a query is refused by the tool but runs in a read-only session
    /// after the user confirms it.
    EXPECT_TRUE(isReadOnlyStatement("SELECT 1"));
    EXPECT_TRUE(isReadOnlyStatement("SELECT * FROM s3('http://bucket.example.com/x', 'CSV')"));
    EXPECT_TRUE(isReadOnlyStatement("SELECT 1 INTO OUTFILE '/tmp/x'"));
    EXPECT_TRUE(isReadOnlyStatement("SELECT 1 SETTINGS max_threads = 1"));
    EXPECT_TRUE(isReadOnlyStatement("SHOW TABLES FROM system"));
    EXPECT_TRUE(isReadOnlyStatement("DESCRIBE TABLE system.tables"));
    EXPECT_TRUE(isReadOnlyStatement("EXPLAIN SELECT 1"));
    EXPECT_TRUE(isReadOnlyStatement("EXISTS TABLE system.tables"));
    EXPECT_TRUE(isReadOnlyStatement("CHECK TABLE t"));

    EXPECT_FALSE(isReadOnlyStatement("INSERT INTO t VALUES (1)"));
    EXPECT_FALSE(isReadOnlyStatement("ALTER TABLE t DELETE WHERE 1"));
    EXPECT_FALSE(isReadOnlyStatement("DROP TABLE t"));
    EXPECT_FALSE(isReadOnlyStatement("SET max_threads = 1"));
    EXPECT_FALSE(isReadOnlyStatement("SYSTEM FLUSH LOGS"));
    EXPECT_FALSE(isReadOnlyStatement("KILL QUERY WHERE 1"));
    /// `USE` changes no data, but it is not in the allowlist: the agent qualifies the table names
    /// of its queries anyway, so there is nothing to gain from admitting more statement types.
    EXPECT_FALSE(isReadOnlyStatement("USE system"));
}

TEST(AIQueryValidation, DetectsSettingChanges)
{
    /// A session with `readonly = 1` rejects a query for any setting change in it, wherever it is.
    EXPECT_TRUE(changesSettings("SET max_threads = 1"));
    EXPECT_TRUE(changesSettings("SELECT 1 SETTINGS max_threads = 1"));
    EXPECT_TRUE(changesSettings("SELECT 1 SETTINGS max_threads = DEFAULT"));
    EXPECT_TRUE(changesSettings("SELECT 1 FROM (SELECT 1 SETTINGS max_threads = 1)"));
    EXPECT_TRUE(changesSettings("WITH t AS (SELECT 1 SETTINGS max_threads = 1) SELECT * FROM t"));
    EXPECT_TRUE(changesSettings("SELECT 1 UNION ALL (SELECT 2 SETTINGS max_threads = 1)"));
    EXPECT_TRUE(changesSettings("INSERT INTO t SETTINGS async_insert = 1 VALUES (1)"));

    EXPECT_FALSE(changesSettings("SELECT 1"));
    EXPECT_FALSE(changesSettings("SELECT getSetting('max_threads')"));
    EXPECT_FALSE(changesSettings("SHOW SETTING max_threads"));
    EXPECT_FALSE(changesSettings("SELECT * FROM system.settings WHERE name = 'readonly'"));
}

#endif
