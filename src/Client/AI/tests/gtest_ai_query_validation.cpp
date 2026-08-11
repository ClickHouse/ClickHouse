#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/AIQueryValidation.h>
#include <Common/Exception.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{

ASTPtr parse(const String & query)
{
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

}

TEST(AIQueryValidation, AllowsReadOnlyStatements)
{
    EXPECT_TRUE(isAllowed("SELECT count() FROM system.tables"));
    EXPECT_TRUE(isAllowed("WITH 1 AS x SELECT x"));
    EXPECT_TRUE(isAllowed("SELECT 1 UNION ALL SELECT 2"));
    EXPECT_TRUE(isAllowed("SHOW TABLES FROM system"));
    EXPECT_TRUE(isAllowed("SHOW DATABASES"));
    EXPECT_TRUE(isAllowed("SHOW CREATE TABLE system.tables"));
    EXPECT_TRUE(isAllowed("SHOW PROCESSLIST"));
    EXPECT_TRUE(isAllowed("DESCRIBE TABLE system.tables"));
    EXPECT_TRUE(isAllowed("EXPLAIN SELECT 1"));
    EXPECT_TRUE(isAllowed("EXPLAIN PIPELINE SELECT number FROM numbers(10)"));
    EXPECT_TRUE(isAllowed("EXISTS TABLE system.tables"));
    EXPECT_TRUE(isAllowed("CHECK TABLE t"));
    EXPECT_TRUE(isAllowed("SHOW GRANTS"));
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

    /// Harmless settings are allowed.
    EXPECT_TRUE(isAllowed("SELECT 1 SETTINGS max_threads = 1"));
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

#endif
