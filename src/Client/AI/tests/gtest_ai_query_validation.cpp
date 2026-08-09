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

#endif
