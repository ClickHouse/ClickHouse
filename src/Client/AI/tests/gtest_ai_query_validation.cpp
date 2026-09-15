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
    return isReadOnlyStatementForAISession(*parse(query));
}

bool changesSettings(const String & query)
{
    return changesSettingsForAIAgent(*parse(query));
}

}

TEST(AIQueryValidation, AllowsReadOnlyStatements)
{
    EXPECT_TRUE(isAllowed("SELECT count() FROM system.tables"));
    /// A regular-looking table name is allowed, whatever it turns out to be on the server.
    EXPECT_TRUE(isAllowed("SELECT * FROM default.events"));
    EXPECT_TRUE(isAllowed("SELECT * FROM v"));
    EXPECT_TRUE(isAllowed("WITH 1 AS x SELECT x"));
    EXPECT_TRUE(isAllowed("SELECT 1 UNION ALL SELECT 2"));
    EXPECT_TRUE(isAllowed("SHOW TABLES FROM system"));
    EXPECT_TRUE(isAllowed("SHOW DATABASES"));
    EXPECT_TRUE(isAllowed("SHOW CREATE TABLE system.tables"));
    EXPECT_TRUE(isAllowed("SHOW CREATE TABLE default.events"));
    EXPECT_TRUE(isAllowed("EXISTS TABLE default.events"));
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

    /// The schema-exploration statements are gated by `allow_schema_access` alone.
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SHOW CREATE TABLE t"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("EXISTS TABLE t"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("DESCRIBE t"));
    EXPECT_TRUE(isAllowed("SHOW CREATE TABLE t"));
    EXPECT_TRUE(isAllowed("EXISTS TABLE t"));
    EXPECT_TRUE(isAllowed("DESCRIBE t"));

    /// Reading a user table is not schema access, so it stays allowed with schema access off.
    EXPECT_TRUE(isAllowedWithoutSchemaAccess("SELECT count() FROM default.events"));
}

TEST(AIQueryValidation, RejectsExternalServerOwnedTables)
{
    /// Most `system` tables read metadata local to the server, but some reach shared external
    /// state when read: Keeper (znodes, the `mntr` command, the DDL and object-storage queues,
    /// the replication state) or the object storage (the Iceberg table metadata). Those must go
    /// through the confirmed `run_query` tool. They are judged by name, not by engine: every
    /// `system` table has an engine of its own (`SystemZooKeeper`, `SystemTables`, ...).
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper_connection"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper_info"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper_watches"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "distributed_ddl_queue"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "s3_queue_metadata"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "azure_queue_metadata"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "replicas"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "database_replicas"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "iceberg_history"));
    EXPECT_FALSE(isAllowedServerOwnedTableForAIAgent("system", "iceberg_files"));

    /// The log tables named alike stay local reads.
    EXPECT_TRUE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper_log"));
    EXPECT_TRUE(isAllowedServerOwnedTableForAIAgent("system", "zookeeper_connection_log"));
    EXPECT_TRUE(isAllowedServerOwnedTableForAIAgent("system", "tables"));

    /// The list applies to `system` only: a user table that happens to be named `replicas` is
    /// judged by its engine like any other.
    EXPECT_TRUE(isAllowedServerOwnedTableForAIAgent("default", "replicas"));

    /// `information_schema` is server-owned as well - its tables are views over `system` by
    /// design, so an engine check would reject every one of them.
    EXPECT_TRUE(isServerOwnedDatabaseForAIAgent("system"));
    EXPECT_TRUE(isServerOwnedDatabaseForAIAgent("information_schema"));
    EXPECT_TRUE(isServerOwnedDatabaseForAIAgent("INFORMATION_SCHEMA"));
    EXPECT_FALSE(isServerOwnedDatabaseForAIAgent("default"));
    EXPECT_FALSE(isServerOwnedDatabaseForAIAgent(""));
}

TEST(AIQueryValidation, AllowsOnlyLocalTableEngines)
{
    /// The whole MergeTree family, however it is prefixed.
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("MergeTree"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("ReplacingMergeTree"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("ReplicatedSummingMergeTree"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("SharedReplacingMergeTree"));
    /// The Log family and the simple engines.
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("Log"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("TinyLog"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("StripeLog"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("Memory"));
    EXPECT_TRUE(isAllowedTableEngineForAIAgent("Null"));

    /// Engines that execute a stored definition, or redirect to a table whose engine is not the
    /// one being checked.
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("View"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("MaterializedView"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("LiveView"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("Dictionary"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("Merge"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("Buffer"));
    /// Engines that read another server or an external system.
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("Distributed"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("URL"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("S3"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("File"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("MySQL"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("PostgreSQL"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("Kafka"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("IcebergS3"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent("KeeperMap"));
    EXPECT_FALSE(isAllowedTableEngineForAIAgent(""));
}

TEST(AIQueryValidation, AllowsOnlyLocalDatabaseEngines)
{
    /// A database that lists all of its tables in `system.tables` is what makes "this name is not
    /// a table" a safe conclusion rather than a guess.
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Atomic"));
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Ordinary"));
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Replicated"));
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Shared"));
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Memory"));
    EXPECT_TRUE(isAllowedDatabaseEngineForAIAgent("Overlay"));

    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("MySQL"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("PostgreSQL"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("S3"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("Iceberg"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("DataLakeCatalog"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent("Filesystem"));
    EXPECT_FALSE(isAllowedDatabaseEngineForAIAgent(""));
}

TEST(AIQueryValidation, CollectsNamedTables)
{
    auto collect = [](const String & query)
    {
        std::vector<String> names;
        for (const auto & reference : collectNamedTablesForAIAgent(*parse(query)))
            names.push_back(reference.database.empty() ? reference.table : reference.database + "." + reference.table);
        return names;
    };

    EXPECT_EQ(collect("SELECT 1"), std::vector<String>{});
    EXPECT_EQ(collect("SELECT * FROM numbers(10)"), std::vector<String>{});
    EXPECT_EQ(collect("SELECT count() FROM db.t"), std::vector<String>{"db.t"});
    /// An unqualified name keeps its database empty: only the server can resolve it.
    EXPECT_EQ(collect("SELECT count() FROM t"), std::vector<String>{"t"});
    EXPECT_EQ(collect("SELECT * FROM a JOIN db.b ON a.x = db.b.x"), (std::vector<String>{"a", "db.b"}));
    EXPECT_EQ(collect("SELECT * FROM (SELECT * FROM db.t) LIMIT 1"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("SELECT * FROM db.t UNION ALL SELECT * FROM db.u"), (std::vector<String>{"db.t", "db.u"}));
    /// Repeated names are collected once.
    EXPECT_EQ(collect("SELECT * FROM db.t WHERE x IN (SELECT x FROM db.t)"), std::vector<String>{"db.t"});
    /// A CTE name is indistinguishable from a table name here, so it is collected too; it simply
    /// does not resolve to a table when the caller looks it up.
    EXPECT_EQ(collect("WITH c AS (SELECT 1) SELECT * FROM c"), std::vector<String>{"c"});
    /// The right-hand side of IN: ambiguous between a column and a table, collected either way.
    EXPECT_EQ(collect("SELECT 1 WHERE 1 IN v"), std::vector<String>{"v"});
    EXPECT_EQ(collect("SELECT 1 WHERE 1 IN db.v"), std::vector<String>{"db.v"});
    /// Values, subqueries and table functions in that position name no table.
    EXPECT_EQ(collect("SELECT 1 WHERE 1 IN (1, 2, 3)"), std::vector<String>{});
    EXPECT_EQ(collect("SELECT 1 WHERE 1 IN numbers(3)"), std::vector<String>{});
}

TEST(AIQueryValidation, CollectsNamedTablesOfStatementTargets)
{
    /// The statements whose target is not an `ASTTableExpression`: it is either a pair of
    /// identifier children of the statement itself (`ASTQueryWithTableAndOutput`), or two plain
    /// strings the traversal cannot reach at all (`SHOW COLUMNS`, `SHOW INDEXES`). Left out, they
    /// would reach the server without their engine ever being checked.
    auto collect = [](const String & query)
    {
        std::vector<String> names;
        for (const auto & reference : collectNamedTablesForAIAgent(*parse(query)))
            names.push_back(reference.database.empty() ? reference.table : reference.database + "." + reference.table);
        return names;
    };

    EXPECT_EQ(collect("SHOW CREATE TABLE db.t"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("SHOW CREATE TABLE t"), std::vector<String>{"t"});
    EXPECT_EQ(collect("SHOW CREATE VIEW db.v"), std::vector<String>{"db.v"});
    EXPECT_EQ(collect("SHOW CREATE DICTIONARY db.d"), std::vector<String>{"db.d"});
    EXPECT_EQ(collect("EXISTS TABLE db.t"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("EXISTS VIEW db.v"), std::vector<String>{"db.v"});
    EXPECT_EQ(collect("EXISTS DICTIONARY db.d"), std::vector<String>{"db.d"});
    EXPECT_EQ(collect("SHOW COLUMNS FROM t FROM db"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("SHOW COLUMNS FROM db.t"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("SHOW INDEXES FROM t FROM db"), std::vector<String>{"db.t"});
    EXPECT_EQ(collect("SHOW INDEXES FROM db.t"), std::vector<String>{"db.t"});

    /// The database-only spellings name no table, and reading a database definition reaches
    /// nothing outside of the server.
    EXPECT_EQ(collect("EXISTS DATABASE db"), std::vector<String>{});
    EXPECT_EQ(collect("SHOW CREATE DATABASE db"), std::vector<String>{});
}

TEST(AIQueryValidation, CollectsTheDatabaseOfShowTables)
{
    /// `SHOW TABLES FROM db` names a database and no table, and it cannot be left out even though
    /// the sandbox turns the remote-visibility switches off: `InterpreterShowTablesQuery` turns
    /// them back on for the database that was asked for, so enumerating it contacts it.
    auto collect = [](const String & query)
    {
        std::vector<String> names;
        for (const auto & reference : collectNamedTablesForAIAgent(*parse(query)))
            names.push_back(reference.database + "/" + reference.table);
        return names;
    };

    EXPECT_EQ(collect("SHOW TABLES FROM db"), std::vector<String>{"db/"});
    EXPECT_EQ(collect("SHOW DICTIONARIES FROM db"), std::vector<String>{"db/"});
    /// Without a FROM the database is the current one, which only the server can resolve.
    EXPECT_EQ(collect("SHOW TABLES"), std::vector<String>{"/"});

    /// The spellings that name no database at all.
    EXPECT_EQ(collect("SHOW DATABASES"), std::vector<String>{});
    EXPECT_EQ(collect("SHOW CLUSTERS"), std::vector<String>{});
    EXPECT_EQ(collect("SHOW SETTINGS ILIKE '%max%'"), std::vector<String>{});
    EXPECT_EQ(collect("SHOW TEMPORARY TABLES"), std::vector<String>{});
}

TEST(AIQueryValidation, RejectsRemoteVisibilityOverrides)
{
    /// Enumerating a remote database or a data-lake catalog in `system.tables` contacts it, so
    /// the sandbox turns both switches off and a generated `SETTINGS` clause must not undo that.
    EXPECT_FALSE(isAllowed("SELECT count() FROM system.tables SETTINGS show_remote_databases_in_system_tables = 1"));
    EXPECT_FALSE(isAllowed("SELECT count() FROM system.tables SETTINGS show_data_lake_catalogs_in_system_tables = 1"));
    EXPECT_FALSE(isAllowed("SET show_remote_databases_in_system_tables = 1"));
    EXPECT_TRUE(isAllowed("SELECT count() FROM system.tables"));
}

TEST(AIQueryValidation, DisablingSchemaAccessBlocksInformationSchema)
{
    /// `information_schema` is views over `system` by design, so it is the same schema surface
    /// under another name: hiding only `system` would leave it readable.
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT * FROM information_schema.tables"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT column_name FROM information_schema.columns"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT * FROM INFORMATION_SCHEMA.TABLES"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT 1 WHERE 1 IN information_schema.tables"));
    /// Still allowed with schema access, like the `system` tables.
    EXPECT_TRUE(isAllowed("SELECT * FROM information_schema.tables"));
}

TEST(AIQueryValidation, AllowsReadingRegularLookingTables)
{
    /// A name in a FROM can be a view whose definition reaches an external resource, and the
    /// static validation cannot see that definition - it does not judge names at all. What the
    /// name resolves to is checked by the caller against `system.tables`; here only the external
    /// access written in the query text is rejected.
    EXPECT_TRUE(isAllowed("SELECT count() FROM db.t"));
    EXPECT_TRUE(isAllowed("SELECT * FROM t WHERE x = 1 ORDER BY y LIMIT 10"));
    EXPECT_TRUE(isAllowed("SELECT * FROM db.t FINAL"));
    EXPECT_TRUE(isAllowed("SELECT * FROM a JOIN b ON a.x = b.x"));
    EXPECT_TRUE(isAllowed("WITH c AS (SELECT * FROM db.t) SELECT * FROM c"));
    EXPECT_TRUE(isAllowed("SELECT * FROM (SELECT * FROM db.t) LIMIT 1"));
    EXPECT_TRUE(isAllowed("SELECT * FROM db.t UNION ALL SELECT * FROM db.u"));
    EXPECT_TRUE(isAllowed("EXPLAIN SELECT * FROM db.t"));

    /// Explicit external access stays rejected, also when combined with a regular table.
    EXPECT_FALSE(isAllowed("SELECT * FROM db.t WHERE x IN (SELECT * FROM url('http://x', 'CSV', 'x String'))"));
    EXPECT_FALSE(isAllowed("SELECT dictGet('d', 'v', toUInt64(x)) FROM db.t"));
    EXPECT_FALSE(isAllowed("SELECT aiGenerate(x) FROM db.t"));
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

TEST(AIQueryValidation, ChecksTableNamesOnTheRightHandSideOfIn)
{
    /// `1 IN v` reads `v` without an `ASTTableExpression` in the AST, so it is held to the same
    /// rules as `FROM v` - which now admit a regular-looking name.
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN v"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN (v)"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN db.v"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 GLOBAL IN v"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 NOT IN v"));
    /// Nested in a subquery of an allowed statement.
    EXPECT_TRUE(isAllowed("SELECT (SELECT 1 WHERE 1 IN v)"));

    /// A table function reaching outside of the server is still rejected in this position.
    EXPECT_FALSE(isAllowed("SELECT 1 WHERE 1 IN url('http://x', 'CSV', 'x String')"));

    /// A name is collected for the engine check instead of being judged here.
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN system.numbers"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN system.zookeeper"));

    /// Literals, tuples of literals, subqueries and the allowed table functions are unaffected.
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN (1, 2, 3)"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN [1, 2, 3]"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN numbers(3)"));
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN (SELECT number FROM numbers(3))"));
    /// The identifiers inside a subquery name columns, not tables.
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN (SELECT number FROM system.numbers LIMIT 1)"));
    /// Only the set expression itself can name a table: inside a tuple the elements are ordinary
    /// expressions, which the server resolves as columns, so they are left alone.
    EXPECT_TRUE(isAllowed("SELECT 1 WHERE 1 IN (toUInt8(1), toUInt8(2))"));
    EXPECT_TRUE(isAllowed("SELECT number FROM numbers(3) WHERE number IN (number, number + 1)"));

    /// A name here is ambiguous between a column and a table; both readings are now allowed.
    EXPECT_TRUE(isAllowed("SELECT 1 IN arr FROM (SELECT [1, 2] AS arr)"));

    /// The same position must not bypass the schema-access restriction either. An unqualified
    /// name is rejected there too: the session database can make it a `system` table.
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT 1 WHERE 1 IN system.tables"));
    EXPECT_FALSE(isAllowedWithoutSchemaAccess("SELECT 1 WHERE 1 IN tables"));
    EXPECT_TRUE(isAllowedWithoutSchemaAccess("SELECT 1 WHERE 1 IN db.v"));
    EXPECT_TRUE(isAllowedWithoutSchemaAccess("SELECT 1 WHERE 1 IN (1, 2, 3)"));
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

TEST(AIQueryValidation, RejectsDisplaySecretsOverride)
{
    /// The read-only sandbox masks the credentials of external-engine tables in `SHOW CREATE`,
    /// whose output goes to the AI provider unseen by the user; a generated `SETTINGS` clause
    /// must not unmask them again.
    EXPECT_FALSE(isAllowed("SHOW CREATE TABLE t SETTINGS format_display_secrets_in_show_and_select = 1"));
    EXPECT_FALSE(isAllowed("SELECT 1 SETTINGS format_display_secrets_in_show_and_select = DEFAULT"));
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
