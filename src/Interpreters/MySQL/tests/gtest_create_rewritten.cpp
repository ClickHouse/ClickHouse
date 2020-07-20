#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#include <gtest/gtest.h>

#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>

using namespace DB;

static inline ASTPtr tryRewrittenCreateQuery(const String & query, Context & context)
{
    ParserExternalDDLQuery external_ddl_parser;
    ASTPtr ast = parseQuery(external_ddl_parser, query, 0, 0);

    context.unsafeSetCurrentDatabase("default");
    return MySQLInterpreter::InterpreterCreateImpl::getRewrittenQuery(
        *ast->as<ASTExternalDDLQuery>()->external_ddl->as<MySQLParser::ASTCreateQuery>(),
        context, "test_database", "test_database");
}

TEST(MySQLCreateRewritten, RewrittenQueryWithPrimaryKey)
{
    registerFunctions();
    auto shared_context = Context::createShared();
    auto global_context = std::make_unique<Context>(Context::createGlobal(shared_context.get()));

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "EXTERNAL DDL FROM MySQL(test_database, test_database) CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL PRIMARY "
        "KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8", *global_context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `_sign` Int8, `_version` UInt64) ENGINE = ReplacingMergeTree(_version) "
        "PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "EXTERNAL DDL FROM MySQL(test_database, test_database) CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL, "
        " PRIMARY KEY (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", *global_context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `_sign` Int8, `_version` UInt64) ENGINE = ReplacingMergeTree(_version) "
        "PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "EXTERNAL DDL FROM MySQL(test_database, test_database) CREATE TABLE `test_database`.`test_table_1` (`key_1` int NOT NULL, "
        " key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", *global_context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int32, `key_2` Int32, `_sign` Int8, `_version` UInt64) ENGINE = "
        "ReplacingMergeTree(_version) PARTITION BY intDiv(key_1, 4294967) ORDER BY (key_1, key_2)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "EXTERNAL DDL FROM MySQL(test_database, test_database) CREATE TABLE `test_database`.`test_table_1` (`key_1` BIGINT NOT NULL, "
        " key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", *global_context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int64, `key_2` Int32, `_sign` Int8, `_version` UInt64) ENGINE = "
        "ReplacingMergeTree(_version) PARTITION BY intDiv(key_2, 4294967) ORDER BY (key_1, key_2)");
}

