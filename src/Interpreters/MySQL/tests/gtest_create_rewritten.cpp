#include "config_core.h"

#include <gtest/gtest.h>

#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Poco/String.h>

#if USE_MYSQL
using namespace DB;

static inline ASTPtr tryRewrittenCreateQuery(const String & query, ContextPtr context)
{
    ParserExternalDDLQuery external_ddl_parser;
    ASTPtr ast = parseQuery(external_ddl_parser, "EXTERNAL DDL FROM MySQL(test_database, test_database) " + query, 0, 0);

    return MySQLInterpreter::InterpreterCreateImpl::getRewrittenQueries(
        *ast->as<ASTExternalDDLQuery>()->external_ddl->as<MySQLParser::ASTCreateQuery>(),
        context, "test_database", "test_database")[0];
}

static const char MATERIALIZEDMYSQL_TABLE_COLUMNS[] = ", `_sign` Int8() MATERIALIZED 1"
                                                     ", `_version` UInt64() MATERIALIZED 1"
                                                     ", INDEX _version _version TYPE minmax GRANULARITY 1";

TEST(MySQLCreateRewritten, ColumnsDataType)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    std::vector<std::pair<String, String>> test_types
    {
        {"TINYINT", "Int8"}, {"SMALLINT", "Int16"}, {"MEDIUMINT", "Int32"}, {"INT", "Int32"},
        {"INTEGER", "Int32"}, {"BIGINT", "Int64"}, {"FLOAT", "Float32"}, {"DOUBLE", "Float64"},
        {"VARCHAR(10)", "String"}, {"CHAR(10)", "String"}, {"Date", "Date32"}, {"DateTime", "DateTime"},
        {"TIMESTAMP", "DateTime"}, {"BOOLEAN", "Bool"}, {"BIT", "UInt64"}, {"SET", "UInt64"},
        {"YEAR", "UInt16"}, {"TIME", "Int64"}, {"GEOMETRY", "String"}
    };

    for (const auto & [test_type, mapped_type] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + ")", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(" + mapped_type + ")" +
            MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " NOT NULL)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' NOT NULL)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` " + mapped_type + " COMMENT 'test_comment'" +
            MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        if (Poco::toUpper(test_type).find("INT") != std::string::npos)
        {
            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(U" + mapped_type + ")" +
                MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(U" + mapped_type + ")" + " COMMENT 'test_comment'" +
                MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " NOT NULL UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` U" + mapped_type +
                MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' UNSIGNED NOT NULL)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` U" + mapped_type + " COMMENT 'test_comment'" +
                MATERIALIZEDMYSQL_TABLE_COLUMNS + ") ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");
        }
    }
}

TEST(MySQLCreateRewritten, PartitionPolicy)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    std::vector<std::tuple<String, String, String>> test_types
    {
        {"TINYINT", "Int8", " PARTITION BY key"}, {"SMALLINT", "Int16", " PARTITION BY intDiv(key, 65)"},
        {"MEDIUMINT", "Int32", " PARTITION BY intDiv(key, 4294967)"}, {"INT", "Int32", " PARTITION BY intDiv(key, 4294967)"},
        {"INTEGER", "Int32", " PARTITION BY intDiv(key, 4294967)"}, {"BIGINT", "Int64", " PARTITION BY intDiv(key, 18446744073709551)"},
        {"FLOAT", "Float32", ""}, {"DOUBLE", "Float64", ""}, {"VARCHAR(10)", "String", ""}, {"CHAR(10)", "String", ""},
        {"Date", "Date32", " PARTITION BY toYYYYMM(key)"}, {"DateTime", "DateTime", " PARTITION BY toYYYYMM(key)"},
        {"TIMESTAMP", "DateTime", " PARTITION BY toYYYYMM(key)"}, {"BOOLEAN", "Bool", " PARTITION BY key"}
    };

    for (const auto & [test_type, mapped_type, partition_policy] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " PRIMARY KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " NOT NULL PRIMARY KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");
    }
}

TEST(MySQLCreateRewritten, OrderbyPolicy)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    std::vector<std::tuple<String, String, String>> test_types
    {
        {"TINYINT", "Int8", " PARTITION BY key"}, {"SMALLINT", "Int16", " PARTITION BY intDiv(key, 65)"},
        {"MEDIUMINT", "Int32", " PARTITION BY intDiv(key, 4294967)"}, {"INT", "Int32", " PARTITION BY intDiv(key, 4294967)"},
        {"INTEGER", "Int32", " PARTITION BY intDiv(key, 4294967)"}, {"BIGINT", "Int64", " PARTITION BY intDiv(key, 18446744073709551)"},
        {"FLOAT", "Float32", ""}, {"DOUBLE", "Float64", ""}, {"VARCHAR(10)", "String", ""}, {"CHAR(10)", "String", ""},
        {"Date", "Date32", " PARTITION BY toYYYYMM(key)"}, {"DateTime", "DateTime", " PARTITION BY toYYYYMM(key)"},
        {"TIMESTAMP", "DateTime", " PARTITION BY toYYYYMM(key)"}, {"BOOLEAN", "Bool", " PARTITION BY key"}
    };

    for (const auto & [test_type, mapped_type, partition_policy] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " PRIMARY KEY, `key2` " + test_type + " UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` Nullable(" + mapped_type + ")" +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, assumeNotNull(key2))");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " NOT NULL PRIMARY KEY, `key2` " + test_type + " NOT NULL UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, key2)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " KEY UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + ", `key2` " + test_type + " UNIQUE KEY, PRIMARY KEY(`key`, `key2`))", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` " + mapped_type +
            MATERIALIZEDMYSQL_TABLE_COLUMNS +
            ") ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, key2)");
    }
}

TEST(MySQLCreateRewritten, RewrittenQueryWithPrimaryKey)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL, PRIMARY KEY (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key_1` int NOT NULL, key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int32, `key_2` Int32" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key_1, 4294967) ORDER BY (key_1, key_2)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key_1` BIGINT NOT NULL, key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int64, `key_2` Int32" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key_2, 4294967) ORDER BY (key_1, key_2)");
}

TEST(MySQLCreateRewritten, RewrittenQueryWithPrefixKey)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL PRIMARY KEY, `prefix_key` varchar(200) NOT NULL, KEY prefix_key_index(prefix_key(2))) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `prefix_key` String" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) + ") ENGINE = "
        "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY (key, prefix_key)");
}

TEST(MySQLCreateRewritten, UniqueKeysConvert)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (code varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,name varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,"
        " id bigint NOT NULL AUTO_INCREMENT, tenant_id bigint NOT NULL, PRIMARY KEY (id), UNIQUE KEY code_id (code, tenant_id), UNIQUE KEY name_id (name, tenant_id))"
        " ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`code` String, `name` String, `id` Int64, `tenant_id` Int64" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(id, 18446744073709551) ORDER BY (code, name, tenant_id, id)");
}

TEST(MySQLCreateRewritten, QueryWithColumnComments)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, `test` INT COMMENT 'test_comment')", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(Int32) COMMENT 'test_comment'" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");
}

TEST(MySQLCreateRewritten, QueryWithEnum)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, `test` ENUM('a','b','c'))", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(Enum8('a' = 1, 'b' = 2, 'c' = 3))" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");
    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, `test` ENUM('a','b','c') NOT NULL)", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Enum8('a' = 1, 'b' = 2, 'c' = 3)" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");
    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, `test` ENUM('a','b','c') COMMENT 'test_comment')", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(Enum8('a' = 1, 'b' = 2, 'c' = 3)) COMMENT 'test_comment'" +
        std::string(MATERIALIZEDMYSQL_TABLE_COLUMNS) +
        ") ENGINE = ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");
}
#endif
