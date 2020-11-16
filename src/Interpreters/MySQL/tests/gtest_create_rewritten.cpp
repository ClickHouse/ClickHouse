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
#include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Poco/String.h>


using namespace DB;

static inline ASTPtr tryRewrittenCreateQuery(const String & query, const Context & context)
{
    ParserExternalDDLQuery external_ddl_parser;
    ASTPtr ast = parseQuery(external_ddl_parser, "EXTERNAL DDL FROM MySQL(test_database, test_database) " + query, 0, 0);

    return MySQLInterpreter::InterpreterCreateImpl::getRewrittenQueries(
        *ast->as<ASTExternalDDLQuery>()->external_ddl->as<MySQLParser::ASTCreateQuery>(),
        context, "test_database", "test_database")[0];
}

TEST(MySQLCreateRewritten, ColumnsDataType)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    std::vector<std::pair<String, String>> test_types
    {
        {"TINYINT", "Int8"}, {"SMALLINT", "Int16"}, {"MEDIUMINT", "Int32"}, {"INT", "Int32"},
        {"INTEGER", "Int32"}, {"BIGINT", "Int64"}, {"FLOAT", "Float32"}, {"DOUBLE", "Float64"},
        {"VARCHAR(10)", "String"}, {"CHAR(10)", "String"}, {"Date", "Date"}, {"DateTime", "DateTime"},
        {"TIMESTAMP", "DateTime"}, {"BOOLEAN", "Int8"}
    };

    for (const auto & [test_type, mapped_type] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + ")", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(" + mapped_type + ")"
            ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " NOT NULL)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` " + mapped_type +
            ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' NOT NULL)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` " + mapped_type +
            ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
            "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

        if (Poco::toUpper(test_type).find("INT") != std::string::npos)
        {
            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(U" + mapped_type + ")"
                ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` Nullable(U" + mapped_type + ")"
                ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " NOT NULL UNSIGNED)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` U" + mapped_type +
                ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
                "ReplacingMergeTree(_version) PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

            EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
                "CREATE TABLE `test_database`.`test_table_1`(`key` INT NOT NULL PRIMARY KEY, test " + test_type + " COMMENT 'test_comment' UNSIGNED NOT NULL)", context_holder.context)),
                "CREATE TABLE test_database.test_table_1 (`key` Int32, `test` U" + mapped_type +
                ", `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
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
        {"Date", "Date", " PARTITION BY toYYYYMM(key)"}, {"DateTime", "DateTime", " PARTITION BY toYYYYMM(key)"},
        {"TIMESTAMP", "DateTime", " PARTITION BY toYYYYMM(key)"}, {"BOOLEAN", "Int8", " PARTITION BY key"}
    };

    for (const auto & [test_type, mapped_type, partition_policy] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " PRIMARY KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " NOT NULL PRIMARY KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");
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
        {"Date", "Date", " PARTITION BY toYYYYMM(key)"}, {"DateTime", "DateTime", " PARTITION BY toYYYYMM(key)"},
        {"TIMESTAMP", "DateTime", " PARTITION BY toYYYYMM(key)"}, {"BOOLEAN", "Int8", " PARTITION BY key"}
    };

    for (const auto & [test_type, mapped_type, partition_policy] : test_types)
    {
        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " PRIMARY KEY, `key2` " + test_type + " UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` Nullable(" + mapped_type + "), `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, assumeNotNull(key2))");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " NOT NULL PRIMARY KEY, `key2` " + test_type + " NOT NULL UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` " + mapped_type + ", `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, key2)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + " KEY UNIQUE KEY)", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY tuple(key)");

        EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
            "CREATE TABLE `test_database`.`test_table_1` (`key` " + test_type + ", `key2` " + test_type + " UNIQUE KEY, PRIMARY KEY(`key`, `key2`))", context_holder.context)),
            "CREATE TABLE test_database.test_table_1 (`key` " + mapped_type + ", `key2` " + mapped_type + ", `_sign` Int8() MATERIALIZED 1, "
            "`_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version)" + partition_policy + " ORDER BY (key, key2)");
    }
}

TEST(MySQLCreateRewritten, RewrittenQueryWithPrimaryKey)
{
    tryRegisterFunctions();
    const auto & context_holder = getContext();

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version) "
        "PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key` int NOT NULL, PRIMARY KEY (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key` Int32, `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = ReplacingMergeTree(_version) "
        "PARTITION BY intDiv(key, 4294967) ORDER BY tuple(key)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key_1` int NOT NULL, key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int32, `key_2` Int32, `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
        "ReplacingMergeTree(_version) PARTITION BY intDiv(key_1, 4294967) ORDER BY (key_1, key_2)");

    EXPECT_EQ(queryToString(tryRewrittenCreateQuery(
        "CREATE TABLE `test_database`.`test_table_1` (`key_1` BIGINT NOT NULL, key_2 INT NOT NULL, PRIMARY KEY (`key_1`, `key_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8", context_holder.context)),
        "CREATE TABLE test_database.test_table_1 (`key_1` Int64, `key_2` Int32, `_sign` Int8() MATERIALIZED 1, `_version` UInt64() MATERIALIZED 1) ENGINE = "
        "ReplacingMergeTree(_version) PARTITION BY intDiv(key_2, 4294967) ORDER BY (key_1, key_2)");
}

