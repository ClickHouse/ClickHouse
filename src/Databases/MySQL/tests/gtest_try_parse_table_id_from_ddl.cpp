#include "config.h"

#include <gtest/gtest.h>

#include <Databases/MySQL/tryParseTableIDFromDDL.h>

using namespace DB;

struct ParseTableIDFromDDLTestCase
{
    String query;
    String database_name;
    String table_name;

    ParseTableIDFromDDLTestCase(
        const String & query_,
        const String & database_name_,
        const String & table_name_)
        : query(query_)
        , database_name(database_name_)
        , table_name(table_name_)
    {
    }
};

std::ostream & operator<<(std::ostream & ostr, const ParseTableIDFromDDLTestCase & test_case)
{
    return ostr << '"' << test_case.query << "\" extracts `" << test_case.database_name << "`.`" << test_case.table_name << "`";
}

class ParseTableIDFromDDLTest : public ::testing::TestWithParam<ParseTableIDFromDDLTestCase>
{
};

TEST_P(ParseTableIDFromDDLTest, parse)
{
    const auto & [query, expected_database_name, expected_table_name] = GetParam();
    auto table_id = tryParseTableIDFromDDL(query, "default");
    EXPECT_EQ(expected_database_name, table_id.database_name);
    EXPECT_EQ(expected_table_name, table_id.table_name);
}

INSTANTIATE_TEST_SUITE_P(MaterializedMySQL, ParseTableIDFromDDLTest, ::testing::ValuesIn(std::initializer_list<ParseTableIDFromDDLTestCase>{
    {
        "SELECT * FROM db.table",
        "",
        ""
    },
    {
        "CREATE TEMPORARY TABLE db.table",
        "db",
        "table"
    },
    {
        "CREATE TEMPORARY TABLE IF NOT EXISTS db.table",
        "db",
        "table"
    },
    {
        "CREATE TEMPORARY TABLE table",
        "default",
        "table"
    },
    {
        "CREATE TEMPORARY TABLE IF NOT EXISTS table",
        "default",
        "table"
    },
    {
        "CREATE TABLE db.table",
        "db",
        "table"
    },
    {
        "CREATE TABLE IF NOT EXISTS db.table",
        "db",
        "table"
    },
    {
        "CREATE TABLE table",
        "default",
        "table"
    },
    {
        "CREATE TABLE IF NOT EXISTS table",
        "default",
        "table"
    },
    {
        "ALTER TABLE db.table",
        "db",
        "table"
    },
    {
        "ALTER TABLE table",
        "default",
        "table"
    },
    {
        "DROP TABLE db.table",
        "db",
        "table"
    },
    {
        "DROP TABLE IF EXISTS db.table",
        "db",
        "table"
    },
    {
        "DROP TABLE table",
        "default",
        "table"
    },
    {
        "DROP TABLE IF EXISTS table",
        "default",
        "table"
    },
    {
        "DROP TEMPORARY TABLE db.table",
        "db",
        "table"
    },
    {
        "DROP TEMPORARY TABLE IF EXISTS db.table",
        "db",
        "table"
    },
    {
        "DROP TEMPORARY TABLE table",
        "default",
        "table"
    },
    {
        "DROP TEMPORARY TABLE IF EXISTS table",
        "default",
        "table"
    },
    {
        "TRUNCATE db.table",
        "db",
        "table"
    },
    {
        "TRUNCATE TABLE db.table",
        "db",
        "table"
    },
    {
        "TRUNCATE table1",
        "default",
        "table1"
    },
    {
        "TRUNCATE TABLE table",
        "default",
        "table"
    },
    {
        "RENAME TABLE db.table",
        "db",
        "table"
    },
    {
        "RENAME TABLE table",
        "default",
        "table"
    },
    {
        "DROP DATABASE db",
        "",
        ""
    },
    {
        "DROP DATA`BASE db",
        "",
        ""
    },
    {
        "NOT A SQL",
        "",
        ""
    },

}));
