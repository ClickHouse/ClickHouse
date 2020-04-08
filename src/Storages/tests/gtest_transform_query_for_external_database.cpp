#include <gtest/gtest.h>

#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageMemory.h>
#include <Functions/registerFunctions.h>


using namespace DB;


/// NOTE How to do better?
struct State
{
    Context context{Context::createGlobal()};
    NamesAndTypesList columns{
        {"column", std::make_shared<DataTypeUInt8>()},
        {"apply_id", std::make_shared<DataTypeUInt64>()},
        {"apply_type", std::make_shared<DataTypeUInt8>()},
        {"apply_status", std::make_shared<DataTypeUInt8>()},
        {"create_time", std::make_shared<DataTypeDateTime>()},
    };

    State()
    {
        registerFunctions();
        DatabasePtr database = std::make_shared<DatabaseMemory>("test");
        database->attachTable("table", StorageMemory::create("test", "table", ColumnsDescription{columns}, ConstraintsDescription{}));
        context.makeGlobalContext();
        context.addDatabase("test", database);
        context.setCurrentDatabase("test");
    }
};

static State & state()
{
    static State res;
    return res;
}


static void check(const std::string & query, const std::string & expected, const Context & context, const NamesAndTypesList & columns)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000);
    std::string transformed_query = transformQueryForExternalDatabase(*ast, columns, IdentifierQuotingStyle::DoubleQuotes, "test", "table", context);

    EXPECT_EQ(transformed_query, expected);
}


TEST(TransformQueryForExternalDatabase, InWithSingleElement)
{
    check("SELECT column FROM test.table WHERE 1 IN (1)",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE 1",
          state().context, state().columns);
    check("SELECT column FROM test.table WHERE column IN (1, 2)",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE \"column\" IN (1, 2)",
          state().context, state().columns);
    check("SELECT column FROM test.table WHERE column NOT IN ('hello', 'world')",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE \"column\" NOT IN ('hello', 'world')",
          state().context, state().columns);
}

TEST(TransformQueryForExternalDatabase, Like)
{
    check("SELECT column FROM test.table WHERE column LIKE '%hello%'",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE \"column\" LIKE '%hello%'",
          state().context, state().columns);
    check("SELECT column FROM test.table WHERE column NOT LIKE 'w%rld'",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE \"column\" NOT LIKE 'w%rld'",
          state().context, state().columns);
}

TEST(TransformQueryForExternalDatabase, Substring)
{
    check("SELECT column FROM test.table WHERE left(column, 10) = RIGHT(column, 10) AND SUBSTRING(column FROM 1 FOR 2) = 'Hello'",
          "SELECT \"column\" FROM \"test\".\"table\"",
          state().context, state().columns);
}

TEST(TransformQueryForExternalDatabase, MultipleAndSubqueries)
{
    check("SELECT column FROM test.table WHERE 1 = 1 AND toString(column) = '42' AND column = 42 AND left(column, 10) = RIGHT(column, 10) AND column IN (1, 42) AND SUBSTRING(column FROM 1 FOR 2) = 'Hello' AND column != 4",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE 1 AND (\"column\" = 42) AND (\"column\" IN (1, 42)) AND (\"column\" != 4)",
          state().context, state().columns);
    check("SELECT column FROM test.table WHERE toString(column) = '42' AND left(column, 10) = RIGHT(column, 10) AND column = 42",
          "SELECT \"column\" FROM \"test\".\"table\" WHERE (\"column\" = 42)",
          state().context, state().columns);
}

TEST(TransformQueryForExternalDatabase, Issue7245)
{
    check("select apply_id from test.table where apply_type = 2 and create_time > addDays(toDateTime('2019-01-01 01:02:03'),-7) and apply_status in (3,4)",
          "SELECT \"apply_id\", \"apply_type\", \"apply_status\", \"create_time\" FROM \"test\".\"table\" WHERE (\"apply_type\" = 2) AND (\"create_time\" > '2018-12-25 01:02:03') AND (\"apply_status\" IN (3, 4))",
          state().context, state().columns);
}
