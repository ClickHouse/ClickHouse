#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
    #pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>

#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageMemory.h>
#include <Functions/registerFunctions.h>


using namespace DB;


void check(const std::string & query, const std::string & expected, const Context & context, const NamesAndTypesList & columns)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000);
    std::string transformed_query = transformQueryForExternalDatabase(*ast, columns, IdentifierQuotingStyle::DoubleQuotes, "test", "table", context);

    EXPECT_EQ(transformed_query, expected);
}


TEST(transformQueryForExternalDatabase, InWithSingleElement)
{
    registerFunctions();

    Context context = Context::createGlobal();
    DatabasePtr database = std::make_shared<DatabaseMemory>("test");
    NamesAndTypesList columns{{"column", std::make_shared<DataTypeUInt8>()}};
    database->attachTable("table", StorageMemory::create("table", ColumnsDescription{columns}));
    context.addDatabase("test", database);
    context.setCurrentDatabase("test");

    check("SELECT column FROM test.table WHERE 1 IN (1)", "SELECT \"column\" FROM \"test\".\"table\"  WHERE 1 IN (1)", context, columns);
    check("SELECT column FROM test.table WHERE column IN (1, 2)", "SELECT \"column\" FROM \"test\".\"table\"  WHERE \"column\" IN (1, 2)", context, columns);
    check("SELECT column FROM test.table WHERE column NOT IN ('hello', 'world')", "SELECT \"column\" FROM \"test\".\"table\"  WHERE \"column\" NOT IN ('hello', 'world')", context, columns);
}

TEST(transformQueryForExternalDatabase, Like)
{
    registerFunctions();

    Context context = Context::createGlobal();
    DatabasePtr database = std::make_shared<DatabaseMemory>("test");
    NamesAndTypesList columns{{"column", std::make_shared<DataTypeUInt8>()}};
    database->attachTable("table", StorageMemory::create("table", ColumnsDescription{columns}));
    context.addDatabase("test", database);
    context.setCurrentDatabase("test");

    check("SELECT column FROM test.table WHERE column LIKE '%hello%'", "SELECT \"column\" FROM \"test\".\"table\"  WHERE \"column\" LIKE '%hello%'", context, columns);
    check("SELECT column FROM test.table WHERE column NOT LIKE 'w%rld'", "SELECT \"column\" FROM \"test\".\"table\"  WHERE \"column\" NOT LIKE 'w%rld'", context, columns);
}
