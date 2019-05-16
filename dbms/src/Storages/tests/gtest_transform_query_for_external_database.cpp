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


TEST(transformQueryForExternalDatabase, InWithSingleElement)
{
    using namespace DB;

    std::string query = "SELECT column FROM test.table WHERE 1 IN (1)";   /// Parentheses around rhs of IN must be retained after transformation.
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000);

    Context context = Context::createGlobal();
    DatabasePtr database = std::make_shared<DatabaseMemory>("test");
    NamesAndTypesList columns{{"column", std::make_shared<DataTypeUInt8>()}};
    database->attachTable("table", StorageMemory::create("table", ColumnsDescription{columns}));
    context.addDatabase("test", database);
    context.setCurrentDatabase("test");

    std::string transformed_query = transformQueryForExternalDatabase(*ast, columns, IdentifierQuotingStyle::DoubleQuotes, "test", "table", context);

    EXPECT_EQ(transformed_query, "SELECT \"column\" FROM \"test\".\"table\"  WHERE 1 IN (1)");
}
