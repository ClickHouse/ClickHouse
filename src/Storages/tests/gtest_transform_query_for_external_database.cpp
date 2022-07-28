#include <gtest/gtest.h>

#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageMemory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>


using namespace DB;


/// NOTE How to do better?
struct State
{
    State(const State&) = delete;

    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

    const NamesAndTypesList & getColumns() const
    {
        return tables[0].columns;
    }

    std::vector<TableWithColumnNamesAndTypes> getTables(size_t num = 0) const
    {
        std::vector<TableWithColumnNamesAndTypes> res;
        for (size_t i = 0; i < std::min(num, tables.size()); ++i)
            res.push_back(tables[i]);
        return res;
    }

private:

    static DatabaseAndTableWithAlias createDBAndTable(String table_name)
    {
        DatabaseAndTableWithAlias res;
        res.database = "test";
        res.table = table_name;
        return res;
    }

    const std::vector<TableWithColumnNamesAndTypes> tables{
        TableWithColumnNamesAndTypes(
            createDBAndTable("table"),
            {
                {"column", std::make_shared<DataTypeUInt8>()},
                {"apply_id", std::make_shared<DataTypeUInt64>()},
                {"apply_type", std::make_shared<DataTypeUInt8>()},
                {"apply_status", std::make_shared<DataTypeUInt8>()},
                {"create_time", std::make_shared<DataTypeDateTime>()},
                {"field", std::make_shared<DataTypeString>()},
                {"value", std::make_shared<DataTypeString>()},
            }),
        TableWithColumnNamesAndTypes(
            createDBAndTable("table2"),
            {
                {"num", std::make_shared<DataTypeUInt8>()},
                {"attr", std::make_shared<DataTypeString>()},
            }),
    };

    explicit State()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        DatabasePtr database = std::make_shared<DatabaseMemory>("test", context);

        for (const auto & tab : tables)
        {
            const auto & table_name = tab.table.table;
            const auto & db_name = tab.table.database;
            database->attachTable(
                table_name,
                StorageMemory::create(
                    StorageID(db_name, table_name), ColumnsDescription{getColumns()}, ConstraintsDescription{}, String{}));
        }
        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase("test");
    }
};

static void check(
    const State & state,
    size_t table_num,
    const std::string & query,
    const std::string & expected)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000);
    SelectQueryInfo query_info;
    SelectQueryOptions select_options;
    query_info.syntax_analyzer_result
        = TreeRewriter(state.context).analyzeSelect(ast, state.getColumns(), select_options, state.getTables(table_num));
    query_info.query = ast;
    std::string transformed_query = transformQueryForExternalDatabase(
        query_info, state.getColumns(), IdentifierQuotingStyle::DoubleQuotes, "test", "table", state.context);

    EXPECT_EQ(transformed_query, expected) << query;
}


TEST(TransformQueryForExternalDatabase, InWithSingleElement)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE 1 IN (1)",
          R"(SELECT "column" FROM "test"."table" WHERE 1)");
    check(state, 1,
          "SELECT column FROM test.table WHERE column IN (1, 2)",
          R"(SELECT "column" FROM "test"."table" WHERE "column" IN (1, 2))");
    check(state, 1,
          "SELECT column FROM test.table WHERE column NOT IN ('hello', 'world')",
          R"(SELECT "column" FROM "test"."table" WHERE "column" NOT IN ('hello', 'world'))");
}

TEST(TransformQueryForExternalDatabase, InWithMultipleColumns)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE (1,1) IN ((1,1))",
          R"(SELECT "column" FROM "test"."table" WHERE 1)");
    check(state, 1,
          "SELECT field, value FROM test.table WHERE (field, value) IN (('foo', 'bar'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar')))");
}

TEST(TransformQueryForExternalDatabase, InWithTable)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE 1 IN external_table",
          R"(SELECT "column" FROM "test"."table")");
    check(state, 1,
          "SELECT column FROM test.table WHERE 1 IN (x)",
          R"(SELECT "column" FROM "test"."table")");
    check(state, 1,
          "SELECT column, field, value FROM test.table WHERE column IN (field, value)",
          R"(SELECT "column", "field", "value" FROM "test"."table" WHERE "column" IN ("field", "value"))");
    check(state, 1,
          "SELECT column FROM test.table WHERE column NOT IN hello AND column = 123",
          R"(SELECT "column" FROM "test"."table" WHERE "column" = 123)");
}

TEST(TransformQueryForExternalDatabase, Like)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE column LIKE '%hello%'",
          R"(SELECT "column" FROM "test"."table" WHERE "column" LIKE '%hello%')");
    check(state, 1,
          "SELECT column FROM test.table WHERE column NOT LIKE 'w%rld'",
          R"(SELECT "column" FROM "test"."table" WHERE "column" NOT LIKE 'w%rld')");
}

TEST(TransformQueryForExternalDatabase, Substring)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE left(column, 10) = RIGHT(column, 10) AND SUBSTRING(column FROM 1 FOR 2) = 'Hello'",
          R"(SELECT "column" FROM "test"."table")");
}

TEST(TransformQueryForExternalDatabase, MultipleAndSubqueries)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT column FROM test.table WHERE 1 = 1 AND toString(column) = '42' AND column = 42 AND left(column, 10) = RIGHT(column, 10) AND column IN (1, 42) AND SUBSTRING(column FROM 1 FOR 2) = 'Hello' AND column != 4",
          R"(SELECT "column" FROM "test"."table" WHERE 1 AND ("column" = 42) AND ("column" IN (1, 42)) AND ("column" != 4))");
    check(state, 1,
          "SELECT column FROM test.table WHERE toString(column) = '42' AND left(column, 10) = RIGHT(column, 10) AND column = 42",
          R"(SELECT "column" FROM "test"."table" WHERE "column" = 42)");
}

TEST(TransformQueryForExternalDatabase, Issue7245)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT apply_id FROM test.table WHERE apply_type = 2 AND create_time > addDays(toDateTime('2019-01-01 01:02:03'),-7) AND apply_status IN (3,4)",
          R"(SELECT "apply_id", "apply_type", "apply_status", "create_time" FROM "test"."table" WHERE ("apply_type" = 2) AND ("create_time" > '2018-12-25 01:02:03') AND ("apply_status" IN (3, 4)))");
}

TEST(TransformQueryForExternalDatabase, Aliases)
{
    const State & state = State::instance();

    check(state, 1,
          "SELECT field AS value, field AS display WHERE field NOT IN ('') AND display LIKE '%test%'",
          R"(SELECT "field" FROM "test"."table" WHERE ("field" NOT IN ('')) AND ("field" LIKE '%test%'))");
}

TEST(TransformQueryForExternalDatabase, ForeignColumnInWhere)
{
    const State & state = State::instance();

    check(state, 2,
          "SELECT column FROM test.table "
          "JOIN test.table2 AS table2 ON (test.table.apply_id = table2.num) "
          "WHERE column > 2 AND (apply_id = 1 OR table2.num = 1) AND table2.attr != ''",
          R"(SELECT "column", "apply_id" FROM "test"."table" WHERE ("column" > 2) AND ("apply_id" = 1))");
}
