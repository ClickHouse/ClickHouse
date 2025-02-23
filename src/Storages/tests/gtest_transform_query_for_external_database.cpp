#include <gtest/gtest.h>

#include <Storages/MemorySettings.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageMemory.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Planner/Utils.h>

using namespace DB;


/// TODO: use gtest fixture
struct State
{
    State(const State&) = delete;

    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

    const NamesAndTypesList & getColumns(size_t idx = 0) const
    {
        return tables[idx].columns;
    }

    std::vector<TableWithColumnNamesAndTypes> getTables(size_t num = 0) const
    {
        std::vector<TableWithColumnNamesAndTypes> res;
        for (size_t i = 0; i < std::min(num, tables.size()); ++i)
            res.push_back(tables[i]);
        return res;
    }

private:

    static DatabaseAndTableWithAlias createDBAndTable(String table_name, String database_name = "test")
    {
        DatabaseAndTableWithAlias res;
        res.database = database_name;
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
                {"a", std::make_shared<DataTypeUInt8>()},
                {"b", std::make_shared<DataTypeDate>()},
                {"foo", std::make_shared<DataTypeString>()},
                {"is_value", DataTypeFactory::instance().get("Bool")},
            }),
        TableWithColumnNamesAndTypes(
            createDBAndTable("table2"),
            {
                {"num", std::make_shared<DataTypeUInt8>()},
                {"attr", std::make_shared<DataTypeString>()},
            }),
        TableWithColumnNamesAndTypes(
            createDBAndTable("external_table"),
            {
                {"ttt", std::make_shared<DataTypeUInt8>()},
            }),
    };

    explicit State()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();
        DatabasePtr database = std::make_shared<DatabaseMemory>("test", context);

        for (const auto & tab : tables)
        {
            const auto & table_name = tab.table.table;
            const auto & db_name = tab.table.database;
            database->attachTable(
                context,
                table_name,
                std::make_shared<StorageMemory>(
                    StorageID(db_name, table_name), ColumnsDescription{tab.columns}, ConstraintsDescription{}, String{}, MemorySettings{}));
        }
        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);

        context->setCurrentDatabase("test");
    }
};

static void checkOld(
    const State & state,
    size_t table_num,
    const std::string & query,
    const std::string & expected)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000, 1000000);
    SelectQueryInfo query_info;
    SelectQueryOptions select_options;
    query_info.syntax_analyzer_result
        = TreeRewriter(state.context).analyzeSelect(ast, DB::TreeRewriterResult(state.getColumns(0)), select_options, state.getTables(table_num));
    query_info.query = ast;
    std::string transformed_query = transformQueryForExternalDatabase(
        query_info,
        query_info.syntax_analyzer_result->requiredSourceColumns(),
        state.getColumns(0), IdentifierQuotingStyle::DoubleQuotes,
        LiteralEscapingStyle::Regular, "test", "table", state.context);

    EXPECT_EQ(transformed_query, expected) << query;
}

/// Required for transformQueryForExternalDatabase. In real life table expression is calculated via planner.
/// But in tests we can just find it in JOIN TREE.
static QueryTreeNodePtr findTableExpression(const QueryTreeNodePtr & node, const String & table_name)
{
    if (node->getNodeType() == QueryTreeNodeType::TABLE)
    {
        if (node->as<TableNode>()->getStorageID().table_name == table_name)
            return node;
    }

    if (node->getNodeType() == QueryTreeNodeType::JOIN)
    {
        if (auto res = findTableExpression(node->as<JoinNode>()->getLeftTableExpression(), table_name))
            return res;
        if (auto res = findTableExpression(node->as<JoinNode>()->getRightTableExpression(), table_name))
            return res;
    }
    return nullptr;
}

/// `column_names` - Normally it's passed to query plan step. But in test we do it manually.
static void checkNewAnalyzer(
    const State & state,
    const Names & column_names,
    const std::string & query,
    const std::string & expected)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000, 1000000);

    SelectQueryOptions select_query_options;
    auto query_tree = buildQueryTree(ast, state.context);
    QueryTreePassManager query_tree_pass_manager(state.context);
    addQueryTreePasses(query_tree_pass_manager);
    query_tree_pass_manager.run(query_tree);

    InterpreterSelectQueryAnalyzer interpreter(query_tree, state.context, select_query_options);
    interpreter.getQueryPlan();

    auto planner_context = interpreter.getPlanner().getPlannerContext();
    SelectQueryInfo query_info = buildSelectQueryInfo(query_tree, planner_context);
    const auto * query_node = query_info.query_tree->as<QueryNode>();
    if (!query_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryNode expected");

    query_info.table_expression = findTableExpression(query_node->getJoinTree(), "table");

    std::string transformed_query = transformQueryForExternalDatabase(
        query_info, column_names, state.getColumns(0), IdentifierQuotingStyle::DoubleQuotes,
        LiteralEscapingStyle::Regular, "test", "table", state.context);

    EXPECT_EQ(transformed_query, expected) << query;
}

static void check(
    const State & state,
    size_t table_num,
    const Names & column_names,
    const std::string & query,
    const std::string & expected,
    const std::string & expected_new = "")
{
    {
        SCOPED_TRACE("Old analyzer");
        checkOld(state, table_num, query, expected);
    }
    {
        SCOPED_TRACE("New analyzer");
        checkNewAnalyzer(state, column_names, query, expected_new.empty() ? expected : expected_new);
    }
}

TEST(TransformQueryForExternalDatabase, InWithSingleElement)
{
    const State & state = State::instance();

    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE 1 IN (1)",
          R"(SELECT "column" FROM "test"."table" WHERE 1 = 1)",
          R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE column IN (1, 2)",
          R"(SELECT "column" FROM "test"."table" WHERE "column" IN (1, 2))");

    check(state, 1, {"field"},
          "SELECT field FROM test.table WHERE field NOT IN ('hello', 'world')",
          R"(SELECT "field" FROM "test"."table" WHERE "field" NOT IN ('hello', 'world'))");
}

TEST(TransformQueryForExternalDatabase, InWithMultipleColumns)
{
    const State & state = State::instance();

    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE (1,1) IN ((1,1))",
          R"(SELECT "column" FROM "test"."table" WHERE 1 = 1)",
          R"(SELECT "column" FROM "test"."table")");
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM test.table WHERE (field, value) IN (('foo', 'bar'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar')))");
}

TEST(TransformQueryForExternalDatabase, InWithTable)
{
    const State & state = State::instance();

    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE 1 IN external_table",
          R"(SELECT "column" FROM "test"."table")");
    check(state, 1, {"column"},
          "WITH x as (SELECT * FROM external_table) SELECT column FROM test.table WHERE 1 IN (x)",
          R"(SELECT "column" FROM "test"."table")");
    check(state, 1, {"column", "field", "value"},
          "SELECT column, field, value FROM test.table WHERE column IN (1, 2)",
          R"(SELECT "column", "field", "value" FROM "test"."table" WHERE "column" IN (1, 2))");
    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE column NOT IN external_table AND column = 123",
          R"(SELECT "column" FROM "test"."table" WHERE "column" = 123)");
}

TEST(TransformQueryForExternalDatabase, Like)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
          "SELECT field FROM test.table WHERE field LIKE '%hello%'",
          R"(SELECT "field" FROM "test"."table" WHERE "field" LIKE '%hello%')");
    check(state, 1, {"field"},
          "SELECT field FROM test.table WHERE field NOT LIKE 'w%rld'",
          R"(SELECT "field" FROM "test"."table" WHERE "field" NOT LIKE 'w%rld')");
}

TEST(TransformQueryForExternalDatabase, Substring)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
          "SELECT field FROM test.table WHERE left(field, 10) = RIGHT(field, 10) AND SUBSTRING(field FROM 1 FOR 2) = 'Hello'",
          R"(SELECT "field" FROM "test"."table")");
}

TEST(TransformQueryForExternalDatabase, MultipleAndSubqueries)
{
    const State & state = State::instance();

    check(
        state,
        1,
        {"column"},
        "SELECT column FROM test.table WHERE 1 = 1 AND toString(column) = '42' AND column = 42 AND left(toString(column), 10) = "
        "RIGHT(toString(column), 10) AND column IN (1, 42) AND SUBSTRING(toString(column) FROM 1 FOR 2) = 'Hello' AND column != 4",
        R"(SELECT "column" FROM "test"."table" WHERE (1 = 1) AND ("column" = 42) AND ("column" IN (1, 42)) AND ("column" != 4))");
    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE toString(column) = '42' AND left(toString(column), 10) = RIGHT(toString(column), 10) AND column = 42",
          R"(SELECT "column" FROM "test"."table" WHERE "column" = 42)");
}

TEST(TransformQueryForExternalDatabase, Issue7245)
{
    const State & state = State::instance();

    check(state, 1, {"apply_id", "apply_type", "apply_status", "create_time"},
          "SELECT apply_id FROM test.table WHERE apply_type = 2 AND create_time > addDays(toDateTime('2019-01-01 01:02:03'),-7) AND apply_status IN (3,4)",
          R"(SELECT "apply_id", "apply_type", "apply_status", "create_time" FROM "test"."table" WHERE ("apply_type" = 2) AND ("create_time" > '2018-12-25 01:02:03') AND ("apply_status" IN (3, 4)))");
}

TEST(TransformQueryForExternalDatabase, Aliases)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
          "SELECT field AS value, field AS display FROM table WHERE field NOT IN ('') AND display LIKE '%test%'",
          R"(SELECT "field" FROM "test"."table" WHERE ("field" NOT IN ('')) AND ("field" LIKE '%test%'))");
}

TEST(TransformQueryForExternalDatabase, ForeignColumnInWhere)
{
    const State & state = State::instance();

    check(state, 2, {"column", "apply_id"},
          "SELECT column FROM test.table "
          "JOIN test.table2 AS table2 ON (test.table.apply_id = table2.num) "
          "WHERE column > 2 AND apply_id = 1 AND table2.num = 1 AND table2.attr != ''",
          R"(SELECT "column", "apply_id" FROM "test"."table" WHERE ("column" > 2) AND ("apply_id" = 1))");
}

TEST(TransformQueryForExternalDatabase, TupleSurroundPredicates)
{
    const State & state = State::instance();

    check(
        state,
        1,
        {"column", "field", "a"},
        "SELECT column, field, a FROM table WHERE ((column > 10) AND (length(field) > 0)) AND a > 0",
        R"(SELECT "column", "field", "a" FROM "test"."table" WHERE ("a" > 0) AND ("column" > 10))");
}

TEST(TransformQueryForExternalDatabase, NoStrict)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field IN (SELECT attr FROM table2)",
          R"(SELECT "field" FROM "test"."table")");
}

TEST(TransformQueryForExternalDatabase, Strict)
{
    const State & state = State::instance();
    state.context->setSetting("external_table_strict_query", true);

    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field = '1'",
          R"(SELECT "field" FROM "test"."table" WHERE "field" = '1')");
    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field IN ('1', '2')",
          R"(SELECT "field" FROM "test"."table" WHERE "field" IN ('1', '2'))");
    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field LIKE '%test%'",
          R"(SELECT "field" FROM "test"."table" WHERE "field" LIKE '%test%')");

    /// removeUnknownSubexpressionsFromWhere() takes place
    EXPECT_THROW(check(state, 1, {"field"}, "SELECT field FROM table WHERE field IN (SELECT attr FROM table2)", ""), Exception);
    /// !isCompatible() takes place
    EXPECT_THROW(check(state, 1, {"column"}, "SELECT column FROM test.table WHERE left(column, 10) = RIGHT(column, 10) AND SUBSTRING(column FROM 1 FOR 2) = 'Hello'", ""), Exception);
}

TEST(TransformQueryForExternalDatabase, Null)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field IS NULL",
          R"(SELECT "field" FROM "test"."table" WHERE "field" IS NULL)",
          R"(SELECT "field" FROM "test"."table" WHERE 1 = 0)");
    check(state, 1, {"field"},
          "SELECT field FROM table WHERE field IS NOT NULL",
          R"(SELECT "field" FROM "test"."table" WHERE "field" IS NOT NULL)",
          R"(SELECT "field" FROM "test"."table")");

    check(state, 1, {"field"},
          "SELECT field FROM table WHERE isNull(field)",
          R"(SELECT "field" FROM "test"."table" WHERE "field" IS NULL)",
          R"(SELECT "field" FROM "test"."table" WHERE 1 = 0)");
    check(state, 1, {"field"},
          "SELECT field FROM table WHERE isNotNull(field)",
          R"(SELECT "field" FROM "test"."table" WHERE "field" IS NOT NULL)",
          R"(SELECT "field" FROM "test"."table")");
}

TEST(TransformQueryForExternalDatabase, ToDate)
{
    const State & state = State::instance();

    check(state, 1, {"a", "b", "foo"},
        "SELECT foo FROM table WHERE a=10 AND b=toDate('2019-10-05')",
        R"(SELECT "a", "b", "foo" FROM "test"."table" WHERE ("a" = 10) AND ("b" = '2019-10-05'))");
}

TEST(TransformQueryForExternalDatabase, Analyzer)
{
    const State & state = State::instance();

    check(state, 1, {"field"},
        "SELECT count() FROM table WHERE field LIKE '%name_%'",
        R"(SELECT "field" FROM "test"."table" WHERE "field" LIKE '%name_%')");

    check(state, 1, {"column"},
        "SELECT 1 FROM table",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT sleepEachRow(1) FROM table",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column", "apply_id", "apply_type", "apply_status", "create_time", "field", "value", "a", "b", "foo"},
        "SELECT * EXCEPT (is_value) FROM table WHERE (column) IN (1)",
        R"(SELECT "column", "apply_id", "apply_type", "apply_status", "create_time", "field", "value", "a", "b", "foo" FROM "test"."table" WHERE "column" IN (1))");

    check(state, 1, {"is_value"},
        "SELECT is_value FROM table WHERE is_value = true",
        R"(SELECT "is_value" FROM "test"."table" WHERE "is_value" = true)");

    check(state, 1, {"is_value"},
        "SELECT is_value FROM table WHERE is_value = 1",
        R"(SELECT "is_value" FROM "test"."table" WHERE "is_value" = 1)");
}
