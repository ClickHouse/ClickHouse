#include <gtest/gtest.h>

#include <optional>

#include <Storages/MemorySettings.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
#include <Analyzer/QueryTreePassManager.h>
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
                {"uuid_col", std::make_shared<DataTypeUUID>()},
                {"lc_uuid_col", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUUID>())},
                {"arr", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>())},
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
                    StorageID(db_name, table_name), ColumnsDescription{tab.columns}, ConstraintsDescription{}, String{}, MemorySettings{}), {});
        }
        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);

        context->setCurrentDatabase("test");
    }
};

/// A filter that is applied locally, on top of the rows read from the external table, and is not a part
/// of the query AST - `additional_table_filters` is the user-facing way to get one. `SelectQueryInfo`
/// is normally filled in by the interpreter / planner, so in the test it is filled in manually.
static ASTPtr parseLocalFilter(const std::string & filter)
{
    if (filter.empty())
        return nullptr;
    ParserExpression parser;
    return parseQuery(parser, filter, 1000, 1000, 1000000);
}

static void checkOld(
    const State & state,
    size_t table_num,
    const std::string & query,
    const std::string & expected,
    LiteralEscapingStyle literal_escaping_style = LiteralEscapingStyle::Regular,
    const std::string & additional_filter = "",
    std::optional<size_t> limit = {},
    bool allow_limit_push_down = true)
{
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000, 1000000);
    SelectQueryInfo query_info;
    SelectQueryOptions select_options;
    query_info.syntax_analyzer_result
        = TreeRewriter(state.context).analyzeSelect(ast, DB::TreeRewriterResult(state.getColumns(0)), select_options, state.getTables(table_num));
    query_info.query = ast;
    if (auto additional_filter_ast = parseLocalFilter(additional_filter))
    {
        query_info.additional_filter_ast = additional_filter_ast;
        query_info.filter_asts.push_back(additional_filter_ast);
    }
    std::string transformed_query = transformQueryForExternalDatabase(
        query_info,
        query_info.syntax_analyzer_result->requiredSourceColumns(),
        state.getColumns(0), IdentifierQuotingStyle::DoubleQuotes,
        literal_escaping_style, "test", "table", state.context, limit, allow_limit_push_down);

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
        if (auto res = findTableExpression(node->as<JoinNode>()->getLeftTableExpressionNode(), table_name))
            return res;
        if (auto res = findTableExpression(node->as<JoinNode>()->getRightTableExpressionNode(), table_name))
            return res;
    }
    return nullptr;
}

/// `column_names` - Normally it's passed to query plan step. But in test we do it manually.
static void checkNewAnalyzer(
    const State & state,
    const Names & column_names,
    const std::string & query,
    const std::string & expected,
    LiteralEscapingStyle literal_escaping_style = LiteralEscapingStyle::Regular,
    const std::string & additional_filter = "",
    std::optional<size_t> limit = {},
    bool allow_limit_push_down = true)
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

    query_info.table_expression = static_pointer_cast<ITableExpressionNode>(findTableExpression(query_node->getJoinTreeNode(), "table"));
    query_info.additional_filter_ast = parseLocalFilter(additional_filter);

    std::string transformed_query = transformQueryForExternalDatabase(
        query_info, column_names, state.getColumns(0), IdentifierQuotingStyle::DoubleQuotes,
        literal_escaping_style, "test", "table", state.context, limit, allow_limit_push_down);

    EXPECT_EQ(transformed_query, expected) << query;
}

static void check(
    const State & state,
    size_t table_num,
    const Names & column_names,
    const std::string & query,
    const std::string & expected,
    const std::string & expected_new = "",
    LiteralEscapingStyle literal_escaping_style = LiteralEscapingStyle::Regular,
    const std::string & additional_filter = "",
    std::optional<size_t> limit = {},
    bool allow_limit_push_down = true)
{
    {
        SCOPED_TRACE("Old analyzer");
        checkOld(state, table_num, query, expected, literal_escaping_style, additional_filter, limit, allow_limit_push_down);
    }
    {
        SCOPED_TRACE("Analyzer");
        checkNewAnalyzer(state, column_names, query, expected_new.empty() ? expected : expected_new, literal_escaping_style, additional_filter, limit, allow_limit_push_down);
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
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM test.table WHERE (field, value) IN (('foo', 'bar'), ('qux', 'baz'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar'), ('qux', 'baz')))");
    /// The same single-row set carried by an explicit `tuple` call instead of the parser's
    /// fast-path `ASTLiteral(Tuple)` must keep its outer parentheses too.
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM test.table WHERE tuple(field, value) IN (tuple('foo', 'bar'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar')))");
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM test.table WHERE tuple(field, value) IN tuple('foo', 'bar')",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar')))");
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM test.table WHERE tuple(field, value) IN (tuple(tuple('foo', 'bar'), tuple('qux', 'baz')))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar'), ('qux', 'baz')))");
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
        R"(SELECT "column" FROM "test"."table" WHERE (1 = 1) AND ("column" = 42) AND ("column" IN (1, 42)) AND ("column" != 4))",
        R"(SELECT "column" FROM "test"."table" WHERE (1 = 1) AND ("column" = 42) AND ("column" IN (1, 42)))");
    check(state, 1, {"column"},
          "SELECT column FROM test.table WHERE toString(column) = '42' AND left(toString(column), 10) = RIGHT(toString(column), 10) AND column = 42",
          R"(SELECT "column" FROM "test"."table" WHERE "column" = 42)");
}

TEST(TransformQueryForExternalDatabase, Issue7245)
{
    const State & state = State::instance();

    check(state, 1, {"apply_id", "apply_type", "apply_status", "create_time"},
          "SELECT apply_id FROM test.table WHERE apply_type = 2 AND create_time > addDays(toDateTime('2019-01-01 01:02:03', 'UTC'),-7) AND apply_status IN (3,4)",
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
        "SELECT foo FROM table WHERE a=10 AND b=toDate('2019-10-05', 'UTC')",
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

    check(state, 1, {"column", "apply_id", "apply_type", "apply_status", "create_time", "field", "value", "a", "b", "foo", "uuid_col", "lc_uuid_col", "arr"},
        "SELECT * EXCEPT (is_value) FROM table WHERE (column) IN (1)",
        R"(SELECT "column", "apply_id", "apply_type", "apply_status", "create_time", "field", "value", "a", "b", "foo", "uuid_col", "lc_uuid_col", "arr" FROM "test"."table" WHERE ("column") IN (1))",
        R"(SELECT "column", "apply_id", "apply_type", "apply_status", "create_time", "field", "value", "a", "b", "foo", "uuid_col", "lc_uuid_col", "arr" FROM "test"."table" WHERE "column" IN (1))");

    check(state, 1, {"is_value"},
        "SELECT is_value FROM table WHERE is_value = true",
        R"(SELECT "is_value" FROM "test"."table" WHERE "is_value" = true)");

    check(state, 1, {"is_value"},
        "SELECT is_value FROM table WHERE is_value = 1",
        R"(SELECT "is_value" FROM "test"."table" WHERE "is_value" = 1)");
}

TEST(TransformQueryForExternalDatabase, Limit)
{
    const State & state = State::instance();

    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table" LIMIT 10)");

    /// The OFFSET is applied locally, so the rows it skips still have to be read from the
    /// external table: the pushed-down limit is `offset + length`.
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10 OFFSET 5",
        R"(SELECT "column" FROM "test"."table" LIMIT 15)");
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 5, 10",
        R"(SELECT "column" FROM "test"."table" LIMIT 15)");

    /// An `OFFSET` without a `LIMIT` gives nothing to push down.
    check(state, 1, {"column"},
        "SELECT column FROM table OFFSET 5",
        R"(SELECT "column" FROM "test"."table")");

    /// `offset + length` must not overflow.
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 18446744073709551615 OFFSET 1",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10 BY column",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 2, {"column", "apply_id"},
        "SELECT column FROM test.table "
        "JOIN test.table2 AS table2 ON (test.table.apply_id = test.table2.num) "
        "WHERE column > 2 AND apply_id = 1 AND table2.num = 1 AND table2.attr != '' LIMIT 10",
        R"(SELECT "column", "apply_id" FROM "test"."table" WHERE ("column" > 2) AND ("apply_id" = 1))");

    check(state, 2, {"column", "apply_id"},
        "SELECT column FROM test.table "
        "JOIN test.table2 AS table2 ON (test.table.apply_id = test.table2.num) LIMIT 10",
        R"(SELECT "column", "apply_id" FROM "test"."table")");

    /// Modifiers that are applied locally and change which rows the query returns must
    /// disable the push-down, including those that are not children of `ASTSelectQuery`
    /// (e.g. DISTINCT is just a flag): limiting remotely could return wrong results.
    check(state, 1, {"column"},
        "SELECT DISTINCT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT column FROM table ORDER BY column LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT column FROM table ORDER BY column LIMIT 10 WITH TIES",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT column FROM table GROUP BY column LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT column FROM table GROUP BY ALL LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    /// The SELECT list is evaluated locally and the LIMIT is applied to its result, so expressions
    /// that do not map one source row to one result row must disable the push-down as well.
    check(state, 1, {"column"},
        "SELECT sum(column) FROM table LIMIT 1",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT sum(column) + 1 FROM table LIMIT 1",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT sum(column) OVER () FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT arrayJoin(range(column)) FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    /// `unnest` is a case-insensitive alias of `arrayJoin`. `TreeRewriter` normally rewrites it to the
    /// canonical name before this code runs, but not when `normalize_function_names` is disabled (and
    /// not for a secondary query of a distributed one), so the alias must be resolved here as well.
    check(state, 1, {"column"},
        "SELECT unnest(range(column)) FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    state.context->setSetting("normalize_function_names", false);
    check(state, 1, {"column"},
        "SELECT unnest(range(column)) FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    check(state, 1, {"column"},
        "SELECT UNNEST(range(column)) FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");
    state.context->setSetting("normalize_function_names", true);

    /// A plain projection expression does not change the number of rows, so it is still pushed down.
    check(state, 1, {"column"},
        "SELECT column + 1 FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table" LIMIT 10)");

    /// When the WHERE clause is copied to the external query only partially,
    /// the rest of it is applied locally, so the LIMIT must not be pushed down either.
    /// (Range comparisons on UUID columns are not compatible with external databases.)
    state.context->setSetting("external_table_strict_query", false);
    check(state, 1, {"column", "uuid_col"},
        "SELECT column FROM table WHERE column > 2 AND uuid_col > toUUID('12345678-1234-1234-1234-123456789012') LIMIT 10",
        R"(SELECT "column", "uuid_col" FROM "test"."table" WHERE "column" > 2)");

    /// The SETTINGS clause does not change the data, so it does not prevent the push-down.
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10 SETTINGS max_threads = 1",
        R"(SELECT "column" FROM "test"."table" LIMIT 10)");

    /// A filter that is applied locally on top of the rows read from the external table - here an
    /// `additional_table_filters` entry - is not a part of the rewritten query, but it runs before the
    /// LIMIT. Pushing the LIMIT down would truncate the remote result before that filter is applied and
    /// could return fewer rows than the query should.
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")",
        /*expected_new=*/"",
        /*literal_escaping_style=*/LiteralEscapingStyle::Regular,
        /*additional_filter=*/"column > 100");

    /// The same, with a WHERE clause that is fully pushed down: the local filter alone still blocks it.
    check(state, 1, {"column"},
        "SELECT column FROM table WHERE column > 2 LIMIT 10",
        R"(SELECT "column" FROM "test"."table" WHERE "column" > 2)",
        /*expected_new=*/"",
        /*literal_escaping_style=*/LiteralEscapingStyle::Regular,
        /*additional_filter=*/"column > 100");

    /// With the analyzer, a custom-key parallel-replicas predicate is installed as a planner filter
    /// instead of being retained in `SelectQueryInfo`. It is still local and runs before `LIMIT`.
    state.context->setSetting("allow_experimental_parallel_reading_from_replicas", String("1"));
    state.context->setSetting("max_parallel_replicas", String("2"));
    state.context->setSetting("parallel_replicas_count", String("2"));
    state.context->setSetting("parallel_replicas_mode", String("custom_key_sampling"));
    state.context->setSetting("parallel_replicas_custom_key", String("column"));
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");
    state.context->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    state.context->setSetting("max_parallel_replicas", String("1"));
    state.context->setSetting("parallel_replicas_count", String("1"));

    /// `external_storage_push_down_limit = false` disables pushing the LIMIT down (previous behavior).
    state.context->setSetting("external_storage_push_down_limit", String("0"));
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")");

    /// An explicit limit supplied by `StoragePostgreSQL` must be disabled too.
    check(state, 1, {"column"},
        "SELECT column FROM table",
        R"(SELECT "column" FROM "test"."table")",
        /*expected_new=*/"",
        /*literal_escaping_style=*/LiteralEscapingStyle::Regular,
        /*additional_filter=*/"",
        /*limit=*/10);
    state.context->setSetting("external_storage_push_down_limit", String("1"));

    /// Generic ODBC/JDBC bridges only report identifier quoting. Until they also report
    /// LIMIT syntax support, they must retain the historical local LIMIT evaluation.
    check(state, 1, {"column"},
        "SELECT column FROM table LIMIT 10",
        R"(SELECT "column" FROM "test"."table")",
        /*expected_new=*/"",
        /*literal_escaping_style=*/LiteralEscapingStyle::Regular,
        /*additional_filter=*/"",
        /*limit=*/{},
        /*allow_limit_push_down=*/false);
}

TEST(TransformQueryForExternalDatabase, UUIDColumn)
{
    const State & state = State::instance();
    /// The State context is shared between tests; make sure strict mode (set by the Strict test)
    /// is off here, so non-compatible predicates are dropped rather than throwing.
    state.context->setSetting("external_table_strict_query", false);

    /// ClickHouse and external databases sort UUIDs differently, so range comparisons on a UUID
    /// column must not be pushed down - the predicate would compare against a different ordering
    /// and silently drop rows. It is applied locally instead, leaving the external query without it.
    /// See https://github.com/ClickHouse/ClickHouse/issues/105558.
    check(state, 1, {"uuid_col"},
          "SELECT uuid_col FROM table WHERE uuid_col >= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')",
          R"(SELECT "uuid_col" FROM "test"."table")");
    check(state, 1, {"uuid_col"},
          "SELECT uuid_col FROM table WHERE uuid_col < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')",
          R"(SELECT "uuid_col" FROM "test"."table")");

    /// The UUID column may be nested inside a tuple/row comparison; that must not be pushed down either.
    /// (The pushed-down query lists columns in table-definition order, so "a" precedes "uuid_col".)
    check(state, 1, {"a", "uuid_col"},
          "SELECT a, uuid_col FROM table WHERE (uuid_col, a) > (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 0)",
          R"(SELECT "a", "uuid_col" FROM "test"."table")");

    /// A LowCardinality(UUID) column is still UUID-backed, so its range comparisons must stay local too.
    check(state, 1, {"lc_uuid_col"},
          "SELECT lc_uuid_col FROM table WHERE lc_uuid_col > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')",
          R"(SELECT "lc_uuid_col" FROM "test"."table")");

    /// Equality does not depend on ordering, so it remains pushed down.
    check(state, 1, {"uuid_col"},
          "SELECT uuid_col FROM table WHERE uuid_col = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')",
          R"(SELECT "uuid_col" FROM "test"."table" WHERE "uuid_col" = '61f0c404-5cb3-11e7-907b-a6006ad3dba0')");

    /// In a conjunction, the equality is still pushed down while the range predicate stays local.
    check(state, 1, {"uuid_col"},
          "SELECT uuid_col FROM table WHERE uuid_col = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AND uuid_col > toUUID('12345678-1234-1234-1234-123456789012')",
          R"(SELECT "uuid_col" FROM "test"."table" WHERE "uuid_col" = '61f0c404-5cb3-11e7-907b-a6006ad3dba0')");
}

TEST(TransformQueryForExternalDatabase, ArrayLiteral)
{
    const State & state = State::instance();
    /// The State context is shared between tests; make sure strict mode (set by the Strict test)
    /// is off here, so non-compatible predicates are dropped rather than throwing.
    state.context->setSetting("external_table_strict_query", false);

    /// External databases do not understand ClickHouse `[...]` array syntax, so predicates with
    /// Array literals must not be pushed down - they are evaluated locally instead. A top-level
    /// Array literal has been rejected since long ago:
    check(state, 1, {"arr"},
          "SELECT arr FROM table WHERE arr = [1, 2]",
          R"(SELECT "arr" FROM "test"."table")");

    /// But an Array literal nested inside an IN tuple must be rejected too, both for a
    /// single-row set (the pushed-down query lists columns in table-definition order):
    check(state, 1, {"a", "arr"},
          "SELECT a, arr FROM table WHERE (a, arr) IN ((1, [1, 2]))",
          R"(SELECT "a", "arr" FROM "test"."table")");

    /// ... and for a multi-row set:
    check(state, 1, {"a", "arr"},
          "SELECT a, arr FROM table WHERE (a, arr) IN ((1, [1, 2]), (3, [4]))",
          R"(SELECT "a", "arr" FROM "test"."table")");

    /// In a conjunction, the compatible predicate is still pushed down while the one with the
    /// nested Array literal stays local.
    check(state, 1, {"a", "arr"},
          "SELECT a, arr FROM table WHERE (a, arr) IN ((1, [1, 2])) AND a > 0",
          R"(SELECT "a", "arr" FROM "test"."table" WHERE "a" > 0)");
}

TEST(TransformQueryForExternalDatabase, RowValueOutsideComparison)
{
    const State & state = State::instance();
    /// The State context is shared between tests; make sure strict mode (set by the Strict test)
    /// is off here, so non-compatible predicates are dropped rather than throwing.
    state.context->setSetting("external_table_strict_query", false);

    /// A multi-column tuple is written as the row value `("field", "value")`, which MySQL and
    /// SQLite accept only next to a comparison or `IN`. As the argument of `IS NOT NULL` it is a
    /// syntax error there (SQLite: "row value misused"), so the predicate must not be pushed down
    /// - it is evaluated by ClickHouse instead. (Under the analyzer it folds away entirely,
    /// because a tuple of non-Nullable columns is never NULL.)
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM table WHERE (field, value) IS NOT NULL",
          R"(SELECT "field", "value" FROM "test"."table")");
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM table WHERE isNull((field, value))",
          R"(SELECT "field", "value" FROM "test"."table")",
          R"(SELECT "field", "value" FROM "test"."table" WHERE 1 = 0)");

    /// In a conjunction only the tuple predicate stays local.
    check(state, 1, {"field", "value", "a"},
          "SELECT field, value, a FROM table WHERE ((field, value) IS NOT NULL) AND (a > 0)",
          R"(SELECT "field", "value", "a" FROM "test"."table" WHERE ("a" > 0))",
          R"(SELECT "field", "value", "a" FROM "test"."table" WHERE (1 = 1) AND ("a" > 0))");

    /// A row value is still pushed down where the external database accepts it: as the left-hand
    /// side of `IN` (a tuple comparison is rewritten into per-column comparisons before the
    /// pushdown, so `IN` is the case that actually reaches the external database as a row value).
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM table WHERE (field, value) IN (('foo', 'bar'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar')))");
    check(state, 1, {"field", "value"},
          "SELECT field, value FROM table WHERE (field, value) IN (('foo', 'bar'), ('x', 'y'))",
          R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IN (('foo', 'bar'), ('x', 'y')))");

    /// In PostgreSQL a row constructor is an ordinary value expression, so there the same
    /// `IS NOT NULL` predicate is pushed down.
    checkOld(state, 1,
             "SELECT field, value FROM table WHERE (field, value) IS NOT NULL",
             R"(SELECT "field", "value" FROM "test"."table" WHERE ("field", "value") IS NOT NULL)",
             LiteralEscapingStyle::PostgreSQL);

    /// A tuple used as the whole condition is never valid SQL for the external database (not even
    /// for PostgreSQL, where `WHERE` requires a boolean and not a record); such a tuple is a list
    /// of predicates in ClickHouse, so it is pushed down as a conjunction instead. Only the old
    /// analyzer accepts a tuple as a filter at all.
    checkOld(state, 1,
             "SELECT a, column FROM table WHERE (a > 0, column > 10)",
             R"(SELECT "column", "a" FROM "test"."table" WHERE ("a" > 0) AND ("column" > 10))");
    checkOld(state, 1,
             "SELECT a, column FROM table WHERE (a > 0, column > 10)",
             R"(SELECT "column", "a" FROM "test"."table" WHERE ("a" > 0) AND ("column" > 10))",
             LiteralEscapingStyle::PostgreSQL);
}

/// Parse a user-provided `(SELECT ...)` table argument of an external database engine / table
/// function and re-serialize it for the external database, the way `StorageMySQL`,
/// `StoragePostgreSQL` and `StorageSQLite` do for a query-backed source.
static String formatQueryTableArgument(
    const State & state,
    const std::string & argument,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style)
{
    ParserSubquery parser;
    ASTPtr ast = parseQuery(parser, argument, 1000, 1000, 1000000);
    auto query = tryGetExternalDatabaseQuery(ast, state.context, identifier_quoting_style, literal_escaping_style);
    EXPECT_TRUE(query.has_value()) << argument;
    return query.value_or("");
}

TEST(TransformQueryForExternalDatabase, QueryTableArgumentForMySQL)
{
    const State & state = State::instance();

    /// The `(SELECT ...)` table argument is re-serialized from the parsed AST and sent to the
    /// external database as is, so ClickHouse-only syntax must not leak into it for MySQL
    /// (`Regular` escaping) either: an explicit `tuple(a, b)` call becomes the row value `(a, b)`,
    /// and a single-row multi-column `IN` set keeps its outer parentheses, exactly as for
    /// PostgreSQL / SQLite.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE tuple(field, value) IN (tuple('foo', 'bar')))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field, value FROM test.`table` WHERE (field, value) IN (('foo', 'bar'))");
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE tuple(field, value) IN (('foo', 'bar')))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field, value FROM test.`table` WHERE (field, value) IN (('foo', 'bar'))");
    /// The same normalization for PostgreSQL, for parity.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE tuple(field, value) IN (tuple('foo', 'bar')))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field, value FROM test."table" WHERE (field, value) IN (('foo', 'bar')))");
    /// A row value is also valid as an operand of a comparison.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE (field, value) = ('foo', 'bar'))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field FROM test.`table` WHERE (field, value) = ('foo', 'bar')");
    /// ... including MySQL's NULL-safe equality `<=>` (`isNotDistinctFrom`).
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE tuple(field, value) <=> tuple('foo', 'bar'))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field FROM test.`table` WHERE (field, value) <=> ('foo', 'bar')");
    /// The internal `_CAST(literal, 'Type')` wrapper that `ConstantNode::toAST` puts around
    /// literals whose type does not survive the text round trip (the analyzer re-serializes the
    /// subquery argument from its query tree) is unwrapped back to the literal instead of being
    /// sent to the external database, which does not know the function.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE (field, value) = _CAST(('foo', 'bar'), 'Tuple(String, String)'))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field FROM test.`table` WHERE (field, value) = ('foo', 'bar')");

    /// Outside a comparison / IN, SQLite and MySQL do not accept the row value `(a, b)` (SQLite
    /// reports "row value misused" for `SELECT (1, 2)`), so a tuple in such a position is rejected
    /// instead of being sent as broken SQL - whether it is an explicit `tuple` call, the
    /// parenthesized form, or the literal the parser folds into a `Tuple` field.
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT tuple(field, value) FROM test.table)",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT (field, value) FROM test.table)",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT ('foo', 'bar') FROM test.table)",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    /// PostgreSQL row constructors are ordinary value expressions, valid in any expression
    /// position - the SELECT list, `IS [NOT] NULL` - so for PostgreSQL such tuples are sent
    /// through as row values instead of being rejected.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT (field, value) FROM test.table)",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT (field, value) FROM test."table")");
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT ('foo', 'bar') FROM test.table)",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT ('foo', 'bar') FROM test."table")");
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE (field, value) IS NOT NULL)",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field FROM test."table" WHERE (field, value) IS NOT NULL)");

    /// Expressions with no MySQL text form are rejected instead of being sent as broken SQL:
    /// `array` / `map` calls and `tuple` with fewer than two arguments ...
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT field FROM test.table WHERE field IN array('foo', 'bar'))",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT field FROM test.table WHERE map('k', 'v') = map('k', 'v'))",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT field FROM test.table WHERE tuple(field) IN (('foo', 'bar')))",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    /// ... as well as the equivalent literals, which the parser folds into `Array` / `Tuple`
    /// fields of a single `ASTLiteral` (for PostgreSQL / SQLite these are rejected at format
    /// time by the dialect field visitors, but the `Regular` style formats them in ClickHouse
    /// syntax without complaint).
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT field FROM test.table WHERE field IN ['foo', 'bar'])",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT a, arr FROM test.table WHERE (a, arr) IN ((1, [1, 2])))",
        IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular));
}

TEST(TransformQueryForExternalDatabase, QueryTableArgumentBooleanPredicate)
{
    const State & state = State::instance();

    /// A tuple in a boolean position - the `WHERE` / `HAVING` of the subquery, or an operand of
    /// `AND` / `OR` / `NOT` - is ClickHouse's list-of-predicates form, not a row value, and no
    /// external database accepts a row value as a condition anyway (PostgreSQL: "argument of
    /// WHERE must be type boolean, not type record"). It is lowered to a conjunction, the same
    /// way the predicate-pushdown path rewrites `WHERE (a > 0, b > 10)`.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE (a > 0, value > 10))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field, value FROM test."table" WHERE (a > 0) AND (value > 10))");
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE tuple(a > 0, value > 10))",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field, value FROM test.`table` WHERE (a > 0) AND (value > 10)");
    /// ... including nested in `AND` / `OR` / `NOT` operands and in `HAVING`.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE (field = 'foo') OR ((a > 0, value > 10)))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field FROM test."table" WHERE (field = 'foo') OR ((a > 0) AND (value > 10)))");
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table GROUP BY field HAVING (count() > 1, a > 0))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field FROM test."table" GROUP BY field HAVING (count() > 1) AND (a > 0))");
    /// A single-predicate `tuple` call is unwrapped to the predicate itself.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table WHERE tuple(a > 0))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field FROM test."table" WHERE a > 0)");
    /// The folded `Tuple` literal carrier (a tuple of constants) is not a list of predicates the
    /// external database could evaluate - it is rejected for every dialect, including PostgreSQL,
    /// whose row constructors are otherwise valid anywhere but not as a condition.
    EXPECT_ANY_THROW(formatQueryTableArgument(state,
        "(SELECT field FROM test.table WHERE ('foo', 'bar'))",
        IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL));
    /// A genuine row value next to a comparison inside the lowered conjunction still works.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table WHERE (a > 0, (field, value) = ('foo', 'bar')))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field, value FROM test."table" WHERE (a > 0) AND ((field, value) = ('foo', 'bar')))");
}

TEST(TransformQueryForExternalDatabase, QueryTableArgumentPrewhere)
{
    const State & state = State::instance();

    /// `PREWHERE` is ClickHouse-only syntax that no external database can parse; for the external
    /// database it is an ordinary filter, so it is lowered into `WHERE` ...
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table PREWHERE field = 'foo')",
            IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular),
        "SELECT field FROM test.`table` WHERE field = 'foo'");
    /// ... merging with an existing `WHERE` via `AND` ...
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field FROM test.table PREWHERE field = 'foo' WHERE value = 'bar')",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field FROM test."table" WHERE (field = 'foo') AND (value = 'bar'))");
    /// ... and the lowered filter is a boolean position: a tuple-of-predicates `PREWHERE`
    /// becomes a conjunction, the same way it does in `WHERE`.
    EXPECT_EQ(
        formatQueryTableArgument(state,
            "(SELECT field, value FROM test.table PREWHERE (a > 0, value > 10))",
            IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::PostgreSQL),
        R"(SELECT field, value FROM test."table" WHERE (a > 0) AND (value > 10))");
}
