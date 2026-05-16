#include <gtest/gtest.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Test fixture: a minimal in-memory database with one table.
/// Modelled on src/Storages/tests/gtest_transform_query_for_external_database.cpp.
struct State
{
    State(const State &) = delete;

    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

private:
    explicit State()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        DatabasePtr database = std::make_shared<DatabaseMemory>("test", context);

        NamesAndTypesList columns{
            {"x", std::make_shared<DataTypeUInt64>()},
            {"y", std::make_shared<DataTypeUInt64>()},
        };
        database->attachTable(
            context,
            "t",
            std::make_shared<StorageMemory>(
                StorageID("test", "t"),
                ColumnsDescription{columns},
                ConstraintsDescription{},
                String{},
                MemorySettings{}),
            {});

        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase("test");
    }
};

}

/// `ExpressionActions::getSmallestColumn` skips entries flagged as subcolumns
/// when `skip_subcolumns=true` (the default for storage column lists, which
/// can carry meta-subcolumns like `.size0` or `.keys`). For subquery
/// projection columns the call site in `PlannerJoinTree.cpp` now passes
/// `skip_subcolumns=false`: subquery projection entries are full query-level
/// outputs even when they happen to be flagged as subcolumns (for example,
/// `tup.a` produced by `FunctionToSubcolumnsPass`).
///
/// This test exercises the new flag directly: a list of nothing-but-subcolumn
/// entries must return the entry instead of being silently skipped (which
/// would have thrown `LOGICAL_ERROR: No available columns` in the original
/// STID 3938-33a6 reproducer).
TEST(ExpressionActionsGetSmallestColumn, AllSubcolumnsWithoutSkipReturnsColumn)
{
    auto u64 = std::make_shared<DataTypeUInt64>();
    auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{u64, u64});

    NamesAndTypesList all_sub;
    all_sub.emplace_back("tup", "a", tuple_type, u64);
    ASSERT_TRUE(all_sub.front().isSubcolumn());

    NameAndTypePair result = ExpressionActions::getSmallestColumn(all_sub, /*skip_subcolumns=*/false);
    EXPECT_EQ(result.name, "tup.a");
}

/// Focused regression test for the empty-projection guard at
/// `PlannerJoinTree.cpp` (the `if (projection_columns.empty())` branch).
///
/// We construct a fully analyzed query tree for `SELECT 1 FROM (SELECT x FROM t)`
/// — the outer projection references no column of the subquery, so the planner
/// is forced to pick the smallest projection column to read from the subquery.
/// We then simulate the AST-fuzzer-induced corruption that motivated this PR
/// by clearing the subquery's projection columns, and pass the tampered tree
/// directly to `InterpreterSelectQueryAnalyzer` (the query-tree constructor
/// does not re-run analyzer passes).
///
/// Before the guard was added, this hit `ExpressionActions::getSmallestColumn`
/// with an empty list, threw `LOGICAL_ERROR: No available columns`, and
/// `abortOnFailedAssertion` terminated the server in debug or sanitizer
/// builds — the exact crash logged as STID 3938-33a6 in CI stress tests.
/// With the guard in place, planning surfaces a normal query exception with
/// the `UNSUPPORTED_METHOD` code that callers can catch and report.
TEST(PlannerJoinTreeEmptyProjectionGuard, SubqueryWithEmptyProjectionThrowsUnsupportedMethod)
{
    const auto & state = State::instance();

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, "SELECT 1 FROM (SELECT x FROM t)", 1000, 1000, 1000000);

    auto query_tree = buildQueryTree(ast, state.context);
    QueryTreePassManager pass_manager(state.context);
    addQueryTreePasses(pass_manager);
    pass_manager.run(query_tree);

    /// Walk down to the inner subquery and empty its projection list.
    auto * outer = query_tree->as<QueryNode>();
    ASSERT_NE(outer, nullptr) << "Outer node must be a QueryNode";

    auto & join_tree = outer->getJoinTree();
    auto * inner = join_tree->as<QueryNode>();
    ASSERT_NE(inner, nullptr) << "Outer query's join tree must be the inner subquery QueryNode";

    inner->clearProjectionColumns();
    ASSERT_TRUE(inner->getProjectionColumns().empty());

    /// Use the query-tree constructor so no further analyzer passes are run
    /// (re-running passes would re-resolve the projection columns we just cleared).
    SelectQueryOptions options;
    InterpreterSelectQueryAnalyzer interpreter(query_tree, state.context, options);

    try
    {
        interpreter.getQueryPlan();
        FAIL() << "Expected UNSUPPORTED_METHOD exception from planner";
    }
    catch (const Exception & e)
    {
        /// We must see a normal query exception, not a `LOGICAL_ERROR` —
        /// the latter would call `abortOnFailedAssertion` in debug/sanitizer
        /// builds and terminate the server.
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD)
            << "Got error code " << e.code() << " (" << ErrorCodes::getName(e.code()) << ")"
            << "; message: " << e.message();
    }
}
