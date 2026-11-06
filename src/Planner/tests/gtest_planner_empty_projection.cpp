#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>

#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Common/CurrentThread.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/MapConfiguration.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/KeyDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageView.h>
#include <base/scope_guard.h>

using namespace DB;

namespace DB::Setting
{
extern const SettingsBool analyzer_inline_views;
}

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

constexpr auto database_name = "planner_empty_projection_test_db";
constexpr auto security_none_table_name = "security_none_mt";
constexpr auto security_none_view_name = "security_none_view";

/// Test fixture: a minimal in-memory database with one table.
/// Modelled on src/Storages/tests/gtest_transform_query_for_external_database.cpp.
struct State
{
    State(const State &) = delete;

    ContextMutablePtr context;
    DatabasePtr database;

    static const State & instance()
    {
        static State state;
        return state;
    }

private:
    explicit State()
        : context(Context::createCopy(getContext().context))
        , database(std::make_shared<DatabaseMemory>(database_name, context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        /// Use a unique database name to avoid colliding with other gtests
        /// (e.g. `gtest_transform_query_for_external_database.cpp`) that
        /// register a database called "test" in the process-wide
        /// `DatabaseCatalog`: whichever gtest runs first wins, the other
        /// then fails with "Database test already exists".

        NamesAndTypesList columns{
            {"x", std::make_shared<DataTypeUInt64>()},
            {"y", std::make_shared<DataTypeUInt64>()},
        };
        database->attachTable(
            context,
            "t",
            std::make_shared<StorageMemory>(
                StorageID(database_name, "t"),
                ColumnsDescription{columns},
                ConstraintsDescription{},
                String{},
                MemorySettings{}),
            {});

        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase(database_name);
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

    auto & join_tree = outer->getJoinTreeNode();
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

/// A transparent SQL SECURITY NONE view is inlined by the analyzer. Its distinct canonical
/// root must survive query-tree cloning and Planner handoff to the pipeline, then expire when
/// the final pipeline owner is destroyed.
static void runInlinedSecurityNoneViewSurvivesClonePlanAndPipeline()
{
    std::weak_ptr<Context> view_root_weak;

    {
        const auto & state = State::instance();
        MainThreadStatus::getInstance();
        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        bool table_attached = false;
        bool view_attached = false;
        std::shared_ptr<StorageMergeTree> merge_tree_storage;
        SCOPE_EXIT({
            if (view_attached)
            {
                auto detached_view = state.database->detachTable(state.context, security_none_view_name);
                detached_view.reset();
            }

            if (table_attached)
            {
                auto detached_table = state.database->detachTable(state.context, security_none_table_name);
                detached_table.reset();
            }

            if (merge_tree_storage)
                merge_tree_storage->flushAndShutdown();
        });

        StorageInMemoryMetadata metadata;
        ColumnsDescription merge_tree_columns;
        merge_tree_columns.add(ColumnDescription("x", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(merge_tree_columns);

        auto order_by_ast = makeASTFunction("tuple");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, state.context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, state.context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, state.context);

        auto storage_settings = std::make_unique<MergeTreeSettings>(state.context->getMergeTreeSettings());
        merge_tree_storage = std::make_shared<StorageMergeTree>(
            StorageID(database_name, security_none_table_name),
            "store/planner_query_context_security_none/",
            metadata,
            LoadingStrictnessLevel::ATTACH,
            state.context,
            /* date_column_name */ "",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));
        state.database->attachTable(state.context, security_none_table_name, merge_tree_storage, {});
        table_attached = true;

        ParserCreateQuery create_parser;
        ASTPtr create_ast = parseQuery(
            create_parser,
            "CREATE VIEW planner_empty_projection_test_db.security_none_view SQL SECURITY NONE AS "
            "SELECT x FROM planner_empty_projection_test_db.security_none_mt",
            1000,
            1000,
            1000000);
        auto & create_query = create_ast->as<ASTCreateQuery &>();
        ColumnsDescription view_columns;
        view_columns.add(ColumnDescription("x", std::make_shared<DataTypeUInt64>()));
        auto view_storage = std::make_shared<StorageView>(
            StorageID(database_name, security_none_view_name), create_query, view_columns, String{});
        state.database->attachTable(state.context, security_none_view_name, view_storage, {});
        view_attached = true;

        auto view_metadata = view_storage->getInMemoryMetadataPtr(state.context, false);
        ASSERT_FALSE(view_storage->isSealed(*view_metadata, state.context));

        auto caller_canonical = Context::createCopy(state.context);
        caller_canonical->makeQueryContext();
        caller_canonical->setSetting("analyzer_inline_views", Field(false));
        EXPECT_FALSE(caller_canonical->getSettingsRef()[Setting::analyzer_inline_views]);
        auto planning_context = Context::createCopy(caller_canonical);
        planning_context->setSetting("analyzer_inline_views", Field(true));
        EXPECT_FALSE(caller_canonical->getSettingsRef()[Setting::analyzer_inline_views]);
        EXPECT_TRUE(planning_context->getSettingsRef()[Setting::analyzer_inline_views]);
        ASSERT_NE(planning_context.get(), caller_canonical.get());
        ASSERT_EQ(planning_context->getQueryContext().get(), caller_canonical.get());

        QueryTreeNodePtr cloned_tree;

        {
            ParserSelectQuery parser;
            ASTPtr ast = parseQuery(
                parser,
                "SELECT x FROM planner_empty_projection_test_db.security_none_view",
                1000,
                1000,
                1000000);
            auto query_tree = buildQueryTree(ast, planning_context);

            {
                QueryTreePassManager pass_manager(planning_context);
                addQueryTreePasses(pass_manager);
                pass_manager.run(query_tree);
            }

            const auto * outer_query = query_tree->as<QueryNode>();
            ASSERT_NE(outer_query, nullptr);
            const auto * view_body = outer_query->getJoinTreeNode()->as<QueryNode>();
            ASSERT_NE(view_body, nullptr) << "Expected the SQL SECURITY NONE view body to be inlined";
            const auto * underlying_table = view_body->getJoinTreeNode()->as<TableNode>();
            ASSERT_NE(underlying_table, nullptr) << "Expected an underlying table after view inlining";
            EXPECT_EQ(underlying_table->getStorageID().getDatabaseName(), database_name);
            EXPECT_EQ(underlying_table->getStorageID().getTableName(), security_none_table_name);

            const auto view_context = view_body->getContext();
            ASSERT_TRUE(view_context->isSQLSecurityOverriddenBody());
            auto view_canonical = view_context->getQueryContext();
            ASSERT_NE(view_canonical, nullptr);
            EXPECT_NE(view_canonical.get(), caller_canonical.get());
            EXPECT_TRUE(view_canonical->getSettingsRef()[Setting::analyzer_inline_views]);
            view_root_weak = view_canonical;
            const auto * expected_view_root = view_canonical.get();

            cloned_tree = query_tree->clone();
            const auto * cloned_outer_query = cloned_tree->as<QueryNode>();
            ASSERT_NE(cloned_outer_query, nullptr);
            const auto * cloned_view_body = cloned_outer_query->getJoinTreeNode()->as<QueryNode>();
            ASSERT_NE(cloned_view_body, nullptr);
            const auto * cloned_underlying_table = cloned_view_body->getJoinTreeNode()->as<TableNode>();
            ASSERT_NE(cloned_underlying_table, nullptr);
            EXPECT_EQ(cloned_underlying_table->getStorageID().getTableName(), security_none_table_name);
            EXPECT_EQ(cloned_view_body->getContext()->getQueryContext().get(), expected_view_root);
            EXPECT_TRUE(cloned_view_body->getContext()->getQueryContext()->getSettingsRef()[Setting::analyzer_inline_views]);

            query_tree.reset();
            view_canonical.reset();
            EXPECT_FALSE(view_root_weak.expired())
                << "The cloned inlined query tree must retain its canonical NONE context";
        }

        std::optional<QueryPlan> plan;
        {
            SelectQueryOptions options;
            InterpreterSelectQueryAnalyzer interpreter(cloned_tree, planning_context, options);
            plan.emplace(std::move(interpreter).extractQueryPlan());
        }

        cloned_tree.reset();
        planning_context.reset();
        caller_canonical.reset();
        ASSERT_FALSE(view_root_weak.expired())
            << "The Planner-created QueryPlan must retain the canonical NONE context";

        {
            QueryPipeline pipeline;
            {
                const auto & settings = state.context->getSettingsRef();
                QueryPlanOptimizationSettings optimization_settings(
                    settings,
                    /* max_entries_for_hash_table_stats_ */ 0,
                    state.context->getInitialQueryId(),
                    ExpressionActionsSettings(settings, CompileExpressions::yes),
                    state.context->getPreparedSetsCache(),
                    /* is_parallel_replicas_initiator_with_projection_support_ */ false);
                BuildQueryPipelineSettings build_pipeline_settings(state.context);
                auto builder = plan->buildQueryPipeline(optimization_settings, build_pipeline_settings);
                ASSERT_NE(builder, nullptr);
                pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
            }

            plan.reset();
            EXPECT_FALSE(view_root_weak.expired())
                << "After plan teardown, the built query pipeline must retain the canonical NONE context";
        }

        EXPECT_TRUE(view_root_weak.expired())
            << "Pipeline teardown must release the last owner of the canonical NONE context";
    }
}

TEST(PlannerQueryContextLifetime, InlinedSecurityNoneViewSurvivesClonePlanAndPipeline)
{
    auto * application_before = Poco::Util::Application::instanceRawPtr();
    const auto previous_death_test_style = ::testing::FLAGS_gtest_death_test_style;
    SCOPE_EXIT({ ::testing::FLAGS_gtest_death_test_style = previous_death_test_style; });
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    EXPECT_EXIT(
        {
            if (!Poco::Util::Application::instanceRawPtr())
            {
                getMutableContext().context->setConfig(
                    Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration()));
            }

            {
                /// GoogleTest suppresses child event listeners in a re-exec death test. Replay failures on both
                /// normal return and stack unwinding, before GoogleTest handles an exception from the body.
                SCOPE_EXIT({
                    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
                    if (!test_info)
                    {
                        std::cerr << "No current GoogleTest result in lifetime-test child\n";
                        std::cerr.flush();
                        std::_Exit(1);
                    }

                    const auto * result = test_info->result();
                    for (int i = 0; i < result->total_part_count(); ++i)
                    {
                        const auto & part = result->GetTestPartResult(i);
                        if (!part.failed())
                            continue;

                        std::cerr << (part.file_name() ? part.file_name() : "<unknown>") << ':' << part.line_number()
                                  << ": Failure\n" << part.message() << '\n';
                    }
                    std::cerr.flush();
                });

                runInlinedSecurityNoneViewSurvivesClonePlanAndPipeline();
            }

            /// _Exit intentionally skips LeakSanitizer's exit-time scan and the test main's global pool shutdown.
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        ".*");

    EXPECT_EQ(application_before, Poco::Util::Application::instanceRawPtr());
}
