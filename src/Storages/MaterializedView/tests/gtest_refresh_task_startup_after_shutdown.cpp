#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMemory.h>

using namespace DB;

namespace
{

/// `DatabaseCatalog` is process-wide and shared by every suite in `unit_tests_dbms`, so the
/// database name has to be unique across the whole binary, not just readable here.
constexpr auto database_name = "refresh_task_startup_after_shutdown_db";

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
    State() : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        DatabasePtr database = std::make_shared<DatabaseMemory>(database_name, context);
        const ColumnsDescription columns{{"x", std::make_shared<DataTypeUInt64>()}};
        for (const auto * table_name : {"src", "target"})
            database->attachTable(
                context,
                table_name,
                std::make_shared<StorageMemory>(
                    StorageID(database_name, table_name), columns, ConstraintsDescription{}, String{}, MemorySettings{}),
                {});

        DatabaseCatalog::instance().attachDatabase(database_name, database);
        context->setCurrentDatabase(database_name);
    }
};

/// An explicit `TO` target and `LoadingStrictnessLevel::ATTACH` keep this to a plain constructor
/// call: neither creates an inner table, and a non-Replicated database leaves the refresh
/// uncoordinated, so no Keeper is needed.
std::shared_ptr<StorageMaterializedView> attachRefreshableView(const ContextMutablePtr & context, const String & view_name)
{
    const String db = database_name;
    const String query = "ATTACH MATERIALIZED VIEW " + db + "." + view_name + " REFRESH EVERY 1 YEAR TO " + db
        + ".target AS SELECT x FROM " + db + ".src";

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query, 100000, 1000, 1000000);

    return std::make_shared<StorageMaterializedView>(
        StorageID(database_name, view_name),
        context,
        ast->as<const ASTCreateQuery &>(),
        ColumnsDescription{{"x", std::make_shared<DataTypeUInt64>()}},
        LoadingStrictnessLevel::ATTACH,
        /*comment=*/String{},
        /*is_restore_from_backup=*/false);
}

}

/// shutdown() is documented to be callable before or during startup(), and it nulls the `view`
/// back-pointer that startup() reads, so this order is sanctioned rather than a misuse.
///
/// The two calls are made directly because no query sequence produces that order: ATTACH TABLE
/// builds a fresh RefreshTask, and a database sweep joins the outstanding startup jobs before it
/// begins (DatabaseOnDisk::shutdown() calls stopLoading()).
TEST(RefreshTaskStartupAfterShutdown, StartupAfterShutdownDoesNotDereferenceNullView)
{
    const auto & state = State::instance();

    auto view = attachRefreshableView(state.context, "mv");
    /// Without a refresh task there is no `view` pointer to null and the test below is vacuous.
    ASSERT_TRUE(view->isRefreshable());
    const StorageID view_id = view->getStorageID();

    view->flushAndPrepareForShutdown();
    view->startup();

    /// startup() must also decline to register the task: `RefreshSet` membership is what
    /// `system.view_refreshes` reports and what schedules refreshes, and this view is shut down.
    EXPECT_TRUE(state.context->getRefreshSet().findTasks(view_id).empty());
}
