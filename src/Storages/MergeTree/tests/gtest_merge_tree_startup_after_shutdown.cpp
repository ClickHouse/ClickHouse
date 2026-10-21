#include <gtest/gtest.h>

#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <thread>

/// A `StorageMergeTree::startup` that runs after the table was already shut down (e.g. an async startup
/// losing a race with a concurrent `DETACH`) must not leave any background task armed: the first `shutdown`
/// has already run `flushAndPrepareForShutdown`, which never runs again, and the destructor's repeated
/// `shutdown` used to return early, so the re-armed tasks could fire while `~MergeTreeData` destroys the
/// state they touch. The first test runs the whole first `shutdown` before `startup`, which is the losing
/// interleaving; the second one runs them concurrently.

namespace
{

size_t countLiveTasks(DB::BackgroundSchedulePool & pool, const DB::StorageID & storage_id)
{
    size_t live = 0;
    for (const auto & info : pool.getTasks())
        if (info.storage.getFullTableName() == storage_id.getFullTableName() && !info.deactivated)
            ++live;
    return live;
}

void initializeEnvironment()
{
    using namespace DB;

    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();
}

std::shared_ptr<DB::StorageMergeTree> createStorage(DB::ContextMutablePtr context, const DB::StorageID & storage_id)
{
    using namespace DB;

    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);

    auto order_by_ast = makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key.definition_ast = nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(
        ProjectionDescription::getMinMaxCountProjection(columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

    return std::make_shared<StorageMergeTree>(
        storage_id,
        "store/" + storage_id.table_name + "/",
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/"",
        MergeTreeData::MergingParams{},
        std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings()));
}

size_t countLiveTasks(DB::ContextPtr context, const DB::StorageID & storage_id)
{
    return countLiveTasks(*context->getSchedulePool(), storage_id) + countLiveTasks(*context->getStreamingSchedulePool(), storage_id);
}

}

TEST(StorageMergeTree, StartupAfterShutdownArmsNoBackgroundTasks)
{
    using namespace DB;

    initializeEnvironment();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

    const StorageID storage_id("test_db", "test_startup_after_shutdown");
    auto storage = createStorage(context, storage_id);

    /// The complete first shutdown, as done by `DETACH`.
    storage->flushAndShutdown();

    /// The late startup.
    storage->startup();

    EXPECT_EQ(countLiveTasks(context, storage_id), 0u);

    /// The repeated shutdown, as done by the destructor.
    storage->shutdown(false);

    EXPECT_EQ(countLiveTasks(context, storage_id), 0u);
}

/// `startup` and `shutdown` running concurrently, in any interleaving, must leave no task armed. This also
/// covers `startStatisticsCache` reassigning the `refresh_stats_task` holder while `shutdown` deactivates it,
/// which is a data race without `refresh_stats_task_mutex` (visible under TSan).
TEST(StorageMergeTree, ConcurrentStartupAndShutdownArmNoBackgroundTasks)
{
    using namespace DB;

    initializeEnvironment();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

    for (size_t i = 0; i < 20; ++i)
    {
        const StorageID storage_id("test_db", "test_concurrent_startup_shutdown_" + std::to_string(i));
        auto storage = createStorage(context, storage_id);

        std::thread startup_thread([&] { storage->startup(); });
        std::thread shutdown_thread([&] { storage->flushAndShutdown(); });
        startup_thread.join();
        shutdown_thread.join();

        EXPECT_EQ(countLiveTasks(context, storage_id), 0u);

        /// The repeated shutdown, as done by the destructor.
        storage->shutdown(false);

        EXPECT_EQ(countLiveTasks(context, storage_id), 0u);
    }
}
