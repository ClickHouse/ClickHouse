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

/// A `StorageMergeTree::startup` that runs after the table was already shut down (e.g. an async startup
/// losing a race with a concurrent `DETACH`) must not leave any background task armed: the first `shutdown`
/// has already run `flushAndPrepareForShutdown`, which never runs again, and the destructor's repeated
/// `shutdown` used to return early, so the re-armed tasks could fire while `~MergeTreeData` destroys the
/// state they touch. Here the whole first `shutdown` runs before `startup`, which is the losing interleaving.

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

}

TEST(StorageMergeTree, StartupAfterShutdownArmsNoBackgroundTasks)
{
    using namespace DB;

    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

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

    const StorageID storage_id("test_db", "test_startup_after_shutdown");
    auto storage = std::make_shared<StorageMergeTree>(
        storage_id,
        "store/test_startup_after_shutdown/",
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/"",
        MergeTreeData::MergingParams{},
        std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings()));

    /// The complete first shutdown, as done by `DETACH`.
    storage->flushAndShutdown();

    /// The late startup.
    storage->startup();

    EXPECT_EQ(countLiveTasks(*context->getSchedulePool(), storage_id), 0u);
    EXPECT_EQ(countLiveTasks(*context->getStreamingSchedulePool(), storage_id), 0u);

    /// The repeated shutdown, as done by the destructor.
    storage->shutdown(false);

    EXPECT_EQ(countLiveTasks(*context->getSchedulePool(), storage_id), 0u);
    EXPECT_EQ(countLiveTasks(*context->getStreamingSchedulePool(), storage_id), 0u);
}
