#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_uk_storage_harness.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB::UniqueKeyTxn::tests
{

UKStorageHarness::UKStorageHarness(const UKStorageHarnessOptions & opts)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    const auto & context_holder = getContext();
    context = Context::createCopy(context_holder.context);

    ColumnsDescription columns;
    columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);

    /// UK table orders by `tuple(a)`; the non-UK marker table keeps an
    /// empty `tuple()` order-by and clears the primary-key definition_ast.
    auto order_by_ast = opts.with_unique_key
        ? makeASTFunction("tuple", make_intrusive<ASTIdentifier>("a"))
        : makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    if (!opts.with_unique_key)
        metadata.primary_key.definition_ast = nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

    if (opts.with_unique_key)
        metadata.unique_key = KeyDescription::getKeyFromAST(
            make_intrusive<ASTIdentifier>("a"), metadata.columns, {}, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(
        ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

    storage = std::make_shared<StorageMergeTree>(
        StorageID("test_db", opts.table_name),
        opts.relative_path,
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/"",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));
}

UKStorageHarness::~UKStorageHarness()
{
    if (storage)
        storage->flushAndShutdown();
}

}
