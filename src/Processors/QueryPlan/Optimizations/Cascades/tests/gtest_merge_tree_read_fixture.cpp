/// No `TEST` here; the `gtest` filename prefix is what the `src/CMakeLists.txt` glob picks up.
#include <Processors/QueryPlan/Optimizations/Cascades/tests/gtest_merge_tree_read_fixture.h>

#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

MergeTreeReadFixture::MergeTreeReadFixture(
    const String & table_name, ContextMutablePtr shared_context, bool partition_by_unsorted_column)
    : context(std::move(shared_context))
    , relative_data_path("store/test_cascades_step_identity_" + table_name + "/")
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    /// `getMinMaxCountProjection` below builds `min`/`max`/`count` over the partition key.
    tryRegisterAggregateFunctions();

    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    if (!context)
        context = Context::createCopy(getContext().context);

    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
    if (partition_by_unsorted_column)
        columns.add(ColumnDescription("p", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);

    ASTPtr order_by_ast = make_intrusive<ASTIdentifier>("a");
    metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key = metadata.sorting_key;
    metadata.primary_key.definition_ast = nullptr;
    ASTPtr partition_by_ast = partition_by_unsorted_column ? ASTPtr(make_intrusive<ASTIdentifier>("p")) : nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, metadata.columns, {}, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
        columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    storage = std::make_shared<StorageMergeTree>(
        StorageID("test_cascades_identity", table_name),
        relative_data_path,
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/ "",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));

    /// The handle only converts to a `StorageMetadataPtr` as an lvalue.
    const StorageMetadataHandle metadata_handle = storage->getInMemoryMetadataPtr(context, false);
    metadata_snapshot = metadata_handle;
    storage_snapshot = storage->getStorageSnapshotWithoutData(metadata_snapshot, context);
    data_settings = storage->getSettings();
    parts = std::make_shared<RangesInDataParts>();
}

}
