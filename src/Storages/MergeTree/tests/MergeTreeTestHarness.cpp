#include <Storages/MergeTree/tests/MergeTreeTestHarness.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/CurrentThread.h>

#include <filesystem>
#include <cstring>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeTestHarness
{
using namespace DB;
namespace fs = std::filesystem;

namespace
{
    /// Lazily register a fresh `DiskLocal` for each invocation under a unique name so that
    /// multiple tests in a single process do not collide on data paths.
    DiskPtr getOrCreateTestDisk(ContextMutablePtr context, const std::string & disk_name, const std::string & disk_root)
    {
        return context->getOrCreateDisk(disk_name, [&](const DisksMap &) -> DiskPtr {
            fs::create_directories(disk_root);
            return std::make_shared<DiskLocal>(disk_name, disk_root + "/");
        });
    }

    ASTPtr makePartitionKeyAST(const std::string & column)
    {
        return make_intrusive<ASTIdentifier>(column);
    }
}

TestStorage createStorageWithVectorColumn(
    const std::string & disk_root,
    const std::string & base_relative_path,
    const std::string & vec_column_name,
    size_t dim,
    const std::string & partition_key_column)
{
    /// Boot the thread pools used by `MergeTreeData` loading. Idempotent.
    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto & ch = getMutableContext();
    auto context = Context::createCopy(ch.context);

    /// Stable disk name derived from the directory name keeps `getOrCreateDisk` idempotent
    /// within a test run without polluting other tests.
    const std::string disk_name = "test_disk_" + fs::path(disk_root).filename().string();
    auto disk = getOrCreateTestDisk(context, disk_name, disk_root);
    disk->createDirectories(base_relative_path);

    /// Build the schema: Array(Float32) vector column + UInt32 partition key.
    auto vec_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    auto pk_type = std::make_shared<DataTypeUInt32>();

    ColumnsDescription columns;
    columns.add(ColumnDescription(vec_column_name, vec_type));
    columns.add(ColumnDescription(partition_key_column, pk_type));

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);

    /// ORDER BY tuple() — no sorting key.
    auto order_by_ast = makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_by_ast, metadata.columns, context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, context);
    metadata.primary_key.definition_ast = nullptr;

    /// Partition by the pk column so we can produce two parts with different partition_id.
    auto part_ast = makePartitionKeyAST(partition_key_column);
    metadata.partition_key = KeyDescription::getKeyFromAST(part_ast, metadata.columns, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key_list = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(
        ProjectionDescription::getMinMaxCountProjection(columns, partition_key_list, minmax_columns,
            metadata.primary_key, &metadata.partition_key, context));

    /// MergeTreeSettings: route to our test disk and persist _block_number / _block_offset.
    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    storage_settings->set("disk", Field(disk_name));
    storage_settings->set("enable_block_number_column", Field(UInt64(1)));
    storage_settings->set("enable_block_offset_column", Field(UInt64(1)));

    TestStorage setup;
    setup.disk = disk;
    setup.storage_id = StorageID("test_db", "test_table_" + fs::path(base_relative_path).filename().string());
    setup.vec_column_name = vec_column_name;
    setup.partition_key_column = partition_key_column;
    setup.dim = dim;

    setup.storage = std::make_shared<StorageMergeTree>(
        setup.storage_id,
        base_relative_path + "/",
        metadata,
        LoadingStrictnessLevel::CREATE,
        context,
        /*date_column_name=*/ "",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));

    setup.storage->startup();
    return setup;
}

void insertVectorBlock(
    StorageMergeTreePtr storage,
    const TestStorage & setup,
    size_t num_rows,
    uint32_t partition_value,
    const std::vector<std::vector<float>> & vectors)
{
    if (vectors.size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "insertVectorBlock: vectors.size() ({}) != num_rows ({})",
            vectors.size(), num_rows);

    auto & ch = getMutableContext();
    auto context = Context::createCopy(ch.context);
    auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);

    /// Build the vector column (Array(Float32)) and partition key column (UInt32).
    auto vec_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    auto vec_col = vec_type->createColumn();
    auto & array_col = assert_cast<ColumnArray &>(*vec_col);
    auto & nested_pod = assert_cast<ColumnFloat32 &>(array_col.getData()).getData();
    auto & offsets = array_col.getOffsets();

    for (const auto & v : vectors)
    {
        if (v.size() != setup.dim)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "insertVectorBlock: vector of length {} does not match dim {}", v.size(), setup.dim);
        const size_t old_size = nested_pod.size();
        nested_pod.resize(old_size + v.size());
        std::memcpy(nested_pod.data() + old_size, v.data(), v.size() * sizeof(float));
        offsets.push_back(nested_pod.size());
    }

    auto pk_type = std::make_shared<DataTypeUInt32>();
    auto pk_col = pk_type->createColumn();
    auto & pk_data = assert_cast<ColumnUInt32 &>(*pk_col).getData();
    pk_data.resize_fill(num_rows, partition_value);

    Block block;
    block.insert({std::move(vec_col), vec_type, setup.vec_column_name});
    block.insert({std::move(pk_col), pk_type, setup.partition_key_column});

    /// Drive the insert through a `Chain` that prepends an `AddDeduplicationInfoTransform` to the
    /// sink returned by `storage->write`. Without this transform the sink asserts on a missing
    /// `DeduplicationInfo` chunk-info — the real insert pipeline (`InterpreterInsertQuery`)
    /// always injects it before the sink. Using the single-argument constructor yields a
    /// disabled info (no `insert_dependencies`), which still satisfies the sink's assertion but
    /// skips the real deduplication logic.
    auto sink = storage->write(/*query=*/ nullptr, metadata_snapshot, context,
        /*async_insert=*/ false);
    auto header = sink->getInputs().front().getSharedHeader();

    Chain chain;
    chain.addSink(sink);
    chain.addSource(std::make_shared<AddDeduplicationInfoTransform>(header));

    QueryPipeline pipeline(std::move(chain));
    PushingPipelineExecutor executor(pipeline);
    executor.push(std::move(block));
    executor.finish();
}

}
