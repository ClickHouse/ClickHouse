#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;
class StorageObjectStorageConfiguration;
using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;

/// Runtime registry of the files read by the main branch of lazy materialization.
/// The main branch's sources register each file when it produces its first chunk and use the
/// index to form the global row index; the lazy branch maps the indexes back to the files.
struct LazyObjectStorageFileRegistry
{
    /// The global row index of a row is (file_index << ROW_INDEX_BITS) | row_index_in_file.
    /// 2^40 rows per file and 2^24 files per query are both far beyond practical limits.
    static constexpr size_t ROW_INDEX_BITS = 40;
    static constexpr UInt64 MAX_ROWS_PER_FILE = 1ul << ROW_INDEX_BITS;
    static constexpr UInt64 ROW_INDEX_MASK = MAX_ROWS_PER_FILE - 1;
    static constexpr UInt64 MAX_FILES = 1ul << (64 - ROW_INDEX_BITS);

    std::mutex mutex;
    std::vector<ObjectInfoPtr> files;

    UInt64 registerFile(const ObjectInfoPtr & object_info);
};

using LazyObjectStorageFileRegistryPtr = std::shared_ptr<LazyObjectStorageFileRegistry>;

/// Shared state between the two branches of lazy materialization for object storage.
struct ObjectStorageLazyMaterializingRows : public ILazyMaterializingRows
{
    struct FileRows
    {
        ObjectInfoPtr object;
        std::shared_ptr<PaddedPODArray<UInt64>> rows;
    };

    /// Filled by filterRangesAndFillRows: the files that contain surviving rows, in the order of
    /// their file index, with the sorted row indexes to read from each of them.
    std::vector<FileRows> rows_in_files;

    LazyObjectStorageFileRegistryPtr file_registry;

    explicit ObjectStorageLazyMaterializingRows(LazyObjectStorageFileRegistryPtr file_registry_);

    void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes) override;
};

using ObjectStorageLazyMaterializingRowsPtr = std::shared_ptr<ObjectStorageLazyMaterializingRows>;

/// The lazy branch of lazy materialization for object storage (see optimizeLazyMaterialization2).
/// Reads the deferred columns for exactly the rows that survived the LIMIT, in the global row
/// index order: files in the order they were registered by the main branch, rows within a file
/// in ascending order (guaranteed by the Parquet reader with `preserve_order`).
class LazilyReadFromObjectStorage final : public ISourceStep
{
public:
    LazilyReadFromObjectStorage(
        SharedHeader header,
        const StorageID & storage_id_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        StorageSnapshotPtr storage_snapshot_,
        const std::optional<DB::FormatSettings> & format_settings_,
        ReadFromFormatInfo info_,
        ContextPtr context_,
        size_t max_block_size_);

    void setLazyMaterializingRows(ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazilyReadFromObjectStorage"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    StorageID storage_id;
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    StorageSnapshotPtr storage_snapshot;
    std::optional<DB::FormatSettings> format_settings;
    ReadFromFormatInfo info;
    ContextPtr context;
    size_t max_block_size;

    ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows;
};

}
