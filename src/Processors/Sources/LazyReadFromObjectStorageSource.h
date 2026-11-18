#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>

namespace DB
{

/// Dynamically creates a reader of the deferred columns of surviving rows from object storage,
/// based on ObjectStorageLazyMaterializingRows. The reader cannot be created in advance because
/// the set of surviving rows becomes known only at run time, after the main branch of the query
/// (with the `LIMIT`) is fully executed. Until then, prepare() reports UpdatePipeline; the
/// pipeline executor calls updatePipeline() when the downstream LazyMaterializingTransform
/// starts pulling from this processor, which happens strictly after it has filled the shared
/// ObjectStorageLazyMaterializingRows state.
class LazyReadFromObjectStorageSource final : public IProcessor
{
public:
    LazyReadFromObjectStorageSource(
        SharedHeader header,
        StorageID storage_id_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        StorageSnapshotPtr storage_snapshot_,
        std::optional<DB::FormatSettings> format_settings_,
        ReadFromFormatInfo info_,
        ContextPtr context_,
        size_t max_block_size_,
        ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyReadFromObjectStorageSource"; }
    Status prepare() override;
    PipelineUpdate updatePipeline() override;

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
