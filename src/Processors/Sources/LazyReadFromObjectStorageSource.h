#pragma once

#include <Common/ListWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
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
/// The files are read by up to `max_threads` readers, each over a contiguous group of files, and the
/// chunks are returned in the order of the groups, so the rows stay sorted by the global row index.
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
        size_t max_threads_,
        size_t main_read_leaf_columns_,
        ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyReadFromObjectStorageSource"; }
    Status prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & /*updated_output_ports*/) override;
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
    size_t max_threads;
    size_t main_read_leaf_columns;

    ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows;

    /// The chunks of a reader that are pulled before the readers of the previous groups have finished.
    VectorWithMemoryTracking<ListWithMemoryTracking<Chunk>> chunks;
    UnorderedMapWithMemoryTracking<const InputPort *, size_t> input_port_to_index;
    size_t next_chunk_to_process = 0;
    InputPorts::iterator next_input_to_process;
};

}
