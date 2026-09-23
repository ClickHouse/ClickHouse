#pragma once

#include <GPU/GPUTypes.h>
#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

/** Reads the columns of whole `MergeTree` parts as the compressed blocks they are stored in and
  * has the device expand and aggregate them, so that the host neither decompresses nor sends the
  * values at full width.
  *
  * Without keys, each column of each part is reduced on its own and the step emits one row per
  * part, in the read's own header; the aggregation above it folds those rows. With keys, every
  * part's rows go through one `GROUP BY` on the device and the step emits one row per group, again
  * in the read's header - the aggregation above then sees one row per key and leaves it as it is.
  */
class ReadFromGPUCompressedColumns : public ISourceStep
{
public:
    struct ColumnToReduce
    {
        NameAndTypePair column;
        DataTypePtr result_type;
        GPU::GPUAggregationKind aggregation;
    };

    /// A `WHERE` the device evaluates before it groups a row, over columns read along with the
    /// keys and the values.
    struct DeviceFilter
    {
        GPU::GPUFilterProgram program;
        std::vector<NameAndTypePair> columns;
        /// The predicate as the plan names it.
        String description;
    };

    ReadFromGPUCompressedColumns(
        SharedHeader output_header_,
        std::vector<NameAndTypePair> keys_,
        std::vector<ColumnToReduce> columns_,
        std::optional<DeviceFilter> filter_,
        DataPartsVector parts_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr context_,
        size_t batch_bytes_,
        size_t num_streams_,
        size_t num_readers_,
        double device_decompression_max_ratio_);

    String getName() const override { return "ReadFromGPUCompressedColumns"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::optional<DeviceFilter> filter;
    DataPartsVector parts;

    StorageSnapshotPtr storage_snapshot;

    ContextPtr context;

    size_t batch_bytes;
    size_t num_streams;
    /// Threads that read parts for a keyed aggregation, each a part at a time.
    size_t num_readers;
    /// A column of a part compressed to at most this fraction of its size is expanded on the
    /// device; one compressed worse is expanded on the CPU and sent as it is.
    double device_decompression_max_ratio;
};

}

#endif
