#pragma once

#include <GPU/GPUTypes.cuh>
#include "config.h"

#if USE_GPU

#include <Core/SettingsEnums.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

/** Reads the columns of `MergeTree` parts as the compressed blocks they are stored in and has the
  * device expand and aggregate them, so that the host neither decompresses nor sends the values at
  * full width. It reads the ranges of marks the indexes left of a part, whole compressed blocks
  * from the one each range starts in, and the device leaves the rows outside the range out of the
  * blocks it expands.
  *
  * Every part's rows go through one `GROUP BY` on the device and the step takes the place of the
  * aggregation: it emits one row per group in the aggregation's header, the keys and then the
  * result of each aggregate, of which several may be of one column and a `count` of none. Without
  * keys, the rows are one group.
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

    /// An aggregate of a `GROUP BY`: of `columns[column]`, or of no column for a `count`.
    struct AggregateToCompute
    {
        GPU::GPUAggregationKind aggregation;
        std::optional<size_t> column;
        DataTypePtr result_type;
        /// The aggregate as the plan names it.
        String description;
    };

    /// A part to read: the ranges of its marks, or every row where there are none.
    struct PartToRead
    {
        DataPartPtr part;
        MarkRanges ranges;
    };

    ReadFromGPUCompressedColumns(
        SharedHeader output_header_,
        std::vector<NameAndTypePair> keys_,
        std::vector<ColumnToReduce> columns_,
        std::vector<AggregateToCompute> aggregates_,
        std::optional<Columns> result_over_no_rows_,
        std::vector<PartToRead> parts_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr context_,
        size_t batch_bytes_,
        size_t num_readers_,
        GPUDecompressionMode decompression_);

    String getName() const override { return "ReadFromGPUCompressedColumns"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::vector<AggregateToCompute> aggregates;
    /// Of an aggregation without keys, the row it emits where no row is aggregated: none under
    /// `empty_result_for_aggregation_by_empty_set`.
    std::optional<Columns> result_over_no_rows;
    std::vector<PartToRead> parts;

    StorageSnapshotPtr storage_snapshot;

    ContextPtr context;

    size_t batch_bytes;
    /// Threads that read the parts, each a part at a time.
    size_t num_readers;
    /// Who expands a column of a part: see `gpu_aggregation_decompression`.
    GPUDecompressionMode decompression;
};

}

#endif
