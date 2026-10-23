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
  * full width. A keyed aggregation reads the ranges of marks the indexes left of a part, whole
  * compressed blocks from the one each range starts in, and the device leaves the rows outside the
  * range out of the blocks it expands.
  *
  * Without keys, each column of each part is reduced on its own and the step emits one row per
  * part, in the read's own header; the aggregation above it folds those rows. With keys, every
  * part's rows go through one `GROUP BY` on the device and the step takes the place of the
  * aggregation: it emits one row per group in the aggregation's header, the keys and then the
  * result of each aggregate, of which several may be of one column and a `count` of none.
  *
  * Collecting, without keys and aggregates, it takes the place of the read itself: the device evaluates the `WHERE` and
  * sends back only the rows it keeps, of `columns` as they are, the step's header.
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

    /// A `Float64` the device computes from `columns` before it groups the rows, the argument of an aggregate.
    struct DerivedColumn
    {
        GPU::GPUFilterProgram program;
        /// Of `columns`, what the program loads as its column `i`.
        std::vector<size_t> columns;
        /// The expression as the plan names it.
        String description;
    };

    /// An aggregate of a `GROUP BY`: of `columns[column]`, of `derived[derived]`, or of no column for a `count`.
    struct AggregateToCompute
    {
        GPU::GPUAggregationKind aggregation;
        std::optional<size_t> column;
        std::optional<size_t> derived;
        DataTypePtr result_type;
        /// The aggregate as the plan names it.
        String description;
    };

    /// A `HAVING` the device evaluates over the groups before it sends them back: of the step's output columns at
    /// `columns`, the keys first and then the aggregates.
    struct GroupFilter
    {
        GPU::GPUFilterProgram program;
        std::vector<uint32_t> columns;
        /// The predicate as the plan names it.
        String description;
    };

    /// A part to read: the ranges of its marks, or every row where there are none.
    struct PartToRead
    {
        DataPartPtr part;
        MarkRanges ranges;
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
        std::vector<DerivedColumn> derived_,
        std::vector<AggregateToCompute> aggregates_,
        std::optional<DeviceFilter> filter_,
        std::vector<PartToRead> parts_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr context_,
        size_t batch_bytes_,
        size_t num_streams_,
        size_t num_readers_,
        GPUDecompressionMode decompression_,
        double device_decompression_max_ratio_,
        bool collect_ = false);

    String getName() const override { return "ReadFromGPUCompressedColumns"; }

    bool isGrouped() const { return !keys.empty(); }
    bool isCollecting() const { return collect; }

    void setGroupFilter(GroupFilter group_filter_) { group_filter = std::move(group_filter_); }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::vector<DerivedColumn> derived;
    std::vector<AggregateToCompute> aggregates;
    std::optional<DeviceFilter> filter;
    std::optional<GroupFilter> group_filter;
    std::vector<PartToRead> parts;

    StorageSnapshotPtr storage_snapshot;

    ContextPtr context;

    size_t batch_bytes;
    size_t num_streams;
    /// Threads that read parts for a keyed aggregation, each a part at a time.
    size_t num_readers;
    /// Who expands a column of a part: see `gpu_aggregation_decompression`.
    GPUDecompressionMode decompression;
    /// A column of a part compressed to at most this fraction of its size is expanded on the
    /// device; one compressed worse is expanded on the CPU and sent as it is.
    double device_decompression_max_ratio;
    /// The rows the filter keeps are sent back, of `columns`, rather than aggregated.
    bool collect;
};

}

#endif
