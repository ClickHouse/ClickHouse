#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>
#include <GPU/GPUDeviceBatch.h>
#include <GPU/GPUExpressionCompiler.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <optional>
#include <vector>

/// reader -> GPUBlockStager -> GPUHostBatch -> GPUBatchUploader::sendHostToDevice -> GPUFuture<GPUDeviceBatch>
///     -> GPUGroupingLane ×L -> merge by parts.
/// Uploaders and lanes have streams of their own, which wait for each other by events, see `GPUFuture` and `takeOn`.

namespace DB::GPU
{

/// The columns of a batch that a lane takes, by their positions in it: a column may be both a key and in the `WHERE`.
struct GPUBatchColumns
{
    std::vector<size_t> keys;
    std::vector<size_t> values;
    std::vector<size_t> where;
};

/// An argument of an aggregate that is an expression over the values, such as `l_extendedprice * (1 - l_discount)`,
/// computed by the lane on the device for every batch.
struct GPUComputedArgument
{
    /// Holds the kernel, which must outlive the stream of the lane.
    CompiledGPUExpression expression;
    /// `columns[i]` is the value the expression reads as its column `i`.
    std::vector<uint32_t> columns;
};

/// A `GROUP BY` on the device, on a CUDA stream of its own. Without aggregates, it collects the values of the rows that
/// the `WHERE` keeps instead.
class GPUGroupingLane
{
public:
    /// The batches it takes are of columns of `column_types`. The aggregates take the values, then the computed arguments.
    GPUGroupingLane(
        const std::vector<GPUElementType> & column_types,
        GPUBatchColumns columns_,
        std::vector<GPUGroupByAggregate> aggregates_,
        std::vector<GPUComputedArgument> computed_,
        std::optional<CompiledGPUExpression> where_,
        size_t num_parts);

    GPUGroupingLane(const GPUGroupingLane &) = delete;
    GPUGroupingLane & operator=(const GPUGroupingLane &) = delete;

    cuda::stream_ref stream() const { return cuda::stream_ref{lane_stream.get()}; }

    void add(GPUFuture<GPUDeviceBatch> future);

    /// Without aggregates: the rows of the batch that the `WHERE` keeps, on the host as columns of `types`, none if it
    /// keeps none.
    MutableColumns filterRows(GPUFuture<GPUDeviceBatch> future, const DataTypes & types);

    /// Only for a lane that took no batch; `other` must take no more batches.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    size_t finalize();

    /// `columns[i]` is the group column of `having.columns[i]`: a key index, or an aggregate index after the keys.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    CudfGroupBy & groupByOrThrow() const;
    std::vector<DeviceColumnView> withComputedArguments(const std::vector<DeviceColumnView> & value_columns, size_t rows);

    const GPUBatchColumns batch_columns;
    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    /// Declared before `lane_stream`, which is synchronized on destruction, and `row_filter`, which points into `where`:
    /// their kernels must outlive them.
    const std::vector<GPUComputedArgument> computed;
    const std::optional<CompiledGPUExpression> where;

    StreamPtr lane_stream;
    /// After `lane_stream`, in whose order they are freed.
    std::vector<DeviceBuffer> computed_values;
    std::unique_ptr<CudfGroupBy> group_by;
    std::unique_ptr<CudfRowFilter> row_filter;

    std::optional<size_t> num_groups;
};

/// Aggregates without `GROUP BY`: each of the columns of a batch, in order.
class GPUAccumulator
{
public:
    GPUAccumulator(const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations);

    void add(GPUFuture<GPUDeviceBatch> future);

    /// The default of the result type where there were no rows.
    std::vector<Field> finalize();

private:
    const DataTypes result_types;

    StreamPtr reduce_stream;
    std::vector<std::unique_ptr<CudfReduction>> reductions;
};

}

#endif
