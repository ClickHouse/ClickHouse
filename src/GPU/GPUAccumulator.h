#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>
#include <GPU/GPUExpressionCompiler.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUUploadPipe.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <optional>
#include <vector>

/// No thread waits for another's stream: a stream waits for an event of another, see `GPUDeviceBatch::moveTo`.

namespace DB::GPU
{

/// Freed in the order of the uploader's stream, after the stream given to `moveTo` is done with them.
class GPUDeviceBatch
{
public:
    GPUDeviceBatch(GPUDeviceBatch &&) noexcept = default;
    GPUDeviceBatch & operator=(GPUDeviceBatch &&) noexcept = delete;
    GPUDeviceBatch(const GPUDeviceBatch &) = delete;
    GPUDeviceBatch & operator=(const GPUDeviceBatch &) = delete;
    ~GPUDeviceBatch();

    size_t rows() const { return num_rows; }

    const std::vector<DeviceColumnView> & columns() const { return views; }

    void moveTo(cuda::stream_ref stream);

private:
    friend class GPUBatchUploader;

    GPUDeviceBatch() = default;

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::vector<DeviceColumnBuffer> buffers;
    std::vector<DeviceColumnView> views;
    size_t num_rows = 0;
    EventPtr ready = createEvent();
    EventPtr consumed;
    cudaStream_t consumer = nullptr;
};

class GPUBatchUploader
{
public:
    GPUBatchUploader(const DataTypes & types, size_t batch_bytes);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    bool wouldOverflow(const Columns & columns) const;

    void add(const Columns & columns);

    size_t stagedRows() const { return pipes.front().stagedRows(); }

    std::optional<GPUDeviceBatch> send();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    size_t stagedVariableBytes() const;

    /// Declared before `pipes`, which copy in its order.
    const std::shared_ptr<StreamPtr> stream;
    const size_t batch_rows;
    std::vector<ColumnUploadPipe> pipes;
};

class GPUGroupingLane
{
public:
    GPUGroupingLane(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        std::optional<CompiledGPUExpression> where_,
        size_t num_parts);

    GPUGroupingLane(const GPUGroupingLane &) = delete;
    GPUGroupingLane & operator=(const GPUGroupingLane &) = delete;

    /// The batch's columns: the keys, the values, then the columns of `where`.
    void add(GPUDeviceBatch batch);

    /// Only for a lane that took no batch; `other` must take no more batches.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    size_t finalize();

    /// `columns[i]` is the group column of `having.columns[i]`: a key index, or an aggregate index after the keys.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    const std::vector<GPUElementType> group_keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    /// Declared before `stream`, which is synchronized on destruction: the predicate's kernel must outlive it.
    const std::optional<CompiledGPUExpression> where;

    StreamPtr stream;
    std::unique_ptr<CudfGroupBy> group_by;
    DeviceBuffer kept;

    std::optional<size_t> num_groups;
};

/// Aggregates without `GROUP BY`.
class GPUAccumulator
{
public:
    GPUAccumulator(const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations);

    void add(GPUDeviceBatch batch);

    /// The default of the result type where there were no rows.
    std::vector<Field> finalize();

private:
    const DataTypes result_types;

    StreamPtr reduce_stream;
    std::vector<std::unique_ptr<CudfReduction>> reductions;
};

}

#endif
