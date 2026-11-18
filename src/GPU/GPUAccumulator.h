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

/// The device side of a GPU aggregation, in pieces that the processors of `GPUAggregatingStep` run each on a thread of
/// the pipeline:
///
///     GPUBatchUploader    columns of the host -> pinned memory -> the device, a CUDA stream per uploader
///     GPUDeviceBatch      the columns of a batch on the device, handed from an uploader's stream to a lane's
///     GPUGroupingLane     a CUDA stream and a `CudfGroupBy`: the groups of the batches it took; several lanes, merged
///                         at the end by parts of the keys (`absorbPart`)
///     GPUAccumulator      without `GROUP BY`: reductions of the batches
///
/// No thread waits for another's stream: a stream waits for an event of another, see `GPUDeviceBatch::moveTo`.

namespace DB::GPU
{

/// The columns of a batch on the device, which it owns. They are there in the order of the stream of the uploader that
/// sent them, and freed in that order, once whoever `moveTo` handed them to is done with them.
class GPUDeviceBatch
{
public:
    GPUDeviceBatch(GPUDeviceBatch &&) noexcept = default;
    GPUDeviceBatch & operator=(GPUDeviceBatch &&) noexcept = delete;
    GPUDeviceBatch(const GPUDeviceBatch &) = delete;
    GPUDeviceBatch & operator=(const GPUDeviceBatch &) = delete;
    ~GPUDeviceBatch();

    size_t rows() const { return num_rows; }

    /// In the order of the columns the uploader takes.
    const std::vector<DeviceColumnView> & columns() const { return views; }

    /// Makes `stream` wait for the batch to be on the device. What `stream` does with it is before the batch is freed.
    void moveTo(cuda::stream_ref stream);

private:
    friend class GPUBatchUploader;

    GPUDeviceBatch() = default;

    /// Shared with the uploader, so that the memory is freed in its order even after it is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::vector<DeviceColumnBuffer> buffers;
    std::vector<DeviceColumnView> views;
    size_t num_rows = 0;
    EventPtr ready = createEvent();
    EventPtr consumed;
    cudaStream_t consumer = nullptr;
};

/// Stages the columns of the blocks of a stream of the pipeline in pinned memory, and sends them to the device as
/// batches, on a CUDA stream of its own, without waiting for the device.
class GPUBatchUploader
{
public:
    /// A batch of `batch_bytes` of the columns, about: of values of varying width, a byte counted per row.
    GPUBatchUploader(const DataTypes & types, size_t batch_bytes);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// Whether `columns` would take the batch staged over its rows or the pinned memory of its values of varying
    /// width: the batch is to be sent first.
    bool wouldOverflow(const Columns & columns) const;

    void add(const Columns & columns);

    size_t stagedRows() const { return pipes.front().stagedRows(); }

    /// The batch staged, sent to the device; nothing without rows.
    std::optional<GPUDeviceBatch> send();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    size_t stagedVariableBytes() const;

    /// First, as the pipes copy in its order.
    const std::shared_ptr<StreamPtr> stream;
    const size_t batch_rows;
    std::vector<ColumnUploadPipe> pipes;
};

/// A lane of a `GROUP BY` on the device: a CUDA stream and the groups of the batches it took, through a `CudfGroupBy`.
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
///
/// With `where`, a predicate the device evaluates over the columns of `where.columns`, which come with each batch after
/// the keys and the values, only the rows it holds for are grouped.
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

    /// The columns of a batch: the keys, the values, then the columns of `where`.
    void add(GPUDeviceBatch batch);

    /// Of a lane of one part, which took no batch: takes part `part` of `other`, which takes no more.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    /// Returns the number of groups.
    size_t finalize();

    /// After `finalize`: keeps the groups for which `having`, a predicate of the device, holds, as a `HAVING` does.
    /// `columns` are of each of `having.columns`, in its order: the index of a key, or of an aggregate after the keys.
    /// Returns the number of groups kept.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    /// After `finalize`: into a column of the keys each, and a column of the result of each aggregate.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    const std::vector<GPUElementType> group_keys;
    /// The columns aggregated, sent once each however many aggregates are of them.
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    /// Before the stream, which is synchronized when it goes, as the kernel of the predicate must stay until then.
    const std::optional<CompiledGPUExpression> where;

    StreamPtr stream;
    std::unique_ptr<CudfGroupBy> group_by;
    /// The bits of the rows of a batch, or of the groups, that a predicate keeps.
    DeviceBuffer kept;

    std::optional<size_t> num_groups;
};

/// Aggregates without `GROUP BY` on the device: each batch of the argument of each aggregate is reduced there.
class GPUAccumulator
{
public:
    GPUAccumulator(const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations);

    /// A column of the argument of each aggregate.
    void add(GPUDeviceBatch batch);

    /// The result of each aggregate, the default of its type where there was no row.
    std::vector<Field> finalize();

private:
    const DataTypes result_types;

    StreamPtr reduce_stream;
    std::vector<std::unique_ptr<CudfReduction>> reductions;
};

}

#endif
