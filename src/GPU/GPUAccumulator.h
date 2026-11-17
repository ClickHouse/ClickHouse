#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUUploadPipe.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB::GPU
{

/// Sends batches of columns to the device and has a thread of its own process each there, so that the device takes a
/// batch while the next one is staged and sent. The batches go round two slots, each with device columns and a stream
/// of its own: a slot is staged into again once the batch before in it is processed, which the device of its stream
/// waits for, and which the host waits for only to be queued.
class GPUBatchPipeline
{
public:
    /// Called on the thread of the pipeline with the columns of a batch, once they are on the device in the order of
    /// `stream`, which it processes them in.
    using Process = std::function<void(const std::vector<DeviceColumnView> & columns)>;

    GPUBatchPipeline(const DataTypes & types, size_t batch_rows, Process process_, cuda::stream_ref stream_);
    ~GPUBatchPipeline();

    GPUBatchPipeline(const GPUBatchPipeline &) = delete;
    GPUBatchPipeline & operator=(const GPUBatchPipeline &) = delete;

    void add(const Columns & columns);

    size_t stagedRows() const;
    size_t stagedVariableBytes() const;

    /// Hands the batch staged over to the thread.
    void send();

    /// Sends the last batch and waits until every batch is processed.
    void finish();

private:
    struct Slot
    {
        /// First, so that the pipes, which free their device memory in its order, go before it.
        StreamPtr upload = createStream();
        std::vector<ColumnUploadPipe> pipes;
        EventPtr uploaded = createEvent();
        EventPtr processed = createEvent();
    };

    void run();
    void rethrowError();
    void fail(std::exception_ptr error) noexcept;

    const Process process;
    const cuda::stream_ref stream;

    std::vector<Slot> slots;
    std::optional<size_t> current;

    ConcurrentBoundedQueue<size_t> work;
    ConcurrentBoundedQueue<size_t> free_slots;

    std::mutex error_mutex;
    std::exception_ptr error;

    ThreadFromGlobalPool thread;
};


/// Aggregates without `GROUP BY` on the device: each batch of the argument of each aggregate is reduced there.
class GPUAccumulator
{
public:
    GPUAccumulator(
        const DataTypes & argument_types,
        const DataTypes & result_types,
        const std::vector<GPUAggregationKind> & aggregations,
        size_t batch_bytes);

    /// A column of the argument of each aggregate.
    void add(const Columns & columns);

    /// The result of each aggregate, the default of its type where there was no row.
    std::vector<Field> finalize();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    const DataTypes result_types;

    StreamPtr reduce_stream;
    std::vector<std::unique_ptr<CudfReduction>> reductions;
    const size_t batch_rows;
    /// Last, so that its thread is joined before what it uses goes.
    GPUBatchPipeline pipeline;
};


/// A `GROUP BY` on the device: the `CudfGroupBy` groups each batch of the keys and the values into the groups so far.
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes);

    void add(const Columns & key_columns, const Columns & value_columns);

    /// Returns the number of groups.
    size_t finalize();

    /// After `finalize`: into a column of the keys each, and a column of the result of each aggregate.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    const std::vector<GPUElementType> group_keys;
    /// The columns aggregated, sent once each however many aggregates are of them.
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const size_t batch_rows;

    StreamPtr group_stream;
    std::unique_ptr<CudfGroupBy> group_by;

    std::optional<size_t> num_groups;

    /// Last, so that its thread is joined before what it uses goes.
    GPUBatchPipeline pipeline;
};

}

#endif
