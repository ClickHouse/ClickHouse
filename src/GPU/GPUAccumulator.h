#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUExpression.cuh>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUUploadPipe.h>
#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB::GPU
{

class GPUAccumulator
{
public:
    GPUAccumulator(
        const IDataType & argument_type,
        const IDataType & result_type,
        GPUAggregationKind aggregation_,
        size_t batch_bytes_,
        std::optional<GPUCodec> codec = {});

    void add(const IColumn & column);

    std::span<char> reserveRaw(size_t max_bytes);
    void commitRaw(size_t bytes);

    /// A compressed block of method `method`, which the device expands with the accumulator's codec, see `deviceCodecOf`.
    void addBlock(UInt8 method, std::string_view payload, size_t decompressed_bytes);

    /// Of a column of values of varying width: a compressed block of the sizes of its rows, whose bytes come through `addBlock`.
    void addSizesBlock(UInt8 method, std::string_view payload, size_t decompressed_bytes);

    Field finalize();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    ColumnUploadPipe & plainPipeOrThrow();
    CompressedUploadPipe & compressedPipeOrThrow();

    void reduceBatchOnDevice();

    const GPUElementType element_type;
    const GPUElementType result_type;
    const DataTypePtr result_data_type;
    const GPUAggregationKind aggregation;
    const size_t batch_bytes;

    std::unique_ptr<CudfReduction> reduction;
    std::unique_ptr<IUploadPipe> pipe;
};


/// A `GROUP BY` on the device over plain columns: the columns of each batch are staged on the host and sent to the
/// device, which groups them through the `CudfGroupBy`. Over compressed blocks, see `GPUCompressedGroupBy`.
///
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes);

    GroupByGPUAccumulator(const GroupByGPUAccumulator &) = delete;
    GroupByGPUAccumulator & operator=(const GroupByGPUAccumulator &) = delete;

    void add(const Columns & key_columns, const Columns & value_columns);

    /// Returns the number of groups.
    size_t finalize();

    /// After `finalize`: keeps the groups for which `expression` holds over the keys and the aggregates' results that
    /// `columns` names, the keys first and then the aggregates. Returns the number of groups kept.
    size_t filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns);

    /// After `finalize`: into a column of the keys each, and a column of the result of each aggregate.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    size_t stagedRows() const { return value_pipes.front().stagedRows(); }
    size_t stagedVariableBytes() const;

    void sendBatchToDevice();

    void groupBatch(const std::vector<DeviceColumnView> & keys, const std::vector<DeviceColumnView> & value_columns);

    const std::vector<GPUElementType> group_keys;
    const std::vector<GPUElementType> key_element_types;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    const size_t batch_rows;

    std::vector<ColumnUploadPipe> key_pipes;
    std::vector<ColumnUploadPipe> value_pipes;

    StreamPtr group_stream;
    std::unique_ptr<CudfGroupBy> group_by;

    std::optional<size_t> num_groups;
};

/// A `GROUP BY` on the device over the compressed blocks of the columns of parts, scheduled as Sirius schedules its
/// pipelines: each reading thread is a worker that does all of a batch itself on a CUDA stream of its own - the upload of
/// its compressed blocks, their expansion by nvcomp straight into the batch's columns, and whatever waits for the device
/// that takes - and then groups the batch in one of a few lanes, each a `CudfGroupBy` on a stream of its own, the first
/// that is free. A thread that waits for the device, in a worker or in cuDF, leaves it to the others, so that no single
/// thread waits for all of the work, as one grouping thread did. The lanes are merged into one by `finalize`.
///
/// The columns are numbered as by `GroupByGPUAccumulator`: the keys, the values, the columns of the `WHERE` that are not
/// a key or a value as well, and then the sizes of each column of varying width, see `offsetsColumnOf`.
class GPUCompressedGroupBy
{
public:
    GPUCompressedGroupBy(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes,
        size_t num_readers,
        const DataTypes & filter_types,
        std::optional<GPUExpression> filter_,
        std::vector<GPUDerivedValue> derived_,
        std::function<void(MutableColumns)> on_collected_,
        std::vector<std::optional<uint32_t>> filter_aliases_);

    ~GPUCompressedGroupBy();

    GPUCompressedGroupBy(const GPUCompressedGroupBy &) = delete;
    GPUCompressedGroupBy & operator=(const GPUCompressedGroupBy &) = delete;

    /// On the thread of reader `reader` only, as each of the following of a reader.
    void addCompressedBlock(
        size_t reader,
        size_t column_index,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes = 0,
        std::optional<size_t> kept_bytes = {});

    std::span<char> reserveRawBytes(size_t reader, size_t column_index, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t reader, size_t column_index, size_t bytes);

    /// The part reader `reader` read ends, of `num_rows` rows.
    void finishPart(size_t reader, size_t num_rows);

    /// The column that the sizes of the column of varying width `column_index` come to.
    size_t offsetsColumnOf(size_t column_index) const;

    /// Once every reader is done. Returns the number of groups.
    size_t finalize();

    size_t filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    /// What a worker staged of a column for its next batch: its compressed blocks, or raw bytes, one run of them.
    struct StagedColumn
    {
        PinnedBuffer staged;
        std::vector<CompressedBlock> blocks;
        std::vector<BitPackingBlock> packed;
        std::optional<GPUCodec> codec;
        bool raw = false;
        /// Of the blocks staged, or the raw bytes: what they expand to, of which the column takes `kept_bytes` after
        /// `skipped_bytes`.
        size_t decompressed_bytes = 0;
        size_t skipped_bytes = 0;
        size_t kept_bytes = 0;
        /// The last block keeps less than the rest of its bytes, so that the next one starts a batch.
        bool cut = false;
    };

    struct Worker
    {
        /// First, so that the buffers of its order, below, go before it.
        StreamPtr stream = createStream();
        std::vector<StagedColumn> staging;
        /// Of each column, what the device expanded that the batches so far did not take: the bytes of a column of a
        /// fixed width or of the bytes of one of varying width, and of a column of sizes, the offsets of the rows left
        /// from 0, one more than the rows.
        std::vector<DeviceBuffer> carried;
        DecompressionBatch decompression;
        EventPtr ready = createEvent();
        /// Of the part being read, the rows grouped.
        size_t rows_grouped = 0;

        Worker() : decompression(cuda::stream_ref{stream.get()}) { }
    };

    struct Lane
    {
        StreamPtr stream = createStream();
        std::unique_ptr<CudfGroupBy> group_by;
        EventPtr consumed = createEvent();
        bool decided = false;
    };

    bool isVariableColumn(size_t column_index) const;
    bool isAliasedColumn(size_t column_index) const;
    std::optional<size_t> variableOrdinalOfOffsetsColumn(size_t column_index) const;
    size_t numColumns() const { return group_keys.size() + values.size() + filter_element_types.size() + variable_column_indices.size(); }
    /// The bytes of a row of a column, of its bytes for one of varying width.
    size_t elementBytesOf(size_t column_index) const;
    GPUElementType elementTypeOf(size_t column_index) const;

    Worker & workerOrThrow(size_t reader);
    /// Whether the worker has staged enough rows of every column for a batch.
    bool batchIsStaged(const Worker & worker) const;

    /// Expands what the worker staged after what it carried, groups the rows every column has, and carries the rest.
    /// At the end of a part, of `part_rows` rows, every row.
    void flush(Worker & worker, std::optional<size_t> part_rows = {});

    void group(
        const std::vector<DeviceColumnView> & keys,
        const std::vector<DeviceColumnView> & value_columns,
        const std::vector<DeviceFixedColumn> & filter_columns,
        cuda::stream_ref worker_stream);
    void flushCollected(Lane & lane);

    const std::vector<GPUElementType> group_keys;
    const std::vector<GPUElementType> key_element_types;
    const std::vector<GPUElementType> values;
    const DataTypes value_data_types;
    const std::function<void(MutableColumns)> on_collected;
    const std::vector<GPUDerivedValue> derived;
    const std::vector<GPUGroupByAggregate> aggregates;
    const std::vector<size_t> variable_column_indices;
    const std::vector<GPUElementType> filter_element_types;
    const std::optional<GPUExpression> filter;
    const std::vector<std::optional<uint32_t>> filter_aliases;
    const size_t batch_rows;

    /// Before the workers, whose batches the lanes may still be grouping when the workers' buffers go.
    std::vector<Lane> lanes;
    ConcurrentBoundedQueue<size_t> free_lanes;

    /// Whether the batches are reduced over their runs, decided by the first batch grouped, see `CudfGroupBy::decided`.
    std::mutex decision_mutex;
    std::optional<bool> reduce_runs;

    std::vector<std::unique_ptr<Worker>> workers;

    std::optional<size_t> num_groups;

    std::atomic<UInt64> lanes_wait_microseconds{0};
    std::atomic<UInt64> device_wait_microseconds{0};
};

}

#endif
