#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUExpression.cuh>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUMemory.h>
#include <GPU/GPUDecompression.h>
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
    GPUAccumulator(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation_, size_t batch_bytes_);

    void add(const IColumn & column);

    Field finalize();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void reduceBatchOnDevice();

    const GPUElementType element_type;
    const GPUElementType result_type;
    const DataTypePtr result_data_type;
    const GPUAggregationKind aggregation;
    const size_t batch_bytes;

    std::unique_ptr<CudfReduction> reduction;
    ColumnUploadPipe pipe;
};


/// The device side of a GPU read over compressed columns, see `ReadFromGPUCompressedColumns`:
///
///     source -> GPUBlockStager (pinned) -> GPUBatchUploader (H2D, nvcomp) -> GPUGroupingLane ×L -> merge by parts
///
/// Uploaders and lanes have streams of their own; a batch moves between them through events, see `GPUDeviceBatch::moveTo`.

/// The columns a GPU read brings: keys, values, columns of the `WHERE` (not read again if also a key or a value, see
/// `isAliased`), then the sizes of each `String` column.
class GPUCompressedLayout
{
public:
    GPUCompressedLayout(
        const DataTypes & key_types,
        const DataTypes & value_types,
        bool collect,
        const DataTypes & filter_types,
        std::vector<std::optional<uint32_t>> filter_aliases_,
        size_t batch_bytes);

    size_t numColumns() const { return keys.size() + values.size() + filter_types.size() + variable_columns.size(); }
    size_t firstFilterColumn() const { return keys.size() + values.size(); }
    size_t firstSizesColumn() const { return firstFilterColumn() + filter_types.size(); }

    bool isAliased(size_t column) const;
    /// Of an aliased column of the `WHERE`: the key or value column it reads.
    size_t aliasOf(size_t column) const;
    /// A `String` key or value: its bytes. Its sizes are `sizesColumnOf` it.
    bool isVariable(size_t column) const;
    bool isSizes(size_t column) const { return column >= firstSizesColumn() && column < numColumns(); }
    size_t sizesColumnOf(size_t variable_column) const;
    /// Of a `String` column or its sizes: which of `variableColumns` it is.
    size_t variableOrdinalOf(size_t column) const;
    const std::vector<size_t> & variableColumns() const { return variable_columns; }

    /// The type of the values of a column; `UInt64` of a column of sizes, `UInt8` of the bytes of a `String`.
    GPUElementType elementTypeOf(size_t column) const;
    size_t elementBytesOf(size_t column) const { return isVariable(column) ? 1 : sizeOf(elementTypeOf(column)); }

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> key_element_types;
    const std::vector<GPUElementType> values;
    const std::vector<GPUElementType> filter_types;
    const std::vector<std::optional<uint32_t>> filter_aliases;
    const std::vector<size_t> variable_columns;
    /// The rows of a batch the stager aims at.
    const size_t batch_rows;
};

/// One batch of a reader, staged in pinned memory: of each column, its compressed blocks or its raw bytes.
struct GPUStagedBatch
{
    struct Column
    {
        PinnedBuffer staged;
        std::vector<CompressedBlock> blocks;
        std::vector<BitPackingBlock> packed;
        std::optional<GPUCodec> codec;
        bool raw = false;
        /// What the blocks, or the raw bytes, expand to; the column takes `kept_bytes` of it after `skipped_bytes`.
        size_t decompressed_bytes = 0;
        size_t skipped_bytes = 0;
        size_t kept_bytes = 0;
        /// The last block keeps less than all of its bytes: the next block starts another batch.
        bool cut = false;

        void clear();
        bool empty() const { return staged.empty() && blocks.empty(); }
    };

    std::vector<Column> columns;
    /// Set on the last batch of a part: the rows of the part. The uploader then sends every row it holds.
    std::optional<size_t> part_rows;

    void clear();
};

/// Staged batches that an uploader is done with, for its stager to fill again, so that their pinned memory is
/// allocated once per stream rather than once per batch.
class GPUStagedBatchPool
{
public:
    std::unique_ptr<GPUStagedBatch> take(size_t num_columns);
    void give(std::unique_ptr<GPUStagedBatch> batch);

private:
    std::mutex mutex;
    std::vector<std::unique_ptr<GPUStagedBatch>> free_batches;
};

/// Stages the blocks a reader reads into batches. A batch is sealed when every column has `batch_rows` rows, a column
/// fills its pinned memory, a block cannot follow the staged ones, or a part ends.
class GPUBlockStager
{
public:
    /// The pinned memory of a column of a batch, at most.
    static constexpr size_t max_column_bytes = 32UL << 20;

    GPUBlockStager(std::shared_ptr<const GPUCompressedLayout> layout_, std::shared_ptr<GPUStagedBatchPool> pool_);

    /// A compressed block of `column`, of a method the device expands, see `deviceCodecOf`. The column takes
    /// `kept_bytes` of what the block expands to, all of the rest without it, after `skipped_bytes`.
    void addCompressedBlock(
        size_t column,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    /// Room for raw bytes of `column`, to be filled and then committed.
    std::span<char> reserveRawBytes(size_t column, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t column, size_t bytes);

    /// The part ends, of `num_rows` rows: seals the batch being staged, even an empty one.
    void finishPart(size_t num_rows);

    std::unique_ptr<GPUStagedBatch> takeSealed();

private:
    GPUStagedBatch::Column & columnOrThrow(size_t column);
    bool isFull() const;
    void seal(std::optional<size_t> part_rows = {});

    const std::shared_ptr<const GPUCompressedLayout> layout;
    const std::shared_ptr<GPUStagedBatchPool> pool;
    std::unique_ptr<GPUStagedBatch> current;
    std::deque<std::unique_ptr<GPUStagedBatch>> sealed;
};

/// The columns of a batch on the device, which it owns. They are complete in the order of the stream of the uploader
/// that made them, until `moveTo` hands them to another stream.
class GPUDeviceBatch
{
public:
    size_t rows() const { return num_rows; }

    /// A key, a value or a column of the `WHERE` that is not aliased, as the layout numbers them.
    DeviceColumnView column(size_t column) const;
    std::vector<DeviceColumnView> keys() const;
    std::vector<DeviceColumnView> values() const;
    /// Of each column of the `WHERE`, those aliased read as the key or value they are.
    std::vector<DeviceFixedColumn> filterColumns() const;

    /// Makes `stream` wait for the batch, and frees the batch's memory in the order of `stream` from now on.
    void moveTo(cuda::stream_ref stream);

private:
    friend class GPUBatchUploader;

    GPUDeviceBatch(std::shared_ptr<const GPUCompressedLayout> layout_, cuda::stream_ref stream) : layout(std::move(layout_)), ready_stream(stream) { }

    std::shared_ptr<const GPUCompressedLayout> layout;
    cuda::stream_ref ready_stream;
    std::vector<DeviceBuffer> buffers;
    /// Of each column, where its rows start; of a column of sizes, the offsets of the rows, one more than them.
    std::vector<char *> starts;
    /// Of each `String` column, the bytes of its rows.
    std::vector<uint64_t> variable_bytes;
    size_t num_rows = 0;
};

/// Sends the staged batches of one reader to the device and expands them, on a stream of its own. A batch has the rows
/// every column has; the rest of each column waits on the device for the next one.
class GPUBatchUploader
{
public:
    explicit GPUBatchUploader(std::shared_ptr<const GPUCompressedLayout> layout_);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// The staged memory is free once it returns. Nothing where no row is complete in every column.
    /// EXPERIMENT: with `keep_alive` and `GPU_EXP_ASYNC_DEPTH` > 0, it does not wait: `keep_alive` holds the staged memory
    /// until the batch is checked, when `depth` more batches were sent, or at `drain`.
    std::optional<GPUDeviceBatch> upload(const GPUStagedBatch & staged, std::shared_ptr<const void> keep_alive = nullptr);

    /// Waits for and checks every batch in flight.
    void drain();

    ~GPUBatchUploader();

private:
    struct InFlight
    {
        std::vector<std::unique_ptr<DecompressionBatch>> decompressions;
        size_t used = 0;
        std::shared_ptr<const void> keep_alive;
        EventPtr sent = createEvent();
        bool pending = false;
    };

    void finish(InFlight & in_flight);

    const std::shared_ptr<const GPUCompressedLayout> layout;
    /// First, so that the buffers of its order go before it.
    StreamPtr stream = createStream();
    /// Of each column, what the batches so far did not take: bytes, or of a column of sizes, the offsets of the rows
    /// left, from 0, one more than them.
    std::vector<DeviceBuffer> carried;
    std::vector<InFlight> in_flight;
    size_t next_in_flight = 0;
    /// Of the part being read, the rows sent.
    size_t part_rows_sent = 0;
};

/// A lane of a `GROUP BY` on the device: a CUDA stream and the groups of the batches it took. Without aggregates, it
/// collects the rows the `WHERE` keeps instead.
class GPUGroupingLane
{
public:
    /// The partial results of the groups are split by the keys into `num_parts` parts, which lanes of one part merge,
    /// see `absorbPart`. Without aggregates, it collects the rows the `WHERE` keeps instead.
    GPUGroupingLane(
        std::vector<GPUElementType> keys_,
        std::vector<GPUElementType> values_,
        std::vector<GPUGroupByAggregate> aggregates_,
        std::vector<GPUDerivedValue> derived_,
        std::optional<GPUExpression> filter_,
        size_t num_parts);

    GPUGroupingLane(const GPUGroupingLane &) = delete;
    GPUGroupingLane & operator=(const GPUGroupingLane &) = delete;

    cuda::stream_ref stream() const { return cuda::stream_ref{lane_stream.get()}; }

    void add(GPUDeviceBatch batch);
    /// Columns there in the order of `stream`, until it is done with them.
    void add(const std::vector<DeviceColumnView> & key_columns, const std::vector<DeviceColumnView> & value_columns,
        const std::vector<DeviceFixedColumn> & filter_columns);

    size_t collectedRows() const { return collector->collectedRows(); }
    /// The rows collected so far, copied to columns of `types` on the host.
    MutableColumns takeCollected(const DataTypes & types);

    /// Of a lane of one part, which took no batch: takes part `part` of `other`, which takes no more.
    void absorbPart(const GPUGroupingLane & other, size_t part);
    /// EXPERIMENT, see `CudfGroupBy::handsOffParts`.
    bool handsOffParts() const { return group_by && group_by->handsOffParts(); }
    size_t partBytes(size_t part) const { return group_by->partBytes(part); }
    void clearPart(size_t part) { group_by->clearPart(part); }
    /// Returns the number of groups.
    size_t finalize();
    size_t filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns);
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUDerivedValue> derived;
    const std::vector<GPUGroupByAggregate> aggregates;
    const std::optional<GPUExpression> filter;

    /// Before the groups, which are of its order.
    StreamPtr lane_stream = createStream();
    std::unique_ptr<CudfGroupBy> group_by;
    std::unique_ptr<CudfRowCollector> collector;
    std::optional<size_t> num_groups;
};

/// A `GROUP BY` on the device over plain columns, which the CPU read: they are staged and sent to the device a batch at a
/// time, and grouped by a `GPUGroupingLane`. Over compressed blocks, see `GPUBlockStager`.
class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(const DataTypes & key_types, const DataTypes & value_types, std::vector<GPUGroupByAggregate> aggregates_, size_t batch_bytes);

    void add(const Columns & key_columns, const Columns & value_columns);

    /// Returns the number of groups.
    size_t finalize();
    size_t filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns) { return lane.filterGroups(expression, columns); }
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns) { lane.copyGroupsTo(key_columns, aggregate_columns); }

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    size_t stagedRows() const { return value_pipes.front().stagedRows(); }
    size_t stagedVariableBytes() const;
    void sendBatchToDevice();

    const size_t batch_rows;
    std::vector<ColumnUploadPipe> key_pipes;
    std::vector<ColumnUploadPipe> value_pipes;
    GPUGroupingLane lane;
};

}

#endif
