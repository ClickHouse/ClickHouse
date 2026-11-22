#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CudfGroupBy.cuh>
#include <GPU/CudfReduction.cuh>
#include <GPU/GPUDecompression.h>
#include <GPU/GPUExpressionCompiler.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

/// The device side of a GPU read over compressed columns:
///
///     reader -> GPUBlockStager (pinned) -> GPUBatchUploader (to the device, nvcomp) -> GPUGroupingLane ×L -> merge by parts
///
/// Uploaders and lanes have streams of their own. No thread waits for another's stream: a stream waits for an event of
/// another, see `GPUDeviceBatch::moveTo`.

namespace DB::GPU
{

/// The columns a GPU read brings: the keys, the values, the columns of the `WHERE` (not read again if also a key or a
/// value, see `isAliased`), then the sizes of each `String` key or value.
class GPUCompressedLayout
{
public:
    /// With `collect`, the values are collected rather than aggregated, see `collectElementTypeOf`. `filter_aliases` is
    /// empty, or of each column of the `WHERE`, the key or value column it reads, if any.
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
    /// Of a `String` column or of its sizes: which of `variableColumns` it is.
    size_t variableOrdinalOf(size_t column) const;
    const std::vector<size_t> & variableColumns() const { return variable_columns; }

    /// The type of the values of a column: `UInt64` of a column of sizes, `UInt8` of the bytes of a `String`.
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
        std::optional<GPUCodec> codec;
        bool raw = false;
        /// What the blocks, or the raw bytes, expand to; the column takes `kept_bytes` of it after `skipped_bytes`.
        size_t decompressed_bytes = 0;
        size_t skipped_bytes = 0;
        size_t kept_bytes = 0;
        /// The last block keeps less than all of its bytes: the next block starts another batch.
        bool cut = false;

        void clear();
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

    /// A compressed block of `column`, of a method the device expands, see `codecOf`, with `payload` after the header.
    /// The column takes `kept_bytes` of what the block expands to, after `skipped_bytes`.
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

    /// The oldest sealed batch, or nothing.
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

/// The columns of a batch on the device, as its layout numbers them. Freed in the order of the uploader's stream, after
/// the stream given to `moveTo` is done with them.
class GPUDeviceBatch
{
public:
    GPUDeviceBatch(GPUDeviceBatch &&) noexcept = default;
    GPUDeviceBatch & operator=(GPUDeviceBatch &&) noexcept = delete;
    GPUDeviceBatch(const GPUDeviceBatch &) = delete;
    GPUDeviceBatch & operator=(const GPUDeviceBatch &) = delete;
    ~GPUDeviceBatch();

    size_t rows() const { return num_rows; }

    /// A key, a value or a column of the `WHERE` that is not aliased.
    DeviceColumnView column(size_t column) const;
    std::vector<DeviceColumnView> keys() const;
    std::vector<DeviceColumnView> values() const;
    /// Of each column of the `WHERE`, those aliased as the key or value they read.
    std::vector<DeviceFixedColumn> filterColumns() const;

    void moveTo(cuda::stream_ref stream);

private:
    friend class GPUBatchUploader;

    GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_, std::shared_ptr<const GPUCompressedLayout> layout_);

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::shared_ptr<const GPUCompressedLayout> layout;
    std::vector<DeviceBuffer> buffers;
    /// Of each column, where its rows start; of a column of sizes, the offsets of the rows, one more than them.
    std::vector<char *> starts;
    /// Of each `String` column, the bytes of its rows.
    std::vector<uint64_t> variable_bytes;
    size_t num_rows = 0;
    EventPtr ready = createEvent();
    EventPtr consumed;
    cudaStream_t consumer = nullptr;
};

/// Sends the staged batches of one reader to the device and expands them, on a stream of its own. A batch has the rows
/// that every column has; the rest of each column waits on the device for the next one.
class GPUBatchUploader
{
public:
    explicit GPUBatchUploader(std::shared_ptr<const GPUCompressedLayout> layout_);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// The staged memory is free once it returns, on an exception too, after which the uploader takes no more batches.
    /// Nothing where no row is complete in every column.
    std::optional<GPUDeviceBatch> upload(const GPUStagedBatch & staged);

private:
    std::optional<GPUDeviceBatch> expand(const GPUStagedBatch & staged);

    /// Declared first, so that the buffers below are freed in its order before it goes.
    const std::shared_ptr<StreamPtr> stream;
    const std::shared_ptr<const GPUCompressedLayout> layout;
    /// Of each column, what the batches so far did not take: bytes, or of a column of sizes, the offsets of the rows
    /// left, from 0, one more than them.
    std::vector<DeviceBuffer> carried;
    DecompressionBatch decompression;
    EventPtr staged_sent = createEvent();
    /// Of the part being read, the rows sent.
    size_t part_rows_sent = 0;
    bool failed = false;
};

/// A value of a lane that the device computes for each batch from its values, see `GPUDerivedValue`.
struct GPUDerivedLaneValue
{
    CompiledGPUExpression expression;
    /// `columns[i]` is the value the expression reads as its column `i`.
    std::vector<uint32_t> columns;
};

/// A `GROUP BY` on the device, on a CUDA stream of its own. Without aggregates, it collects the values of the rows that
/// the `WHERE` keeps instead.
class GPUGroupingLane
{
public:
    /// The aggregates take the values, then the derived values.
    GPUGroupingLane(
        std::vector<GPUElementType> keys_,
        std::vector<GPUElementType> values_,
        std::vector<GPUGroupByAggregate> aggregates_,
        std::vector<GPUDerivedLaneValue> derived_,
        std::optional<CompiledGPUExpression> where_,
        size_t num_parts);

    GPUGroupingLane(const GPUGroupingLane &) = delete;
    GPUGroupingLane & operator=(const GPUGroupingLane &) = delete;

    cuda::stream_ref stream() const { return cuda::stream_ref{lane_stream.get()}; }

    void add(GPUDeviceBatch batch);

    /// Columns there in the order of `stream`, until it is done with them: the keys, the values without the derived
    /// ones, and the columns of the `WHERE`.
    void add(
        const std::vector<DeviceColumnView> & key_columns,
        const std::vector<DeviceColumnView> & value_columns,
        const std::vector<DeviceFixedColumn> & filter_columns);

    /// Without aggregates: the rows collected and not taken yet.
    size_t collectedRows() const;

    /// Without aggregates: the rows collected so far, copied to columns of `types` on the host.
    MutableColumns takeCollected(const DataTypes & types);

    /// Only for a lane that took no batch; `other` must take no more batches.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    size_t finalize();

    /// `columns[i]` is the group column of `having.columns[i]`: a key index, or an aggregate index after the keys.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    CudfGroupBy & groupByOrThrow() const;
    CudfRowCollector & collectorOrThrow() const;

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    /// Declared before `lane_stream`, which is synchronized on destruction: their kernels must outlive it.
    const std::vector<GPUDerivedLaneValue> derived;
    const std::optional<CompiledGPUExpression> where;

    StreamPtr lane_stream;
    std::unique_ptr<CudfGroupBy> group_by;
    std::unique_ptr<CudfRowCollector> collector;

    std::optional<size_t> num_groups;
};

/// Aggregates without `GROUP BY`, of the values of batches of a layout without keys and without a `WHERE`.
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
