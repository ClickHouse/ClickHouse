#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CompressedHostColumn.h>
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

/// reader -> GPUBlockStager -> GPUBatchUploader -> GPUGroupingLane ×L -> merge by parts. Uploaders and lanes have streams
/// of their own, which wait for each other by events, see `GPUDeviceBatch::moveTo`.

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
    size_t aliasOf(size_t column) const;
    /// A `String` key or value: its bytes. Its sizes are `sizesColumnOf` it.
    bool isVariable(size_t column) const;
    bool isSizes(size_t column) const { return column >= firstSizesColumn() && column < numColumns(); }
    size_t sizesColumnOf(size_t variable_column) const;
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
    const size_t batch_rows;
};

/// A batch of a reader on the host: a run of each column.
struct GPUStagedBatch
{
    std::vector<CompressedHostColumn> columns;
    /// Set on the last batch of a part: the rows of the part. The uploader then sends every row it holds.
    std::optional<size_t> part_rows;

    void clear();
};

/// Batches to fill again, so that pinned memory is allocated once per stream.
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
    static constexpr size_t max_column_bytes = 32UL << 20;

    GPUBlockStager(std::shared_ptr<const GPUCompressedLayout> layout_, std::shared_ptr<GPUStagedBatchPool> pool_);

    /// The column takes `kept_bytes` of what the block expands to, after `skipped_bytes`.
    void addCompressedBlock(
        size_t column,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    std::span<char> reserveRawBytes(size_t column, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t column, size_t bytes);

    /// The part ends, of `num_rows` rows: seals the batch being staged, even an empty one.
    void finishPart(size_t num_rows);

    std::unique_ptr<GPUStagedBatch> takeSealed();

private:
    CompressedHostColumn & columnOrThrow(size_t column);
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

    DeviceColumnView column(size_t column) const;
    std::vector<DeviceColumnView> keys() const;
    std::vector<DeviceColumnView> values() const;
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
    /// What the batches so far did not take: bytes, or of a column of sizes, the offsets left, from 0.
    std::vector<DeviceBuffer> carried;
    /// Of each column of sizes, the bytes of a size that a block cut, fewer than 8.
    std::vector<DeviceBuffer> carried_size_tails;
    DecompressionBatch decompression;
    EventPtr staged_sent = createEvent();
    size_t part_rows_sent = 0;
    bool failed = false;
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
    /// The aggregates take the values, then the computed arguments.
    GPUGroupingLane(
        std::vector<GPUElementType> keys_,
        std::vector<GPUElementType> values_,
        std::vector<GPUGroupByAggregate> aggregates_,
        std::vector<GPUComputedArgument> computed_,
        std::optional<CompiledGPUExpression> where_,
        size_t num_parts);

    GPUGroupingLane(const GPUGroupingLane &) = delete;
    GPUGroupingLane & operator=(const GPUGroupingLane &) = delete;

    cuda::stream_ref stream() const { return cuda::stream_ref{lane_stream.get()}; }

    void add(GPUDeviceBatch batch);

    /// Columns there in the order of `stream` until it is done with them, without the computed arguments.
    void add(
        const std::vector<DeviceColumnView> & key_columns,
        const std::vector<DeviceColumnView> & value_columns,
        const std::vector<DeviceFixedColumn> & filter_columns);

    size_t collectedRows() const;

    MutableColumns takeCollected(const DataTypes & types);

    /// Only for a lane that took no batch; `other` must take no more batches.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    size_t finalize();

    /// `columns[i]` is the group column of `having.columns[i]`: a key index, or an aggregate index after the keys.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    CudfGroupBy & groupByOrThrow() const;
    std::vector<DeviceColumnView> withComputedArguments(const std::vector<DeviceColumnView> & value_columns, size_t rows);
    CudfRowCollector & collectorOrThrow() const;

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUGroupByAggregate> aggregates;
    /// Declared before `lane_stream`, which is synchronized on destruction: their kernels must outlive it.
    const std::vector<GPUComputedArgument> computed;
    const std::optional<CompiledGPUExpression> where;

    StreamPtr lane_stream;
    /// After `lane_stream`, in whose order they are freed.
    std::vector<DeviceBuffer> computed_values;
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
