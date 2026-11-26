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
#include <DataTypes/Serializations/ISerialization.h>
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

/// A stream of a GPU read: a substream of one of its columns, as MergeTree stores it, see `ISerialization::enumerateStreams`.
/// Each is staged, sent and expanded on its own.
struct GPUReadStream
{
    /// The column of the read.
    size_t column;
    ISerialization::SubstreamPath path;
    /// `UInt64` of the sizes of a `String`, `UInt8` of its bytes.
    GPUElementType element_type;
    /// Of the bytes of a `String`: the stream of its sizes.
    std::optional<size_t> sizes;

    bool isSizes() const;
    bool isVariable() const { return sizes.has_value(); }
    size_t elementBytes() const { return sizeOf(element_type); }
};

/// What a GPU read brings: its columns, the keys, then the values, then the columns of the `WHERE` that are neither, and
/// their streams. A `String` is read with its sizes in a stream of their own.
class GPUReadLayout
{
public:
    /// With `collect`, the values are collected rather than aggregated, see `collectElementTypeOf`. `filter_aliases` is
    /// empty, or of each column of the `WHERE`, the key or value it is, if any.
    GPUReadLayout(
        const DataTypes & key_types,
        const DataTypes & value_types,
        bool collect,
        const DataTypes & filter_types,
        const std::vector<std::optional<uint32_t>> & filter_aliases,
        size_t batch_bytes);

    size_t numStreams() const { return streams.size(); }
    /// The stream of the values, or of the bytes, of a column.
    size_t valuesStreamOf(size_t column) const;
    /// The stream of `column` at `path`, as `enumerateStreams` gives it.
    size_t streamOf(size_t column, const ISerialization::SubstreamPath & path) const;

    const std::vector<GPUElementType> keys;
    const std::vector<GPUElementType> values;
    const std::vector<GPUElementType> filter_types;
    /// Of each column of the `WHERE`: the column of the read it is.
    std::vector<size_t> filter_columns;
    std::vector<GPUReadStream> streams;
    size_t batch_rows = 0;

private:
    std::vector<size_t> values_streams;
};

/// A batch of a reader on the host: a run of each stream of the layout.
struct GPUStagedBatch
{
    std::vector<CompressedHostColumn> streams;
    /// Set on the last batch of a part: the rows of the part. The uploader then sends every row it holds.
    std::optional<size_t> part_rows;

    void clear();
};

/// Batches to fill again, so that pinned memory is allocated once per stream.
class GPUStagedBatchPool
{
public:
    std::unique_ptr<GPUStagedBatch> take(size_t num_streams);
    void give(std::unique_ptr<GPUStagedBatch> batch);

private:
    std::mutex mutex;
    std::vector<std::unique_ptr<GPUStagedBatch>> free_batches;
};

/// Stages the blocks a reader reads into batches, by the streams of the layout. A batch is sealed when every stream has
/// `batch_rows` rows, a stream fills its pinned memory, a block cannot follow the staged ones, or a part ends.
class GPUBlockStager
{
public:
    static constexpr size_t max_column_bytes = 32UL << 20;

    GPUBlockStager(std::shared_ptr<const GPUReadLayout> layout_, std::shared_ptr<GPUStagedBatchPool> pool_);

    /// The stream takes `kept_bytes` of what the block expands to, after `skipped_bytes`.
    void addCompressedBlock(
        size_t stream,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    std::span<char> reserveRawBytes(size_t stream, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t stream, size_t bytes);

    /// The part ends, of `num_rows` rows: seals the batch being staged, even an empty one.
    void finishPart(size_t num_rows);

    std::unique_ptr<GPUStagedBatch> takeSealed();

private:
    CompressedHostColumn & streamOrThrow(size_t stream);
    bool isFull() const;
    void seal(std::optional<size_t> part_rows = {});

    const std::shared_ptr<const GPUReadLayout> layout;
    const std::shared_ptr<GPUStagedBatchPool> pool;
    std::unique_ptr<GPUStagedBatch> current;
    std::deque<std::unique_ptr<GPUStagedBatch>> sealed;
};

/// The streams of a batch on the device, as its layout numbers them. Freed in the order of the uploader's stream, after
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

    GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_, std::shared_ptr<const GPUReadLayout> layout_);

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::shared_ptr<const GPUReadLayout> layout;
    std::vector<DeviceBuffer> buffers;
    /// Of each stream, where its rows start; of a stream of sizes, the offsets of the rows, one more than them.
    std::vector<char *> starts;
    /// Of each stream of the bytes of a `String`, those that the rows of the batch take.
    std::vector<uint64_t> variable_bytes;
    size_t num_rows = 0;
    EventPtr ready = createEvent();
    EventPtr consumed;
    cudaStream_t consumer = nullptr;
};

/// Sends the staged batches of one reader to the device and expands them, on a stream of its own. A batch has the rows
/// that every stream has; the rest of each stream waits on the device for the next one.
class GPUBatchUploader
{
public:
    explicit GPUBatchUploader(std::shared_ptr<const GPUReadLayout> layout_);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// The staged memory is free once it returns, on an exception too, after which the uploader takes no more batches.
    /// Nothing where no row is complete in every stream.
    std::optional<GPUDeviceBatch> upload(const GPUStagedBatch & staged);

private:
    std::optional<GPUDeviceBatch> expand(const GPUStagedBatch & staged);

    /// Declared first, so that the buffers below are freed in its order before it goes.
    const std::shared_ptr<StreamPtr> stream;
    const std::shared_ptr<const GPUReadLayout> layout;
    /// Of each stream, what the batches so far did not take: bytes, or of a stream of sizes, the offsets left, from 0.
    std::vector<DeviceBuffer> carried;
    /// Of each stream of sizes, the bytes of a size that a block cut, fewer than 8.
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

    /// Without aggregates: the rows of the batch that the `WHERE` keeps, on the host as columns of `types`, none if it
    /// keeps none.
    MutableColumns filterRows(GPUDeviceBatch batch, const DataTypes & types);

    /// Only for a lane that took no batch; `other` must take no more batches.
    void absorbPart(const GPUGroupingLane & other, size_t part);

    size_t finalize();

    /// `columns[i]` is the group column of `having.columns[i]`: a key index, or an aggregate index after the keys.
    size_t filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns);

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    CudfGroupBy & groupByOrThrow() const;
    std::vector<DeviceColumnView> withComputedArguments(const std::vector<DeviceColumnView> & value_columns, size_t rows);

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
