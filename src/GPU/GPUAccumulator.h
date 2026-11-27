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

/// The streams of a column of a GPU read, as MergeTree stores it: its values, and of a `String` its bytes and the sizes
/// of its rows, as `string_serialization_version = 'with_size_stream'` writes them.
enum class GPUStream : uint8_t
{
    Values,
    Sizes,
};

/// A column of a batch of a reader on the host: a run of each of its streams.
struct GPUStagedColumn
{
    CompressedHostColumn values;
    /// Of a `String` only.
    CompressedHostColumn sizes;

    void clear();
};

/// A batch of a reader on the host: a run of each column the reader reads.
struct GPUStagedBatch
{
    std::vector<GPUStagedColumn> columns;
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

/// Stages the blocks a reader reads of columns of `column_types` into batches. A batch is finished when every column has
/// about `batch_bytes` of rows, a stream fills its pinned memory, a block cannot follow the staged ones, or a part ends.
class GPUBlockStager
{
public:
    static constexpr size_t max_column_bytes = 32UL << 20;

    GPUBlockStager(std::vector<GPUElementType> column_types_, size_t batch_bytes, std::shared_ptr<GPUStagedBatchPool> pool_);

    /// The stream takes `kept_bytes` of what the block expands to, after `skipped_bytes`.
    void addCompressedBlock(
        size_t column,
        GPUStream stream,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    /// The part ends, of `num_rows` rows: finishes the batch being staged, even an empty one.
    void finishPart(size_t num_rows);

    /// The next finished batch, the oldest first, if there is one.
    std::unique_ptr<GPUStagedBatch> takeFinished();

private:
    CompressedHostColumn & streamOrThrow(size_t column, GPUStream stream);
    bool isFull() const;
    /// Queues the batch being staged as finished, and starts a new one.
    void finishBatch(std::optional<size_t> part_rows = {});

    const std::vector<GPUElementType> column_types;
    const size_t batch_rows;
    const std::shared_ptr<GPUStagedBatchPool> pool;
    std::unique_ptr<GPUStagedBatch> current;
    std::deque<std::unique_ptr<GPUStagedBatch>> finished;
};

/// The columns of a batch on the device, as the reader reads them. Freed in the order of the uploader's stream, after
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
    const std::vector<DeviceColumnView> & columns() const { return column_views; }

    void moveTo(cuda::stream_ref stream);

private:
    friend class GPUBatchUploader;

    explicit GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_);

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::vector<DeviceBuffer> buffers;
    std::vector<DeviceColumnView> column_views;
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
    explicit GPUBatchUploader(std::vector<GPUElementType> column_types_);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// The staged memory is free once it returns, on an exception too, after which the uploader takes no more batches.
    /// Nothing where no row is complete in every column.
    std::optional<GPUDeviceBatch> upload(const GPUStagedBatch & staged);

private:
    /// What the batches so far did not take of a column: its bytes, and of a `String` the offsets of its rows left,
    /// from 0, and the bytes of a size that a block cut, fewer than 8.
    struct Carried
    {
        DeviceBuffer values;
        DeviceBuffer offsets;
        DeviceBuffer size_tail;
    };

    class BatchUpload;
    class ColumnOnDevice;

    std::optional<GPUDeviceBatch> expand(const GPUStagedBatch & staged);

    /// Declared first, so that the buffers below are freed in its order before it goes.
    const std::shared_ptr<StreamPtr> stream;
    const std::vector<GPUElementType> column_types;
    std::vector<Carried> carried;
    DecompressionBatch decompression;
    EventPtr staged_sent = createEvent();
    size_t part_rows_sent = 0;
    bool failed = false;
};

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
