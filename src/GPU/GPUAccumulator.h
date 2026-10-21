#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypeMapping.h>
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
#include <memory>
#include <condition_variable>
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

    void addBlock(std::string_view payload, size_t decompressed_bytes);

    /// Of a column of values of varying width: a compressed block of the sizes of its rows, whose bytes come through `addBlock`.
    void addSizesBlock(std::string_view payload, size_t decompressed_bytes);

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


/// The device memory that the pieces of a GPU aggregation take between a reader handing one over and the device thread
/// taking it into a column: a reader waits for room before it sends a piece, so the queue holds at most `limit` bytes
/// however many readers there are. A piece larger than the limit is let through alone.
class DeviceStagingBudget
{
public:
    explicit DeviceStagingBudget(size_t limit_) : limit(limit_) { }

    /// Throws once `cancel` was called, by an error of the device thread or the end of the aggregation.
    void acquire(size_t bytes);
    void release(size_t bytes) noexcept;
    void cancel() noexcept;

private:
    std::mutex mutex;
    std::condition_variable room;
    const size_t limit;
    size_t used = 0;
    bool cancelled = false;
};

/// Holds bytes of a `DeviceStagingBudget` until it is destroyed.
class DeviceStagingTicket
{
public:
    DeviceStagingTicket() = default;
    DeviceStagingTicket(DeviceStagingBudget & budget_, size_t bytes_) : budget(&budget_), bytes(bytes_) { budget->acquire(bytes); }
    ~DeviceStagingTicket() { reset(); }

    DeviceStagingTicket(DeviceStagingTicket && other) noexcept : budget(std::exchange(other.budget, nullptr)), bytes(other.bytes) { }
    DeviceStagingTicket & operator=(DeviceStagingTicket && other) noexcept
    {
        if (this != &other)
        {
            reset();
            budget = std::exchange(other.budget, nullptr);
            bytes = other.bytes;
        }
        return *this;
    }

    DeviceStagingTicket(const DeviceStagingTicket &) = delete;
    DeviceStagingTicket & operator=(const DeviceStagingTicket &) = delete;

private:
    void reset() noexcept
    {
        if (budget)
            budget->release(bytes);
        budget = nullptr;
    }

    DeviceStagingBudget * budget = nullptr;
    size_t bytes = 0;
};

class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & value_types,
        std::vector<GPUGroupByAggregate> aggregates_,
        size_t batch_bytes,
        bool compressed_ = false,
        size_t num_readers_ = 1,
        const DataTypes & filter_types = {},
        std::optional<GPUFilterProgram> filter_ = {},
        std::vector<GPUDerivedValue> derived_ = {});

    ~GroupByGPUAccumulator();

    GroupByGPUAccumulator(const GroupByGPUAccumulator &) = delete;
    GroupByGPUAccumulator & operator=(const GroupByGPUAccumulator &) = delete;

    void add(const Columns & key_columns, const Columns & value_columns);

    void addCompressedBlock(
        size_t reader, size_t column_index, GPUCodec codec, std::string_view payload, size_t decompressed_bytes, size_t expected_bytes);

    std::span<char> reserveRawBytes(size_t reader, size_t column_index, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t reader, size_t column_index, size_t bytes);

    void finishPart(size_t reader, size_t num_rows);

    /// Offsets a column of values of varying width - a key or a value - ends its rows at, from where its part starts.
    void addVariableOffsets(size_t reader, size_t column_index, std::span<const UInt64> offsets);

    struct Waits
    {
        UInt64 readers_microseconds = 0;
        UInt64 device_microseconds = 0;
    };

    Waits waits() const
    {
        return {
            .readers_microseconds = readers_wait_microseconds.load(std::memory_order_relaxed),
            .device_microseconds = device_wait_microseconds.load(std::memory_order_relaxed),
        };
    }

    size_t finalize();

    /// After `finalize`: keeps the groups for which `program` holds over `columns`, the keys first and then the
    /// aggregates. Returns how many are kept.
    size_t filterGroups(const GPUFilterProgram & program, const std::vector<uint32_t> & columns);

    /// Into a column of the keys each, and a column of the result of each aggregate.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    struct StagedColumn
    {
        PinnedBuffer staged;
        std::vector<CompressedBlock> blocks;
        std::optional<GPUCodec> codec;
        bool raw = false;
        size_t decompressed_bytes = 0;
    };

    struct DeviceWork
    {
        enum class Kind
        {
            Blocks,
            Raw,
            EndOfPart,
            Finish,
        };

        /// First, so that it is released after the buffers below.
        DeviceStagingTicket ticket;
        Kind kind = Kind::Finish;
        size_t reader = 0;
        size_t column_index = 0;
        GPUCodec codec = GPUCodec::LZ4;
        PinnedBuffer staged;
        DeviceBuffer on_device{GPUResources::get().upload};
        EventPtr uploaded;
        std::vector<CompressedBlock> blocks;
        size_t num_rows = 0;
    };

    struct Reader
    {
        std::vector<StagedColumn> staging;
        std::vector<DeviceFixedColumnBuffer> device_columns;
        std::vector<std::vector<UInt64>> variable_offsets;
        std::vector<DeviceBuffer> piece_offsets;
        std::vector<DeviceBuffer> derived_values;
        size_t grouped_rows = 0;
        size_t dropped_rows = 0;
    };

    size_t stagedRows() const { return value_pipes.front().stagedRows(); }
    size_t stagedVariableBytes() const;

    /// The keys, the values and the columns of the filter come first, then the offsets of each column of varying width.
    bool isVariableColumn(size_t column_index) const;
    size_t variableOffsetsColumnOf(size_t column_index) const;

    size_t numColumns() const { return group_keys.size() + values.size() + filter_element_types.size() + variable_column_indices.size(); }

    std::optional<size_t> variableOrdinalOfOffsetsColumn(size_t column_index) const;

    void sendBatchToDevice();

    Reader & readerOrThrow(size_t reader);

    void handOff(size_t reader, size_t column_index);
    void enqueue(DeviceWork && work);
    void rethrowDeviceError();

    void runDeviceThread();

    void appendRaw(DeviceWork && work);
    void flushPendingRaw(bool wait);
    void freeCopiedRaw(bool wait);

    void startExpansion(std::vector<DeviceWork> && works);
    void finishExpansion();

    bool groupPiece();
    bool hasPieceToGroup() const;

    void finishPartOnDevice(size_t reader, size_t num_rows);
    size_t rowsOnDeviceInEveryColumn(const Reader & reader) const;

    void groupRowsOnDevice(Reader & reader, size_t up_to);

    void dropGroupedRows(Reader & reader);

    void groupBatch(
        const std::vector<DeviceColumnView> & keys,
        const std::vector<DeviceColumnView> & value_columns,
        const std::vector<DeviceFixedColumn> & filter_columns);

    const std::vector<GPUElementType> group_keys;
    const std::vector<GPUElementType> key_element_types;
    /// The columns aggregated, read and sent once each however many aggregates are of them.
    const std::vector<GPUElementType> values;
    /// Computed on the device from `values` before each batch is grouped, and aggregated after them: an aggregate of
    /// column `values.size() + i` is of `derived[i]`.
    const std::vector<GPUDerivedValue> derived;
    const std::vector<GPUGroupByAggregate> aggregates;
    const std::vector<size_t> variable_column_indices;
    const std::vector<GPUElementType> filter_element_types;
    const std::optional<GPUFilterProgram> filter;
    const size_t batch_rows;
    /// The rows a reader gathers on the device before they are grouped: a share of `batch_rows`, so that all the readers
    /// together hold about one batch however many there are.
    const size_t reader_batch_rows;
    const bool compressed;

    std::vector<ColumnUploadPipe> key_pipes;
    std::vector<ColumnUploadPipe> value_pipes;

    StreamPtr group_stream;
    std::unique_ptr<CudfGroupBy> group_by;

    std::optional<size_t> num_groups;

    std::vector<Reader> readers;
    AsyncDecompressor decompressor;
    /// What `decompressor` expands into. Allocated in the order of the compute stream, which reads it.
    DeviceBuffer expanded{GPUResources::get().compute};
    std::vector<DeviceWork> expanding;
    Stopwatch expansion_watch;

    std::vector<DeviceWork> raw_pending;

    struct CopiedRaw
    {
        DeviceWork work;
        EventPtr copied;
    };
    std::vector<CopiedRaw> raw_in_flight;

    DeviceStagingBudget staging_budget;
    std::unique_ptr<ConcurrentBoundedQueue<DeviceWork>> work_queue;

    std::mutex device_error_mutex;
    std::exception_ptr device_error;
    std::atomic<bool> device_failed{false};

    std::atomic<UInt64> readers_wait_microseconds{0};
    std::atomic<UInt64> device_wait_microseconds{0};

    ThreadFromGlobalPool device_thread;
};

}

#endif
