#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.cuh>
#include <GPU/GPUUploadPipe.h>
#include <GPU/CudfGroupBy.cuh>
#include <GPU/RecordGroupBy.cuh>
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

    Field finalize();

    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    ColumnUploadPipe & plainPipeOrThrow();

    void reduceBatchOnDevice();

    const GPUElementType element_type;
    const GPUElementType result_type;
    const GPUAggregationKind aggregation;
    const size_t element_size;
    const size_t batch_bytes;

    std::unique_ptr<CudfReduction> reduction;
    std::unique_ptr<IUploadPipe> pipe;
};


class GroupByGPUAccumulator
{
public:
    GroupByGPUAccumulator(
        const DataTypes & key_types,
        const DataTypes & argument_types,
        const DataTypes & result_types,
        const std::vector<GPUAggregationKind> & aggregations,
        size_t batch_bytes,
        bool compressed_ = false,
        size_t num_readers_ = 1,
        const DataTypes & filter_types = {},
        std::optional<GPUFilterProgram> filter_ = {});

    ~GroupByGPUAccumulator();

    GroupByGPUAccumulator(const GroupByGPUAccumulator &) = delete;
    GroupByGPUAccumulator & operator=(const GroupByGPUAccumulator &) = delete;

    void add(const Columns & key_columns, const Columns & value_columns);

    void addCompressedBlock(
        size_t reader, size_t column_index, GPUCodec codec, std::string_view payload, size_t decompressed_bytes, size_t expected_bytes);

    std::span<char> reserveRawBytes(size_t reader, size_t column_index, size_t max_bytes, size_t expected_bytes);
    void commitRawBytes(size_t reader, size_t column_index, size_t bytes);

    void finishPart(size_t reader, size_t num_rows);

    void addVariableOffsets(size_t reader, size_t key_index, std::span<const UInt64> offsets);

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

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

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

        Kind kind = Kind::Finish;
        size_t reader = 0;
        size_t column_index = 0;
        GPUCodec codec = GPUCodec::LZ4;
        PinnedBuffer staged;
        DeviceBuffer on_device;
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
        size_t grouped_rows = 0;
        size_t dropped_rows = 0;
    };

    size_t stagedRows() const { return value_pipes.front().stagedRows(); }
    size_t stagedVariableBytes() const;

    size_t variableOffsetsColumnOf(size_t key_index) const;

    size_t numColumns() const { return group_keys.size() + values.size() + filter_element_types.size() + variable_key_indices.size(); }

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

    void copyVariableGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

    void groupByVariable(const std::vector<DeviceColumnView> & keys, const std::vector<DeviceFixedColumn> & value_columns, size_t num_rows);

    const std::vector<GPUElementType> group_keys;
    const std::vector<GPUElementType> key_element_types;
    const std::vector<size_t> variable_key_indices;
    const std::vector<GPUGroupByValue> values;
    const std::vector<GPUElementType> filter_element_types;
    const std::optional<GPUFilterProgram> filter;
    const size_t batch_rows;
    const bool compressed;

    std::vector<ColumnUploadPipe> key_pipes;
    std::vector<ColumnUploadPipe> value_pipes;

    std::unique_ptr<RecordGroupBy> group_by;
    StreamPtr variable_group_stream;
    std::unique_ptr<CudfGroupBy> variable_group_by;

    std::optional<size_t> num_groups;

    std::vector<Reader> readers;
    AsyncDecompressor decompressor;
    std::vector<DeviceWork> expanding;
    Stopwatch expansion_watch;

    std::vector<DeviceWork> raw_pending;

    struct CopiedRaw
    {
        DeviceWork work;
        EventPtr copied;
    };
    std::vector<CopiedRaw> raw_in_flight;

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
