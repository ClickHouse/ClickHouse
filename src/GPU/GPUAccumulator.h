#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypeMapping.h>
#include <GPU/GPUTypes.h>
#include <GPU/GPUUploadPipe.h>
#include <GPU/IGroupBy.h>
#include <GPU/IReduction.h>

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

    /// A batch is staged through host memory in slots of at most this, however large it is on the
    /// device, so that the copy of one slot overlaps with the filling of the next.
    static constexpr size_t max_stage_bytes = 256UL * 1024 * 1024;

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void reduceBatchOnDevice();

    const GPUElementType element_type;
    const GPUElementType result_type;
    const GPUAggregationKind aggregation;
    const size_t element_size;
    const size_t batch_bytes;
    const std::optional<GPUCodec> codec;

    std::unique_ptr<IReduction> reduction;
    UploadPipe pipe;
};


/** A `GROUP BY` over integer keys, batch by batch on the device.
  *
  * Unlike `GPUAccumulator`, the partial result cannot be folded on the host - it is one row per
  * group, not one value - so it stays on the device and each batch is merged into it there.
  *
  * A compressed accumulator takes the columns as compressed blocks, a part at a time per reading
  * thread, and has the device expand them. A reading thread gathers the blocks of each column in
  * pinned memory and hands each filled buffer to a thread of the accumulator's own, which expands
  * it on the device and groups what is ready; so the reading of the next blocks overlaps with the
  * device's work on the last ones. A block of one column covers a different number of rows than
  * a block of another, so the columns arrive on the device unevenly: what is grouped is the rows
  * every column has arrived up to, and the rest waits. `finishPart` groups what the part's last
  * blocks left and checks that every column came to the same number of rows.
  *
  * One reading thread copies into pinned memory slower than the link carries, so several read at
  * once, each a part of its own. Each is a `Reader` here - its staging buffers, its columns on the
  * device and how far they are grouped - and names itself in what it hands over; the groups of
  * every part meet in the one table on the device.
  */
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

    /// A compressed block of the column at `column_index` - the keys first, then the values, then
    /// the columns of the filter - of the part that `reader` is reading.
    void addCompressedBlock(size_t reader, size_t column_index, GPUCodec codec, std::string_view payload, size_t decompressed_bytes);

    /// Room for up to `max_bytes` of values of the column at `column_index`, expanded on the host,
    /// for the reading thread to write straight into and then `commitRawBytes`; as much as the
    /// staging buffer has, at least one byte. A column of a part comes either this way or as
    /// compressed blocks, not both.
    std::span<char> reserveRawBytes(size_t reader, size_t column_index, size_t max_bytes);
    void commitRawBytes(size_t reader, size_t column_index, size_t bytes);

    void finishPart(size_t reader, size_t num_rows);

    size_t finalize();

    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

private:
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    /// Compressed blocks of one column, or its values as they are, gathered in pinned memory by
    /// the reading thread.
    struct StagedColumn
    {
        PinnedBuffer staged;
        std::vector<CompressedBlock> blocks;
        std::optional<GPUCodec> codec;
        bool raw = false;
        size_t decompressed_bytes = 0;
    };

    /// What the reading thread hands the device thread. The blocks are on their way to the device
    /// already: their upload was queued on the copy stream as they were handed over, so that it
    /// runs while the device thread's kernels do, and `uploaded` says when it has landed.
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
        std::optional<DeviceEvent> uploaded;
        std::vector<CompressedBlock> blocks;
        size_t num_rows = 0;
    };

    /// One reading thread's part in flight.
    struct Reader
    {
        std::vector<StagedColumn> staging;
        std::vector<DeviceColumn> device_columns;
        size_t grouped_rows = 0;
        size_t dropped_rows = 0;
    };

    size_t stagedRows() const { return key_pipes.front().stagedRows(); }

    void sendBatchToDevice();

    Reader & readerOrThrow(size_t reader);

    void handOff(size_t reader, size_t column_index);
    void enqueue(DeviceWork && work);
    void rethrowDeviceError();

    void runDeviceThread();

    /// Keeps a run of plain values until its upload has landed, then appends it to its column
    /// through the default stream: waiting for the upload in that stream would hold the kernels
    /// queued behind it. `flushPendingRaw` appends the runs that have landed, in upload order.
    void appendRaw(DeviceWork && work);
    void flushPendingRaw(bool wait);
    void freeCopiedRaw(bool wait);

    /// Queues the expansion of the staged blocks of several buffers on the decompression stream
    /// and returns; `finishExpansion` waits for it, appends the runs to their columns through the
    /// default stream and frees the buffers. One expansion is in flight at a time.
    void startExpansion(std::vector<DeviceWork> && works);
    void finishExpansion();

    /// Groups one piece of some reader's ready rows, at most `group_piece_rows` of them, and
    /// answers whether there was one. The device thread looks at its queue between pieces, so
    /// that the next expansion starts as soon as its blocks arrive and runs while the kernels
    /// over the pieces do.
    bool groupPiece();
    bool hasPieceToGroup() const;

    void finishPartOnDevice(size_t reader, size_t num_rows);
    size_t rowsOnDeviceInEveryColumn(const Reader & reader) const;

    /// Groups the rows of every column of the reader from the first not yet grouped up to
    /// `up_to`, which every column has on the device.
    void groupRowsOnDevice(Reader & reader, size_t up_to);

    void dropGroupedRows(Reader & reader);

    const std::vector<GPUElementType> key_element_types;
    const std::vector<GPUGroupByValue> values;
    /// The columns a `WHERE` evaluated on the device reads, after the keys and the values.
    const std::vector<GPUElementType> filter_element_types;
    const std::optional<GPUFilterProgram> filter;
    const size_t batch_rows;
    const bool compressed;

    std::vector<UploadPipe> key_pipes;
    std::vector<UploadPipe> value_pipes;

    std::unique_ptr<IGroupBy> group_by;

    std::optional<size_t> num_groups;

    std::vector<Reader> readers;
    Decompressor decompressor;
    std::vector<DeviceWork> expanding;
    Stopwatch expansion_watch;

    std::vector<DeviceWork> raw_pending;

    struct CopiedRaw
    {
        DeviceWork work;
        DeviceEvent copied;
    };
    std::vector<CopiedRaw> raw_in_flight;

    std::unique_ptr<ConcurrentBoundedQueue<DeviceWork>> work_queue;

    std::mutex device_error_mutex;
    std::exception_ptr device_error;
    std::atomic<bool> device_failed{false};

    ThreadFromGlobalPool device_thread;
};

}

#endif
