#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUDevice.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <numeric>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
    extern const Event GPUGroupByKernelMicroseconds;
    extern const Event GPUDecompressionMicroseconds;
    extern const Event GPUDecompressionBytes;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

GPUAccumulator::GPUAccumulator(
    const IDataType & argument_type,
    const IDataType & result_type_,
    GPUAggregationKind aggregation_,
    size_t batch_bytes_,
    std::optional<GPUCodec> codec_)
    : element_type(reducibleElementTypeOrThrow(argument_type, result_type_, aggregation_))
    , result_type(sumResultTypeFor(element_type))
    , aggregation(aggregation_)
    , element_size(sizeOf(element_type))
    , batch_bytes(std::clamp(batch_bytes_, element_size, max_batch_rows * element_size))
    , codec(codec_)
    , reduction(onDevice(
          [&] { return std::make_unique<CudfReduction>(element_type, result_type, aggregation); },
          "Cannot set up a `{}` of {} on the device",
          aggregationName(aggregation_),
          argument_type.getName()))
    , pipe(argument_type, std::min(batch_bytes, max_stage_bytes), codec.has_value())
{
}

void GPUAccumulator::add(const IColumn & column)
{
    pipe.stage(column);

    if (pipe.stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

std::span<char> GPUAccumulator::reserveRaw(size_t max_bytes)
{
    if (codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain values for an accumulator of compressed blocks");

    return pipe.reserveRaw(max_bytes);
}

void GPUAccumulator::commitRaw(size_t bytes)
{
    pipe.commitRaw(bytes);

    if (pipe.stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::addBlock(std::string_view payload, size_t decompressed_bytes)
{
    if (!codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A compressed block for an accumulator of plain values");

    pipe.stageCompressedBlock(*codec, payload, decompressed_bytes);

    if (pipe.stagedRows() * element_size >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (pipe.stagedRows() == 0)
        return;

    Stopwatch watch;

    const DeviceColumnView values = pipe.flush().view();

    onDevice([&] { reduction->addBatch(values); }, "Cannot reduce {} values by `{}` on a GPU", values.rows, aggregationName(aggregation));

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches, 1);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    pipe.reset();
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

    if (pipe.stagedBytes() != 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A column ended with {} bytes of a {}-byte value",
            pipe.stagedBytes(),
            element_size);

    Stopwatch watch;
    const UInt64 bits = onDevice([&] { return reduction->finalize(); }, "Cannot read a `{}` back from a GPU", aggregationName(aggregation));
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    switch (result_type)
    {
        case GPUElementType::UInt64:
            return Field(bits);
        case GPUElementType::Int64:
            return Field(static_cast<Int64>(bits));
        case GPUElementType::Float64:
            return Field(std::bit_cast<Float64>(bits));
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU reduction into {}, which is not eight bytes wide", result_type);
    }
}

namespace
{

std::vector<GPUGroupByValue> groupByValuesOrThrow(
    const DataTypes & argument_types, const DataTypes & result_types, const std::vector<GPUAggregationKind> & aggregations)
{
    if (argument_types.size() != result_types.size() || argument_types.size() != aggregations.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation of {} arguments, {} result types and {} aggregate functions",
            argument_types.size(),
            result_types.size(),
            aggregations.size());

    std::vector<GPUGroupByValue> values;
    values.reserve(argument_types.size());

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        const GPUElementType element_type = reducibleElementTypeOrThrow(*argument_types[i], *result_types[i], aggregations[i]);

        values.push_back({
            .element_type = element_type,
            .result_type = sumResultTypeFor(element_type),
            .aggregation = aggregations[i],
        });
    }

    return values;
}

size_t rowBytesOf(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUGroupByValue> & values,
    const std::vector<GPUElementType> & filter_element_types)
{
    size_t bytes = 0;
    for (const GPUElementType key_element_type : key_element_types)
        bytes += sizeOf(key_element_type);
    for (const GPUGroupByValue & value : values)
        bytes += sizeOf(value.element_type);
    for (const GPUElementType filter_element_type : filter_element_types)
        bytes += sizeOf(filter_element_type);
    return bytes;
}

std::vector<UploadPipe> pipesFor(const DataTypes & types, size_t batch_rows, bool compressed)
{
    std::vector<UploadPipe> pipes;
    if (compressed)
        return pipes;

    pipes.reserve(types.size());
    for (const auto & type : types)
        pipes.emplace_back(*type, std::min(batch_rows * sizeOf(elementTypeOrThrow(*type)), GPUAccumulator::max_stage_bytes));
    return pipes;
}

std::vector<DeviceColumn> deviceColumnsFor(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUGroupByValue> & values,
    const std::vector<GPUElementType> & filter_element_types,
    bool compressed)
{
    std::vector<DeviceColumn> columns;
    if (!compressed)
        return columns;

    columns.reserve(key_element_types.size() + values.size() + filter_element_types.size());
    for (const GPUElementType key_element_type : key_element_types)
        columns.emplace_back(key_element_type);
    for (const GPUGroupByValue & value : values)
        columns.emplace_back(value.element_type);
    for (const GPUElementType filter_element_type : filter_element_types)
        columns.emplace_back(filter_element_type);
    return columns;
}

/// nvcomp expands each block with one warp, so a call over few blocks leaves the device idle
/// however large they are. Blocks are gathered until they expand to this much and are expanded in
/// one call: a block holds a megabyte of values, so this is also how many warps the call keeps busy.
constexpr size_t compressed_stage_bytes = 256UL * 1024 * 1024;

/// How many filled staging buffers may wait for the device thread, per column: a part's end hands
/// over every column's buffer at once, and the reading thread must not have to wait for the device
/// to take them before it goes on to the next part.
constexpr size_t queued_buffers_per_column = 2;

/// How many rows the device thread groups before it looks at its queue again.
constexpr size_t group_piece_rows = 4UL << 20;

}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations,
    size_t batch_bytes,
    bool compressed_,
    size_t num_readers_,
    const DataTypes & filter_types,
    std::optional<GPUFilterProgram> filter_)
    : key_element_types(elementTypesOrThrow(key_types))
    , values(groupByValuesOrThrow(argument_types, result_types, aggregations))
    , filter_element_types(elementTypesOrThrow(filter_types))
    , filter(std::move(filter_))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, filter_element_types), size_t{1}, max_batch_rows))
    , compressed(compressed_)
    , key_pipes(pipesFor(key_types, batch_rows, compressed))
    , value_pipes(pipesFor(argument_types, batch_rows, compressed))
    , group_by(onDevice(
          [&] { return std::make_unique<RecordGroupBy>(key_element_types, values); },
          "Cannot set up a `GROUP BY` over {} keys on the device",
          key_element_types.size()))
{
    if (filter.has_value() != !filter_element_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with a `WHERE` and the columns of one do not go together");

    if (filter && filter->num_columns != filter_element_types.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation's `WHERE` reads {} columns and is given {}",
            filter->num_columns,
            filter_element_types.size());

    if (!compressed)
    {
        if (filter)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over plain columns with a `WHERE` for the device");
        return;
    }

    if (num_readers_ == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed blocks with no reading threads");

    const size_t num_columns = key_element_types.size() + values.size() + filter_element_types.size();
    readers.reserve(num_readers_);
    for (size_t i = 0; i < num_readers_; ++i)
    {
        Reader & reader = readers.emplace_back();
        reader.staging.resize(num_columns);
        reader.device_columns = deviceColumnsFor(key_element_types, values, filter_element_types, compressed);
    }

    work_queue = std::make_unique<ConcurrentBoundedQueue<DeviceWork>>(queued_buffers_per_column * (num_columns + 1) * num_readers_);

    device_thread = ThreadFromGlobalPool([this, thread_group = CurrentThread::getGroup()]
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::GPU_GROUP_BY);
        runDeviceThread();
    });
}

GroupByGPUAccumulator::~GroupByGPUAccumulator()
{
    if (!device_thread.joinable())
        return;

    try
    {
        work_queue->finish();
        device_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of plain columns for a GPU aggregation over compressed blocks");

    if (key_columns.size() != key_pipes.size() || value_columns.size() != value_pipes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block of {} key and {} value columns for a GPU aggregation over {} and {}",
            key_columns.size(),
            value_columns.size(),
            key_pipes.size(),
            value_pipes.size());

    const size_t num_rows = key_columns.front()->size();
    if (num_rows == 0)
        return;

    if (stagedRows() + num_rows > max_batch_rows)
        sendBatchToDevice();

    for (size_t i = 0; i < key_columns.size(); ++i)
        key_pipes[i].stage(*key_columns[i]);

    for (size_t i = 0; i < value_columns.size(); ++i)
        value_pipes[i].stage(*value_columns[i]);

    if (stagedRows() >= batch_rows)
        sendBatchToDevice();
}

void GroupByGPUAccumulator::sendBatchToDevice()
{
    const size_t num_rows = stagedRows();
    if (num_rows == 0)
        return;

    const auto flushed = [](std::vector<UploadPipe> & pipes)
    {
        std::vector<DeviceColumnView> columns;
        columns.reserve(pipes.size());
        for (auto & pipe : pipes)
            columns.push_back(pipe.flush().view());
        return columns;
    };

    Stopwatch watch;

    const std::vector<DeviceColumnView> keys = flushed(key_pipes);
    const std::vector<DeviceColumnView> value_columns = flushed(value_pipes);

    const double kernel_microseconds
        = onDevice([&] { return group_by->addBatch(keys, value_columns, {}, nullptr); }, "Cannot group {} rows on the device", num_rows);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUGroupByKernelMicroseconds, static_cast<UInt64>(kernel_microseconds));

    for (auto & pipe : key_pipes)
        pipe.reset();
    for (auto & pipe : value_pipes)
        pipe.reset();
}

GroupByGPUAccumulator::Reader & GroupByGPUAccumulator::readerOrThrow(size_t reader)
{
    if (!compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A compressed block for a GPU aggregation over plain columns");

    if (reader >= readers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reader {} of a GPU aggregation with {} readers", reader, readers.size());

    return readers[reader];
}

void GroupByGPUAccumulator::addCompressedBlock(
    size_t reader_index, size_t column_index, GPUCodec codec, std::string_view payload, size_t decompressed_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    if (column_index >= reader.staging.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation over {} columns", column_index, reader.staging.size());

    rethrowDeviceError();

    StagedColumn & column = reader.staging[column_index];

    if (column.raw || (!column.blocks.empty() && (column.decompressed_bytes + decompressed_bytes > compressed_stage_bytes || column.codec != codec)))
        handOff(reader_index, column_index);

    if (column.blocks.empty())
        column.staged.reserve(compressed_stage_bytes);

    column.codec = codec;
    column.blocks.push_back({
        .offset = column.staged.size(),
        .compressed_bytes = payload.size(),
        .decompressed_bytes = decompressed_bytes,
    });
    column.staged.append(payload);
    column.decompressed_bytes += decompressed_bytes;
}

std::span<char> GroupByGPUAccumulator::reserveRawBytes(size_t reader_index, size_t column_index, size_t max_bytes)
{
    Reader & reader = readerOrThrow(reader_index);

    if (column_index >= reader.staging.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU aggregation over {} columns", column_index, reader.staging.size());

    rethrowDeviceError();

    StagedColumn & column = reader.staging[column_index];

    if (!column.blocks.empty() || column.staged.size() >= compressed_stage_bytes)
        handOff(reader_index, column_index);

    column.raw = true;
    column.staged.reserve(compressed_stage_bytes);
    return {column.staged.data() + column.staged.size(), std::min(max_bytes, compressed_stage_bytes - column.staged.size())};
}

void GroupByGPUAccumulator::commitRawBytes(size_t reader_index, size_t column_index, size_t bytes)
{
    StagedColumn & column = readerOrThrow(reader_index).staging[column_index];
    column.staged.grow(bytes);
    column.decompressed_bytes += bytes;

    if (column.staged.size() >= compressed_stage_bytes)
        handOff(reader_index, column_index);
}

void GroupByGPUAccumulator::handOff(size_t reader_index, size_t column_index)
{
    StagedColumn & column = readers[reader_index].staging[column_index];
    if (column.staged.empty())
        return;

    DeviceWork work;
    work.kind = column.raw ? DeviceWork::Kind::Raw : DeviceWork::Kind::Blocks;
    work.reader = reader_index;
    work.column_index = column_index;
    if (!column.raw)
        work.codec = *column.codec;
    work.staged = std::move(column.staged);
    work.blocks = std::move(column.blocks);

    work.on_device = DeviceBuffer(StreamRegistry::get().upload);
    work.on_device.append(work.staged.bytes());
    work.uploaded.emplace();
    work.uploaded->record(StreamRegistry::get().upload);

    column.staged = PinnedBuffer{};
    column.blocks = {};
    column.codec.reset();
    column.raw = false;
    column.decompressed_bytes = 0;

    enqueue(std::move(work));
}

void GroupByGPUAccumulator::enqueue(DeviceWork && work)
{
    rethrowDeviceError();

    if (!work_queue->push(std::move(work)))
    {
        rethrowDeviceError();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The GPU aggregation's device thread stopped taking work without an error");
    }
}

void GroupByGPUAccumulator::rethrowDeviceError()
{
    if (!device_failed.load(std::memory_order_acquire))
        return;

    std::lock_guard lock(device_error_mutex);
    std::rethrow_exception(device_error);
}

void GroupByGPUAccumulator::finishPart(size_t reader_index, size_t num_rows)
{
    Reader & reader = readerOrThrow(reader_index);

    for (size_t i = 0; i < reader.staging.size(); ++i)
        handOff(reader_index, i);

    DeviceWork work;
    work.kind = DeviceWork::Kind::EndOfPart;
    work.reader = reader_index;
    work.num_rows = num_rows;
    enqueue(std::move(work));
}

void GroupByGPUAccumulator::runDeviceThread()
{
    try
    {
        /// A buffer of blocks of another codec than those taken with it, kept for the next round.
        std::optional<DeviceWork> carried;

        while (true)
        {
            std::vector<DeviceWork> blocks;
            std::optional<DeviceWork> other;

            flushPendingRaw(/*wait=*/false);

            /// Only an idle thread waits for work: one with an expansion in flight, an upload
            /// still landing or rows to group looks and goes on.
            const bool idle = expanding.empty() && raw_pending.empty() && !carried && !hasPieceToGroup();
            DeviceWork work;
            bool taken = false;
            if (carried)
            {
                work = std::move(*carried);
                carried.reset();
                taken = true;
            }
            else
            {
                taken = idle ? work_queue->pop(work) : work_queue->tryPop(work);
                if (!taken && idle)
                    return;
            }

            while (taken)
            {
                if (work.kind != DeviceWork::Kind::Blocks || (!blocks.empty() && work.codec != blocks.front().codec))
                {
                    other = std::move(work);
                    break;
                }
                blocks.push_back(std::move(work));
                taken = work_queue->tryPop(work);
            }

            if (!blocks.empty())
            {
                finishExpansion();
                startExpansion(std::move(blocks));
            }

            if (other)
            {
                switch (other->kind)
                {
                    case DeviceWork::Kind::Blocks:
                        carried = std::move(*other);
                        break;
                    case DeviceWork::Kind::Raw:
                        appendRaw(std::move(*other));
                        break;
                    case DeviceWork::Kind::EndOfPart:
                        finishPartOnDevice(other->reader, other->num_rows);
                        break;
                    case DeviceWork::Kind::Finish:
                        finishExpansion();
                        flushPendingRaw(/*wait=*/true);
                        while (groupPiece())
                        {
                        }
                        freeCopiedRaw(/*wait=*/true);
                        return;
                }
                continue;
            }

            if (groupPiece())
                continue;

            if (!expanding.empty())
            {
                finishExpansion();
                continue;
            }

            /// Nothing to do until an upload lands.
            flushPendingRaw(/*wait=*/!raw_pending.empty());
        }
    }
    catch (...)
    {
        try
        {
            flushPendingRaw(/*wait=*/true);
            freeCopiedRaw(/*wait=*/true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        {
            std::lock_guard lock(device_error_mutex);
            device_error = std::current_exception();
        }
        device_failed.store(true, std::memory_order_release);
        work_queue->finish();
    }
}

void GroupByGPUAccumulator::appendRaw(DeviceWork && work)
{
    const size_t num_columns = key_element_types.size() + values.size() + filter_element_types.size();
    if (work.reader >= readers.size() || work.column_index >= num_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A buffer of column {} from reader {} on the GPU aggregation's device thread", work.column_index, work.reader);

    raw_pending.push_back(std::move(work));
}

void GroupByGPUAccumulator::flushPendingRaw(bool wait)
{
    freeCopiedRaw(/*wait=*/false);

    /// The uploads run on one stream, so they land in the order they were queued, which is the
    /// order of a column's runs.
    size_t landed = 0;
    for (; landed < raw_pending.size(); ++landed)
    {
        DeviceWork & work = raw_pending[landed];
        if (wait)
            work.uploaded->wait();
        else if (!work.uploaded->isComplete())
            break;

        /// The buffer the run came in is freed on the copy stream, so it is kept until the copy
        /// out of it has run.
        const size_t bytes = work.on_device.size();
        DeviceColumn & column = readers[work.reader].device_columns[work.column_index];
        checkCuda(
            cudaMemcpyAsync(column.grow(bytes), work.on_device.data(), bytes, cudaMemcpyDeviceToDevice, StreamRegistry::get().compute),
            "Cannot append {} plain bytes to a device column",
            bytes);

        work.staged = PinnedBuffer{};
        CopiedRaw copied{.work = std::move(work), .copied = DeviceEvent{}};
        copied.copied.record(StreamRegistry::get().compute);
        raw_in_flight.push_back(std::move(copied));
    }

    raw_pending.erase(raw_pending.begin(), raw_pending.begin() + landed);
}

void GroupByGPUAccumulator::freeCopiedRaw(bool wait)
{
    if (wait)
    {
        for (const CopiedRaw & copied : raw_in_flight)
            copied.copied.wait();
        raw_in_flight.clear();
        return;
    }

    std::erase_if(raw_in_flight, [](const CopiedRaw & copied) { return copied.copied.isComplete(); });
}

void GroupByGPUAccumulator::startExpansion(std::vector<DeviceWork> && works)
{
    expansion_watch.restart();

    const size_t num_columns = key_element_types.size() + values.size() + filter_element_types.size();

    std::vector<Decompressor::Piece> pieces;
    pieces.reserve(works.size());
    for (DeviceWork & work : works)
    {
        if (work.reader >= readers.size() || work.column_index >= num_columns)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "A buffer of column {} from reader {} on the GPU aggregation's device thread", work.column_index, work.reader);

        pieces.push_back({
            .device_compressed = work.on_device.data(),
            .compressed_bytes = work.on_device.size(),
            .blocks = work.blocks,
            .uploaded = &*work.uploaded,
        });
    }

    decompressor.start(works.front().codec, pieces);
    expanding = std::move(works);
}

void GroupByGPUAccumulator::finishExpansion()
{
    if (expanding.empty())
        return;

    const char * expanded = decompressor.finish();

    /// The runs are appended to their columns through the default stream, behind the kernels
    /// queued over the last pieces. Each column's runs keep their order.
    size_t compressed_bytes = 0;
    size_t at = 0;
    for (DeviceWork & work : expanding)
    {
        const size_t bytes = decompressedBytesOf(work.blocks);
        DeviceColumn & column = readers[work.reader].device_columns[work.column_index];
        checkCuda(
            cudaMemcpyAsync(column.grow(bytes), expanded + at, bytes, cudaMemcpyDeviceToDevice, StreamRegistry::get().compute),
            "Cannot append {} expanded bytes to a device column",
            bytes);
        at += bytes;
        compressed_bytes += work.on_device.size();
    }

    decompressor.release();
    expanding.clear();

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, expansion_watch.elapsedMicroseconds());
}

bool GroupByGPUAccumulator::hasPieceToGroup() const
{
    for (const Reader & reader : readers)
    {
        const size_t ready = rowsOnDeviceInEveryColumn(reader);
        if (ready > reader.grouped_rows && (reader.grouped_rows > 0 || ready - reader.grouped_rows >= batch_rows))
            return true;
    }
    return false;
}

bool GroupByGPUAccumulator::groupPiece()
{
    for (Reader & reader : readers)
    {
        const size_t ready = rowsOnDeviceInEveryColumn(reader);
        if (ready <= reader.grouped_rows || (reader.grouped_rows == 0 && ready - reader.grouped_rows < batch_rows))
            continue;

        groupRowsOnDevice(reader, std::min(ready, reader.grouped_rows + group_piece_rows));
        if (reader.grouped_rows == ready)
            dropGroupedRows(reader);
        return true;
    }
    return false;
}

void GroupByGPUAccumulator::finishPartOnDevice(size_t reader_index, size_t num_rows)
{
    if (reader_index >= readers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The end of a part from reader {} on the GPU aggregation's device thread", reader_index);

    finishExpansion();
    flushPendingRaw(/*wait=*/true);

    Reader & reader = readers[reader_index];

    for (size_t i = 0; i < reader.device_columns.size(); ++i)
    {
        const DeviceColumn & column = reader.device_columns[i];
        if (reader.dropped_rows * column.elementSize() + column.bytes() != num_rows * column.elementSize())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of a part came to {} rows and {} bytes on the device, where the part has {} rows of {} bytes",
                i,
                reader.dropped_rows,
                column.bytes(),
                num_rows,
                column.elementSize());
    }

    groupRowsOnDevice(reader, num_rows - reader.dropped_rows);

    for (auto & column : reader.device_columns)
        column.clear();
    reader.grouped_rows = 0;
    reader.dropped_rows = 0;
}

size_t GroupByGPUAccumulator::rowsOnDeviceInEveryColumn(const Reader & reader) const
{
    size_t rows = std::numeric_limits<size_t>::max();
    for (const auto & column : reader.device_columns)
        rows = std::min(rows, column.rows());
    return rows;
}

void GroupByGPUAccumulator::groupRowsOnDevice(Reader & reader, size_t up_to)
{
    const size_t num_rows = up_to - reader.grouped_rows;
    if (num_rows == 0)
        return;

    std::vector<DeviceColumnView> keys;
    std::vector<DeviceColumnView> value_columns;
    std::vector<DeviceColumnView> filter_columns;
    keys.reserve(key_element_types.size());
    value_columns.reserve(values.size());
    filter_columns.reserve(filter_element_types.size());

    for (size_t i = 0; i < reader.device_columns.size(); ++i)
    {
        DeviceColumnView view = reader.device_columns[i].view();
        view.data += reader.grouped_rows * sizeOf(view.element_type);
        view.rows = num_rows;

        if (i < key_element_types.size())
            keys.push_back(view);
        else if (i < key_element_types.size() + values.size())
            value_columns.push_back(view);
        else
            filter_columns.push_back(view);
    }

    Stopwatch watch;

    const double kernel_microseconds = onDevice(
        [&] { return group_by->addBatch(keys, value_columns, filter_columns, filter ? &*filter : nullptr); },
        "Cannot group {} rows on the device",
        num_rows);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, num_rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::GPUGroupByKernelMicroseconds, static_cast<UInt64>(kernel_microseconds));

    reader.grouped_rows = up_to;
}

void GroupByGPUAccumulator::dropGroupedRows(Reader & reader)
{
    for (auto & column : reader.device_columns)
        column.dropFront(reader.grouped_rows);
    reader.dropped_rows += reader.grouped_rows;
    reader.grouped_rows = 0;
}

size_t GroupByGPUAccumulator::finalize()
{
    if (compressed)
    {
        DeviceWork work;
        work.kind = DeviceWork::Kind::Finish;
        enqueue(std::move(work));
        device_thread.join();
        rethrowDeviceError();
    }
    else
    {
        sendBatchToDevice();
    }

    Stopwatch watch;
    const size_t groups = onDevice([&] { return group_by->finalize(); }, "Cannot finalize a `GROUP BY` on the device");
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    num_groups = groups;
    return groups;
}

void GroupByGPUAccumulator::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    std::vector<HostColumnView> keys;
    keys.reserve(key_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i)
        keys.push_back(resizeForElementType(*key_columns[i], *num_groups, key_element_types[i]));

    std::vector<HostColumnView> groups;
    groups.reserve(value_columns.size());
    for (size_t i = 0; i < value_columns.size(); ++i)
    {
        const GPUElementType left_in = values[i].aggregation == GPUAggregationKind::Sum ? values[i].result_type : values[i].element_type;
        groups.push_back(resizeForElementType(*value_columns[i], *num_groups, left_in));
    }

    if (*num_groups == 0)
        return;

    Stopwatch watch;
    onDevice([&] { group_by->copyGroupsOut(keys, groups); }, "Cannot copy {} groups back from the device", *num_groups);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

}

#endif
