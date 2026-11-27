#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/OffsetsKernels.cuh>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::GPU
{

namespace
{

/// What cuDF takes as the rows of a column.
constexpr size_t max_batch_rows = (1UL << 31) - 1;

/// Where each compressed block starts on the device, as nvcomp reads them, see `nvcompZstdRequiredDecompressionAlignment`.
constexpr size_t compressed_alignment = 8;

/// The alignment of the addresses that nvcomp expands blocks to: 8 bytes for ZSTD, 4 for LZ4.
constexpr size_t expand_alignment = 8;

/// The bytes the sizes of a `String` column may add up to, as `MAX_TOTAL_STRING_SIZE` of `SerializationString`.
constexpr UInt64 max_total_string_bytes = 1ULL << 48;

size_t alignedUp(size_t bytes)
{
    return (bytes + compressed_alignment - 1) / compressed_alignment * compressed_alignment;
}

GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

bool isVariable(GPUElementType type)
{
    return columnKindOf(type) == GPUColumnKind::Variable;
}

/// A `String` counts as one byte per row.
size_t rowBytesOf(const std::vector<GPUElementType> & column_types)
{
    size_t bytes = 0;
    for (const GPUElementType type : column_types)
        bytes += isVariable(type) ? 1 : sizeOf(type);
    return bytes;
}

std::vector<GPUElementType> typesAt(const std::vector<GPUElementType> & column_types, const std::vector<size_t> & positions)
{
    std::vector<GPUElementType> types;
    types.reserve(positions.size());
    for (const size_t position : positions)
    {
        if (position >= column_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch of {} columns", position, column_types.size());
        types.push_back(column_types[position]);
    }
    return types;
}

std::vector<DeviceColumnView> columnsAt(const GPUDeviceBatch & batch, const std::vector<size_t> & positions)
{
    const auto & columns = batch.columns();
    std::vector<DeviceColumnView> result;
    result.reserve(positions.size());
    for (const size_t position : positions)
    {
        if (position >= columns.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch of {} columns", position, columns.size());
        result.push_back(columns[position]);
    }
    return result;
}

std::vector<DeviceFixedColumn> fixedColumnsAt(const GPUDeviceBatch & batch, const std::vector<size_t> & positions)
{
    std::vector<DeviceFixedColumn> result;
    result.reserve(positions.size());
    for (const DeviceColumnView & column : columnsAt(batch, positions))
        result.push_back(fixedOrThrow(column));
    return result;
}

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_values, std::vector<GPUGroupByAggregate> aggregates, bool keyed)
{
    if (keyed && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with keys and without aggregates");

    for (const auto & aggregate : aggregates)
    {
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= num_values)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregate of value {} of {}", aggregate.column, num_values);
    }
    return aggregates;
}

std::vector<GPUComputedArgument> computedArgumentsOrThrow(const std::vector<GPUElementType> & values, std::vector<GPUComputedArgument> computed)
{
    for (const auto & value : computed)
    {
        const GPUExpression & expression = value.expression.expression;
        if (expression.result != GPUExpressionResult::Value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed by a predicate");
        if (value.columns.size() != expression.num_columns)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed by an expression of {} columns from {}", expression.num_columns, value.columns.size());
        for (const uint32_t column : value.columns)
        {
            if (column >= values.size() || columnKindOf(values[column]) != GPUColumnKind::Fixed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "An argument of a GPU aggregate computed from value {} of {}", column, values.size());
        }
    }
    return computed;
}

}

void GPUStagedColumn::clear()
{
    values.clear();
    sizes.clear();
}

void GPUStagedBatch::clear()
{
    for (auto & column : columns)
        column.clear();
    part_rows.reset();
}

std::unique_ptr<GPUStagedBatch> GPUStagedBatchPool::take(size_t num_columns)
{
    std::unique_ptr<GPUStagedBatch> batch;
    {
        std::lock_guard lock(mutex);
        if (!free_batches.empty())
        {
            batch = std::move(free_batches.back());
            free_batches.pop_back();
        }
    }
    if (!batch)
        batch = std::make_unique<GPUStagedBatch>();
    batch->clear();
    batch->columns.resize(num_columns);
    return batch;
}

void GPUStagedBatchPool::give(std::unique_ptr<GPUStagedBatch> batch)
{
    std::lock_guard lock(mutex);
    free_batches.push_back(std::move(batch));
}

GPUBlockStager::GPUBlockStager(std::vector<GPUElementType> column_types_, size_t batch_bytes, std::shared_ptr<GPUStagedBatchPool> pool_)
    : column_types(std::move(column_types_))
    , batch_rows(std::clamp(batch_bytes / std::max<size_t>(rowBytesOf(column_types), 1), size_t{1}, max_batch_rows))
    , pool(std::move(pool_))
{
    if (column_types.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read of no columns");
    current = pool->take(column_types.size());
}

CompressedHostColumn & GPUBlockStager::streamOrThrow(size_t column, GPUStream stream)
{
    if (column >= column_types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read of {} columns", column, column_types.size());
    if (stream == GPUStream::Values)
        return current->columns[column].values;
    if (!isVariable(column_types[column]))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of column {} of a GPU read, which is of a fixed width", column);
    return current->columns[column].sizes;
}

bool GPUBlockStager::isFull() const
{
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < column_types.size(); ++c)
    {
        const GPUStagedColumn & column = current->columns[c];
        if (column.values.bytes.size() >= max_column_bytes || column.sizes.bytes.size() >= max_column_bytes)
            return true;
        /// The bytes of a `String` make no rows of their own: its sizes do.
        rows = std::min(rows, isVariable(column_types[c]) ? column.sizes.kept_bytes / sizeof(UInt64) : column.values.kept_bytes / sizeOf(column_types[c]));
    }
    return rows >= batch_rows;
}

void GPUBlockStager::finishBatch(std::optional<size_t> part_rows)
{
    current->part_rows = part_rows;
    finished.push_back(std::move(current));
    current = pool->take(column_types.size());
}

std::unique_ptr<GPUStagedBatch> GPUBlockStager::takeFinished()
{
    if (finished.empty())
        return nullptr;
    auto batch = std::move(finished.front());
    finished.pop_front();
    return batch;
}

void GPUBlockStager::addCompressedBlock(
    size_t column_index,
    GPUStream stream,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    size_t kept_bytes)
{
    const auto codec = codecOf(method);
    /// The plan knows the codecs of the table, not those of the blocks of a part: a part written before an `ALTER` of a
    /// codec, or by the adaptive codec, may hold blocks of others.
    if (!codec)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A block of compression method {:#x} read for the GPU, which the device does not expand; such a part is aggregated "
            "on the CPU with `allow_experimental_gpu_aggregation = 0`",
            static_cast<UInt16>(method));

    if (skipped_bytes + kept_bytes > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept_bytes, skipped_bytes, decompressed_bytes);

    /// A batch has one run of bytes of each stream: it starts at a block that skips bytes, and ends at one cut short.
    {
        const CompressedHostColumn & column = streamOrThrow(column_index, stream);
        if (column.isCut() || (!column.empty() && (skipped_bytes != 0 || column.codec != codec)))
            finishBatch();
    }

    CompressedHostColumn & column = streamOrThrow(column_index, stream);
    if (column.blocks.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.bytes.reserve(std::min(max_column_bytes, std::max(expected_bytes, payload.size())));
    }
    column.kept_bytes += kept_bytes;
    column.codec = codec;

    column.bytes.resize_fill(alignedUp(column.bytes.size()), 0);
    addCompressedBlocksOf(*codec, payload, column.bytes.size(), decompressed_bytes, column.blocks);
    appendHostBytes(column.bytes, payload);

    if (isFull())
        finishBatch();
}

void GPUBlockStager::finishPart(size_t num_rows)
{
    finishBatch(num_rows);
}

GPUDeviceBatch::GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_)
    : upload_stream(std::move(upload_stream_))
{
}

GPUDeviceBatch::~GPUDeviceBatch()
{
    if (!consumed || !consumer || !upload_stream)
        return;

    /// The uploader's stream frees the memory, so it must wait for the consumer's stream.
    try
    {
        checkCuda(cudaEventRecord(consumed.get(), consumer), "Cannot mark a point in the stream of a GPU batch's consumer");
        checkCuda(cudaStreamWaitEvent(upload_stream->get(), consumed.get(), 0), "Cannot make an upload stream wait for a GPU batch's consumer");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        cudaStreamSynchronize(consumer);
    }
}

void GPUDeviceBatch::moveTo(cuda::stream_ref stream)
{
    if (consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch handed to a stream twice");

    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for a GPU batch");
    consumed = createEvent();
    consumer = stream.get();
}

GPUBatchUploader::GPUBatchUploader(std::vector<GPUElementType> column_types_)
    : stream(std::make_shared<StreamPtr>(createStream()))
    , column_types(std::move(column_types_))
    , decompression(cuda::stream_ref{stream->get()})
{
    const cuda::stream_ref own{stream->get()};
    carried.reserve(column_types.size());
    for (size_t c = 0; c < column_types.size(); ++c)
        carried.push_back({.values = DeviceBuffer(own), .offsets = DeviceBuffer(own), .size_tail = DeviceBuffer(own)});
}

std::optional<GPUDeviceBatch> GPUBatchUploader::upload(const GPUStagedBatch & staged)
{
    if (failed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A batch for a GPU uploader that failed on one before");

    /// What is carried does not follow the rows sent anymore if it throws.
    failed = true;
    std::optional<GPUDeviceBatch> batch;
    waitOnFailure(cuda::stream_ref{stream->get()}, [&]
    {
        if (auto expanded = expand(staged))
            batch.emplace(std::move(*expanded));
    });
    failed = false;
    return batch;
}

/// The compressed blocks of a batch: sent to the device in one buffer, and expanded there by nvcomp, a call per codec.
class GPUBatchUploader::BatchUpload
{
public:
    BatchUpload(const GPUStagedBatch & staged, cuda::stream_ref own_)
        : own(own_)
        , uploaded(own_)
    {
        size_t compressed_total = 0;
        for (const auto & column : staged.columns)
            compressed_total += alignedUp(column.values.bytes.size()) + alignedUp(column.sizes.bytes.size());
        uploaded_base = compressed_total ? uploaded.grow(compressed_total) : nullptr;
    }

    /// Sends the compressed blocks of a stream, to be expanded to `destination`.
    void send(const CompressedHostColumn & column, char * destination)
    {
        const size_t expanded_bytes = column.expandedBytes();
        if (expanded_bytes == 0)
            return;
        char * const at = uploaded_base + uploaded_at;
        checkCuda(
            cudaMemcpyAsync(at, column.bytes.data(), column.bytes.size(), cudaMemcpyHostToDevice, own.get()),
            "Cannot send compressed blocks to the device");
        char * expand_to = destination;
        if (reinterpret_cast<uintptr_t>(destination) % expand_alignment != 0)
        {
            expand_to = expanded_elsewhere.emplace_back(own).grow(expanded_bytes);
            moves.push_back({expand_to, destination, expanded_bytes});
        }
        piecesOf(*column.codec).push_back({
            .device_compressed = at,
            .compressed_bytes = column.bytes.size(),
            .blocks = column.blocks,
            .destination = expand_to,
        });
        uploaded_at += alignedUp(column.bytes.size());
    }

    /// Expands what was sent. The staged memory is free once it returns: nvcomp waits for the uploads as well.
    void expandAll(DecompressionBatch & batch_decompression, cudaEvent_t sent)
    {
        checkCuda(cudaEventRecord(sent, own.get()), "Cannot mark a point in an uploader's own");
        for (auto & [codec, list] : pieces)
        {
            batch_decompression.launch(codec, list, nullptr, sent);
            batch_decompression.waitAndCheck();
        }
        checkCuda(cudaEventSynchronize(sent), "Cannot wait for the uploads of a batch");
        for (const Move & move : moves)
            checkCuda(
                cudaMemcpyAsync(move.to, move.from, move.bytes, cudaMemcpyDeviceToDevice, own.get()), "Cannot move expanded bytes on the device");
    }

private:
    struct Move
    {
        const char * from;
        char * to;
        size_t bytes;
    };

    std::vector<CompressedPiece> & piecesOf(GPUCodec codec)
    {
        for (auto & [of, list] : pieces)
        {
            if (of == codec)
                return list;
        }
        return pieces.emplace_back(codec, std::vector<CompressedPiece>{}).second;
    }

    const cuda::stream_ref own;
    DeviceBuffer uploaded;
    char * uploaded_base = nullptr;
    size_t uploaded_at = 0;
    std::vector<std::pair<GPUCodec, std::vector<CompressedPiece>>> pieces;
    /// nvcomp takes only aligned destinations: the blocks of others expand elsewhere and are copied in place.
    std::vector<DeviceBuffer> expanded_elsewhere;
    std::vector<Move> moves;
};

/// A column of a batch on the device: the rows carried from the batch before, then those of its staged blocks. Of a
/// `String`, its bytes so, and the offsets of its rows, carried and from its sizes.
class GPUBatchUploader::ColumnOnDevice
{
public:
    /// Places the column in a buffer of its own among `buffers`, and sends its blocks through `upload`.
    ColumnOnDevice(
        GPUElementType type_,
        const GPUStagedColumn & staged,
        const Carried & carried_column,
        std::vector<DeviceBuffer> & buffers,
        BatchUpload & upload,
        cuda::stream_ref own)
        : type(type_)
    {
        /// The carried bytes are copied over the skipped ones; the padding aligns the start of the column.
        const CompressedHostColumn & staged_values = staged.values;
        const size_t carried_bytes = carried_column.values.size();
        const size_t lead = std::max(carried_bytes, staged_values.skipped_bytes);
        const size_t element = isVariable(type) ? 1 : sizeOf(type);
        const size_t padding = (element - (lead - carried_bytes) % element) % element;
        char * const base = buffers.emplace_back(own).grow(padding + lead - staged_values.skipped_bytes + staged_values.expandedBytes());
        values = base + padding + lead - carried_bytes;
        values_bytes = carried_bytes + staged_values.kept_bytes;
        upload.send(staged_values, base + padding + lead - staged_values.skipped_bytes);

        if (!isVariable(type))
            return;

        /// The offsets carried, then the bytes of the size that the batch before ended within, then the sizes.
        const CompressedHostColumn & staged_sizes = staged.sizes;
        if (staged_sizes.skipped_bytes != 0 || staged_sizes.isCut())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
        const size_t carried_entries = carried_column.offsets.empty() ? 1 : carried_column.offsets.size() / sizeof(UInt64);
        const DeviceBuffer & tail = carried_column.size_tail;
        char * const sizes_base = buffers.emplace_back(own).grow(carried_entries * sizeof(UInt64) + tail.size() + staged_sizes.expandedBytes());
        if (carried_column.offsets.empty())
            checkCuda(cudaMemsetAsync(sizes_base, 0, sizeof(UInt64), own.get()), "Cannot start the offsets of a column of varying width");
        else
            checkCuda(
                cudaMemcpyAsync(sizes_base, carried_column.offsets.data(), carried_column.offsets.size(), cudaMemcpyDeviceToDevice, own.get()),
                "Cannot carry offsets on the device");
        char * const first_size = sizes_base + carried_entries * sizeof(UInt64);
        if (!tail.empty())
            checkCuda(
                cudaMemcpyAsync(first_size, tail.data(), tail.size(), cudaMemcpyDeviceToDevice, own.get()),
                "Cannot carry the bytes of a string size on the device");

        Sizes & of = sizes.emplace();
        of.offsets = reinterpret_cast<uint64_t *>(sizes_base);
        of.carried_rows = carried_entries - 1;
        const size_t size_bytes = tail.size() + staged_sizes.kept_bytes;
        of.sized_rows = of.carried_rows + size_bytes / sizeof(UInt64);
        of.tail_bytes = size_bytes % sizeof(UInt64);
        of.tail = first_size + size_bytes - of.tail_bytes;
        upload.send(staged_sizes, first_size + tail.size());
    }

    /// Once its blocks are expanded: the carried bytes before them, and of a `String` the offsets of its sizes.
    void finish(const Carried & carried_column, cuda::stream_ref own)
    {
        if (!carried_column.values.empty())
            checkCuda(
                cudaMemcpyAsync(values, carried_column.values.data(), carried_column.values.size(), cudaMemcpyDeviceToDevice, own.get()),
                "Cannot carry bytes on the device");

        if (!sizes || sizes->sized_rows == sizes->carried_rows)
            return;
        /// The sizes are untrusted: bounded as on the CPU, from the carried offset, before they become offsets.
        const size_t fresh = sizes->sized_rows - sizes->carried_rows;
        uint64_t * const offsets = sizes->offsets + sizes->carried_rows;
        if (endOfSizesUpTo(offsets, offsets + 1, fresh, max_total_string_bytes, own) > max_total_string_bytes)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The sizes of a String column read on the GPU declare more than {} bytes, most likely the data is corrupted",
                max_total_string_bytes);
        offsetsFromSizes(offsets + 1, fresh, offsets, own);
    }

    /// The rows whose values are all there: of a `String`, those whose bytes are.
    size_t completeRows(cuda::stream_ref own)
    {
        if (!sizes)
            return values_bytes / sizeOf(type);
        sizes->covered = rowsCoveredBy(sizes->offsets, sizes->sized_rows, values_bytes, own);
        return sizes->covered.rows;
    }

    /// The first `rows` of it, the rest of which goes to `carried_column`. Of the end of a part, nothing may be left.
    DeviceColumnView take(size_t rows, bool part_ends, size_t index, Carried & carried_column, cuda::stream_ref own) const
    {
        size_t taken_bytes = rows * (sizes ? 1 : sizeOf(type));
        if (sizes)
            taken_bytes = sizes->covered.rows == rows ? sizes->covered.bytes : rowsCoveredBy(sizes->offsets, rows, values_bytes, own).bytes;

        if (part_ends)
            checkPartEnds(rows, taken_bytes, index);

        DeviceBuffer next(own);
        if (const size_t left = values_bytes - taken_bytes; left != 0)
            checkCuda(
                cudaMemcpyAsync(next.grow(left), values + taken_bytes, left, cudaMemcpyDeviceToDevice, own.get()),
                "Cannot carry bytes on the device");
        carried_column.values = std::move(next);

        if (!sizes)
            return DeviceFixedColumn{.element_type = type, .data = values, .rows = rows};

        DeviceBuffer next_tail(own);
        if (sizes->tail_bytes != 0)
            checkCuda(
                cudaMemcpyAsync(next_tail.grow(sizes->tail_bytes), sizes->tail, sizes->tail_bytes, cudaMemcpyDeviceToDevice, own.get()),
                "Cannot carry the bytes of a string size on the device");
        carried_column.size_tail = std::move(next_tail);

        DeviceBuffer next_offsets(own);
        if (const size_t sizes_left = sizes->sized_rows - rows; sizes_left != 0)
            subtractFromOffsets(
                sizes->offsets + rows,
                sizes_left + 1,
                taken_bytes,
                reinterpret_cast<uint64_t *>(next_offsets.grow((sizes_left + 1) * sizeof(UInt64))),
                own);
        carried_column.offsets = std::move(next_offsets);

        return DeviceVariableColumn{.offsets = sizes->offsets, .chars = values, .rows = rows, .chars_bytes = taken_bytes};
    }

private:
    struct Sizes
    {
        /// One more than the rows with a size: the carried ones, then those of this batch.
        uint64_t * offsets = nullptr;
        size_t carried_rows = 0;
        size_t sized_rows = 0;
        /// The bytes of a size that the last block of sizes cut.
        const char * tail = nullptr;
        size_t tail_bytes = 0;
        CoveredRows covered;
    };

    void checkPartEnds(size_t rows, size_t taken_bytes, size_t index) const
    {
        if (!sizes)
        {
            if (values_bytes != taken_bytes)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Column {} of a part came to {} bytes on the device, of which its {} rows take {}", index, values_bytes, rows, taken_bytes);
            return;
        }
        if (sizes->tail_bytes != 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "The sizes of a String column of a part read on the GPU end within a size, the data is corrupted");
        if (sizes->sized_rows != rows || taken_bytes != values_bytes)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of varying width of a part came to {} sizes and {} bytes on the device, of which its {} rows take {}",
                index,
                sizes->sized_rows,
                values_bytes,
                rows,
                taken_bytes);
    }

    const GPUElementType type;
    /// The values, or the bytes of a `String`, from its first row.
    char * values = nullptr;
    size_t values_bytes = 0;
    std::optional<Sizes> sizes;
};

std::optional<GPUDeviceBatch> GPUBatchUploader::expand(const GPUStagedBatch & staged)
{
    const cuda::stream_ref own{stream->get()};
    const size_t num_columns = column_types.size();
    if (staged.columns.size() != num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A staged batch of {} columns for a GPU read of {}", staged.columns.size(), num_columns);

    GPUDeviceBatch batch(stream);
    batch.buffers.reserve(2 * num_columns);

    BatchUpload upload(staged, own);
    std::vector<ColumnOnDevice> columns;
    columns.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        columns.emplace_back(column_types[c], staged.columns[c], carried[c], batch.buffers, upload, own);

    upload.expandAll(decompression, staged_sent.get());
    for (size_t c = 0; c < num_columns; ++c)
        columns[c].finish(carried[c], own);

    size_t rows = std::numeric_limits<size_t>::max();
    for (auto & column : columns)
        rows = std::min(rows, column.completeRows(own));

    if (staged.part_rows && (*staged.part_rows < part_rows_sent || rows != *staged.part_rows - part_rows_sent))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A part of {} rows came to {} rows sent and {} on the device in every column of a GPU read",
            *staged.part_rows,
            part_rows_sent,
            rows);

    /// What the batch does not take is carried to the next one.
    batch.column_views.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        batch.column_views.push_back(columns[c].take(rows, staged.part_rows.has_value(), c, carried[c], own));

    part_rows_sent = staged.part_rows ? 0 : part_rows_sent + rows;
    if (rows == 0)
        return std::nullopt;

    batch.num_rows = rows;
    checkCuda(cudaEventRecord(batch.ready.get(), own.get()), "Cannot mark a point in an upload stream");
    return batch;
}

GPUGroupingLane::GPUGroupingLane(
    const std::vector<GPUElementType> & column_types,
    GPUBatchColumns columns_,
    std::vector<GPUGroupByAggregate> aggregates_,
    std::vector<GPUComputedArgument> computed_,
    std::optional<CompiledGPUExpression> where_,
    size_t num_parts)
    : batch_columns(std::move(columns_))
    , keys(typesAt(column_types, batch_columns.keys))
    , values(typesAt(column_types, batch_columns.values))
    , aggregates(aggregatesOrThrow(values.size() + computed_.size(), std::move(aggregates_), !keys.empty()))
    , computed(computedArgumentsOrThrow(values, std::move(computed_)))
    , where(std::move(where_))
    , lane_stream(createStream())
{
    if (where && where->expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` of a GPU aggregation that is not a predicate");

    const size_t num_where_columns = where ? where->columns.size() : 0;
    if (batch_columns.where.size() != num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A `WHERE` of {} columns over {} columns of a GPU batch", num_where_columns, batch_columns.where.size());
    for (const GPUElementType type : typesAt(column_types, batch_columns.where))
    {
        if (isVariable(type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` on the GPU over a column of varying width");
    }

    if (aggregates.empty())
    {
        if (!computed.empty() || !where)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows filtered on the GPU with computed arguments or without a `WHERE`");
        row_filter = std::make_unique<CudfRowFilter>(values, where->expression, stream());
        return;
    }

    std::vector<GPUElementType> all_values = values;
    computed_values.reserve(computed.size());
    for (const auto & argument : computed)
    {
        all_values.push_back(argument.expression.expression.value_type);
        computed_values.emplace_back(stream());
    }
    group_by = std::make_unique<CudfGroupBy>(keys, all_values, aggregates, num_parts, stream());
}

CudfGroupBy & GPUGroupingLane::groupByOrThrow() const
{
    if (!group_by)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU lane that filters rows");
    return *group_by;
}

void GPUGroupingLane::add(GPUDeviceBatch batch)
{
    CudfGroupBy & groups = groupByOrThrow();
    batch.moveTo(stream());
    const std::vector<DeviceColumnView> key_columns = columnsAt(batch, batch_columns.keys);
    const std::vector<DeviceColumnView> value_columns = columnsAt(batch, batch_columns.values);
    const std::vector<DeviceFixedColumn> filter_columns = fixedColumnsAt(batch, batch_columns.where);
    const size_t rows = batch.rows();
    const GPUExpression * const filter = where ? &where->expression : nullptr;

    Stopwatch watch;
    if (computed.empty())
        groups.addBatch(key_columns, value_columns, filter_columns, filter);
    else
        groups.addBatch(key_columns, withComputedArguments(value_columns, rows), filter_columns, filter);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, rows);
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

std::vector<DeviceColumnView> GPUGroupingLane::withComputedArguments(const std::vector<DeviceColumnView> & value_columns, size_t rows)
{
    std::vector<DeviceColumnView> all_values = value_columns;
    std::vector<GPUExpressionInput> inputs;
    for (size_t i = 0; i < computed.size(); ++i)
    {
        const GPUExpression & expression = computed[i].expression.expression;
        inputs.clear();
        for (const uint32_t column : computed[i].columns)
            inputs.push_back({.data = value_columns[column].fixed.data, .type = value_columns[column].fixed.element_type});

        DeviceBuffer & out = computed_values[i];
        out.clear();
        char * const result = out.grow(rows * sizeOf(expression.value_type));
        evaluateGPUExpression(expression, {.columns = inputs.data(), .count = static_cast<uint32_t>(inputs.size())}, rows, result, stream());
        all_values.emplace_back(DeviceFixedColumn{.element_type = expression.value_type, .data = result, .rows = rows});
    }
    return all_values;
}

MutableColumns GPUGroupingLane::filterRows(GPUDeviceBatch batch, const DataTypes & types)
{
    if (!row_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows filtered by a GPU lane that groups them");
    if (types.size() != values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} types of {} columns filtered on the GPU", types.size(), values.size());

    batch.moveTo(stream());
    const std::vector<DeviceColumnView> value_columns = columnsAt(batch, batch_columns.values);
    const std::vector<DeviceFixedColumn> where_columns = fixedColumnsAt(batch, batch_columns.where);

    Stopwatch watch;
    MutableColumns kept;
    const size_t rows = row_filter->filter(value_columns, where_columns);
    if (rows != 0)
    {
        std::vector<DeviceColumnView> from;
        std::vector<IColumn *> to;
        kept.reserve(types.size());
        for (size_t i = 0; i < types.size(); ++i)
        {
            from.push_back(row_filter->value(i));
            kept.push_back(types[i]->createColumn());
            to.push_back(kept.back().get());
        }
        copyDeviceToHost(from, to, stream());
    }

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return kept;
}

void GPUGroupingLane::absorbPart(const GPUGroupingLane & other, size_t part)
{
    Stopwatch watch;
    groupByOrThrow().absorbPart(other.groupByOrThrow(), part);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

size_t GPUGroupingLane::finalize()
{
    Stopwatch watch;
    num_groups = groupByOrThrow().finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

size_t GPUGroupingLane::filterGroups(const CompiledGPUExpression & having, const std::vector<size_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");
    if (having.expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `HAVING` of a GPU aggregation that is not a predicate");
    if (*num_groups == 0)
        return 0;

    Stopwatch watch;

    const cuda::stream_ref own = stream();
    /// The caller holds the kernel only for this call, so the stream is drained before it returns, on an exception too.
    waitOnFailure(own, [&] { num_groups = groupByOrThrow().filterGroups(having.expression, columns); });
    synchronizeStream(own);

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GPUGroupingLane::copyGroupsTo(MutableColumns & key_columns, MutableColumns & aggregate_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    if (key_columns.size() != keys.size() || aggregate_columns.size() != aggregates.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            aggregate_columns.size(),
            keys.size(),
            aggregates.size());

    const size_t rows = *num_groups;
    if (rows == 0)
        return;

    Stopwatch watch;

    const CudfGroupBy & groups = groupByOrThrow();
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(keys.size() + aggregates.size());
    to.reserve(keys.size() + aggregates.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        const DeviceColumnView key = groups.key(i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const DeviceColumnView value = groups.value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of aggregate {} of {} groups", value.rows(), i, rows);
        from.push_back(value);
        to.push_back(aggregate_columns[i].get());
    }

    copyDeviceToHost(from, to, stream());

    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

GPUAccumulator::GPUAccumulator(
    const DataTypes & argument_types, const DataTypes & result_types_, const std::vector<GPUAggregationKind> & aggregations)
    : result_types(result_types_)
    , reduce_stream(createStream())
{
    if (!canAggregateOnDevice({}, argument_types, result_types, aggregations))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reduce these aggregates on a GPU");

    reductions.reserve(aggregations.size());
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const GPUElementType element_type = *columnTypeOf(*argument_types[i]);
        reductions.push_back(std::make_unique<CudfReduction>(
            element_type, resultTypeFor(element_type, aggregations[i]), aggregations[i], cuda::stream_ref{reduce_stream.get()}));
    }
}

void GPUAccumulator::add(GPUDeviceBatch batch)
{
    const std::vector<DeviceColumnView> & columns = batch.columns();
    if (columns.size() != reductions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} columns for {} reductions", columns.size(), reductions.size());

    batch.moveTo(cuda::stream_ref{reduce_stream.get()});

    Stopwatch watch;
    for (size_t i = 0; i < columns.size(); ++i)
        reductions[i]->addBatch(columns[i]);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

std::vector<Field> GPUAccumulator::finalize()
{
    Stopwatch watch;
    std::vector<Field> results;
    results.reserve(reductions.size());
    for (size_t i = 0; i < reductions.size(); ++i)
    {
        const DeviceColumnView result = reductions[i]->finalize();
        if (result.rows() == 0)
        {
            results.push_back(result_types[i]->getDefault());
            continue;
        }

        MutableColumnPtr column = result_types[i]->createColumn();
        IColumn * const to = column.get();
        copyDeviceToHost(std::span<const DeviceColumnView>(&result, 1), std::span<IColumn * const>(&to, 1), cuda::stream_ref{reduce_stream.get()});
        results.push_back((*column)[0]);
    }
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    return results;
}

}

#endif
