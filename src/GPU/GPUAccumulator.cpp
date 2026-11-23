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

GPUElementType dataElementTypeOf(GPUElementType column_type)
{
    return columnKindOf(column_type) == GPUColumnKind::Variable ? GPUElementType::UInt8 : column_type;
}

std::vector<GPUElementType> valueTypesOrThrow(const DataTypes & value_types, bool collect)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(value_types.size());
    for (const auto & type : value_types)
    {
        if (!collect)
        {
            element_types.push_back(columnTypeOrThrow(*type));
            continue;
        }
        const auto element_type = collectElementTypeOf(*type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} collected on the GPU", type->getName());
        element_types.push_back(*element_type);
    }
    return element_types;
}

std::vector<GPUElementType> keyElementTypesOf(const std::vector<GPUElementType> & keys)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(keys.size());
    for (const auto & key : keys)
        element_types.push_back(dataElementTypeOf(key));
    return element_types;
}

std::vector<size_t> variableColumnsOf(const std::vector<GPUElementType> & keys, const std::vector<GPUElementType> & values)
{
    std::vector<size_t> columns;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (columnKindOf(keys[i]) == GPUColumnKind::Variable)
            columns.push_back(i);
    }
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (columnKindOf(values[i]) == GPUColumnKind::Variable)
            columns.push_back(keys.size() + i);
    }
    return columns;
}

/// A variable-width column counts as one byte per row.
size_t rowBytesOf(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUElementType> & values,
    const std::vector<GPUElementType> & filter_types)
{
    size_t bytes = 0;
    for (const GPUElementType key_element_type : key_element_types)
        bytes += sizeOf(key_element_type);
    for (const GPUElementType value : values)
        bytes += sizeOf(dataElementTypeOf(value));
    for (const GPUElementType filter_type : filter_types)
        bytes += sizeOf(filter_type);
    return bytes;
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

GPUCompressedLayout::GPUCompressedLayout(
    const DataTypes & key_types,
    const DataTypes & value_types,
    bool collect,
    const DataTypes & filter_types_,
    std::vector<std::optional<uint32_t>> filter_aliases_,
    size_t batch_bytes)
    : keys(groupByKeysOrThrow(key_types))
    , key_element_types(keyElementTypesOf(keys))
    , values(valueTypesOrThrow(value_types, collect))
    , filter_types(filterElementTypesOrThrow(filter_types_))
    , filter_aliases(std::move(filter_aliases_))
    , variable_columns(variableColumnsOf(keys, values))
    , batch_rows(std::clamp(batch_bytes / std::max<size_t>(rowBytesOf(key_element_types, values, filter_types), 1), size_t{1}, max_batch_rows))
{
    if (numColumns() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read of no columns");

    if (!filter_aliases.empty() && filter_aliases.size() != filter_types.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} aliases of {} columns of the `WHERE` of a GPU read", filter_aliases.size(), filter_types.size());

    for (size_t i = 0; i < filter_aliases.size(); ++i)
    {
        if (!filter_aliases[i])
            continue;
        const size_t alias = *filter_aliases[i];
        if (alias >= firstFilterColumn() || isVariable(alias) || elementTypeOf(alias) != filter_types[i])
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Column {} of the `WHERE` of a GPU read reads column {}, which is not of its type", i, alias);
    }
}

bool GPUCompressedLayout::isAliased(size_t column) const
{
    return column >= firstFilterColumn() && column < firstSizesColumn() && column - firstFilterColumn() < filter_aliases.size()
        && filter_aliases[column - firstFilterColumn()];
}

size_t GPUCompressedLayout::aliasOf(size_t column) const
{
    if (!isAliased(column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read is not an alias", column);
    return *filter_aliases[column - firstFilterColumn()];
}

bool GPUCompressedLayout::isVariable(size_t column) const
{
    return std::find(variable_columns.begin(), variable_columns.end(), column) != variable_columns.end();
}

size_t GPUCompressedLayout::sizesColumnOf(size_t variable_column) const
{
    return firstSizesColumn() + variableOrdinalOf(variable_column);
}

size_t GPUCompressedLayout::variableOrdinalOf(size_t column) const
{
    if (isSizes(column))
        return column - firstSizesColumn();
    const auto it = std::find(variable_columns.begin(), variable_columns.end(), column);
    if (it == variable_columns.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read is not of varying width", column);
    return static_cast<size_t>(it - variable_columns.begin());
}

GPUElementType GPUCompressedLayout::elementTypeOf(size_t column) const
{
    if (isSizes(column))
        return GPUElementType::UInt64;
    if (column < keys.size())
        return key_element_types[column];
    if (column < firstFilterColumn())
        return dataElementTypeOf(values[column - keys.size()]);
    if (column < firstSizesColumn())
        return filter_types[column - firstFilterColumn()];
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read of {} columns", column, numColumns());
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

GPUBlockStager::GPUBlockStager(std::shared_ptr<const GPUCompressedLayout> layout_, std::shared_ptr<GPUStagedBatchPool> pool_)
    : layout(std::move(layout_)), pool(std::move(pool_)), current(pool->take(layout->numColumns()))
{
}

CompressedHostColumn & GPUBlockStager::columnOrThrow(size_t column)
{
    if (column >= current->columns.size() || layout->isAliased(column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU read of {} columns is not read", column, current->columns.size());
    return current->columns[column];
}

bool GPUBlockStager::isFull() const
{
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < layout->numColumns(); ++c)
    {
        if (layout->isAliased(c))
            continue;
        const CompressedHostColumn & column = current->columns[c];
        if (column.bytes.size() >= max_column_bytes)
            return true;
        /// The bytes of a `String` make no rows of their own: its sizes do.
        if (layout->isVariable(c))
            continue;
        rows = std::min(rows, column.kept_bytes / layout->elementBytesOf(c));
    }
    return rows != std::numeric_limits<size_t>::max() && rows >= layout->batch_rows;
}

void GPUBlockStager::seal(std::optional<size_t> part_rows)
{
    current->part_rows = part_rows;
    sealed.push_back(std::move(current));
    current = pool->take(layout->numColumns());
}

std::unique_ptr<GPUStagedBatch> GPUBlockStager::takeSealed()
{
    if (sealed.empty())
        return nullptr;
    auto batch = std::move(sealed.front());
    sealed.pop_front();
    return batch;
}

void GPUBlockStager::addCompressedBlock(
    size_t column_index,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes,
    size_t expected_bytes,
    size_t skipped_bytes,
    size_t kept_bytes)
{
    const auto codec = codecOf(method);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A block of compression method {:#x} for the GPU, which the device does not expand", static_cast<UInt16>(method));

    if (skipped_bytes + kept_bytes > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept_bytes, skipped_bytes, decompressed_bytes);

    /// A batch has one run of bytes of each column: it starts at a block that skips bytes, and ends at one cut short.
    {
        const CompressedHostColumn & column = columnOrThrow(column_index);
        const bool decompressed = !column.empty() && !column.isCompressed();
        if (decompressed || column.isCut() || (!column.blocks.empty() && (skipped_bytes != 0 || column.codec != codec)))
            seal();
    }

    CompressedHostColumn & column = current->columns[column_index];
    if (column.blocks.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.bytes.reserve(std::min(max_column_bytes, std::max(expected_bytes, payload.size())));
    }
    column.kept_bytes += kept_bytes;
    column.codec = codec;

    column.bytes.resize_fill(alignedUp(column.bytes.size()), 0);
    column.blocks.push_back({.offset = column.bytes.size(), .compressed_bytes = payload.size(), .decompressed_bytes = decompressed_bytes});
    appendHostBytes(column.bytes, payload);

    if (isFull())
        seal();
}

std::span<char> GPUBlockStager::reserveRawBytes(size_t column_index, size_t max_bytes, size_t expected_bytes)
{
    {
        const CompressedHostColumn & column = columnOrThrow(column_index);
        if (column.isCompressed() || column.bytes.size() >= max_column_bytes)
            seal();
    }

    CompressedHostColumn & column = current->columns[column_index];
    column.bytes.reserve(std::min(max_column_bytes, column.bytes.size() + std::max<size_t>(expected_bytes, 1)));
    return {
        column.bytes.data() + column.bytes.size(),
        std::min({max_bytes, max_column_bytes - column.bytes.size(), column.bytes.capacity() - column.bytes.size()})};
}

void GPUBlockStager::commitRawBytes(size_t column_index, size_t bytes)
{
    CompressedHostColumn & column = columnOrThrow(column_index);
    if (column.isCompressed() || column.bytes.size() + bytes > column.bytes.capacity())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} raw bytes of a column for the GPU committed beyond what was reserved", bytes);

    growBy(column.bytes, bytes);
    column.kept_bytes += bytes;

    if (isFull())
        seal();
}

void GPUBlockStager::finishPart(size_t num_rows)
{
    seal(num_rows);
}

GPUDeviceBatch::GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_, std::shared_ptr<const GPUCompressedLayout> layout_)
    : upload_stream(std::move(upload_stream_)), layout(std::move(layout_))
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

DeviceColumnView GPUDeviceBatch::column(size_t column) const
{
    if (column >= layout->firstSizesColumn() || layout->isAliased(column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch is not one of its values", column);

    if (layout->isVariable(column))
    {
        return DeviceVariableColumn{
            .offsets = reinterpret_cast<const uint64_t *>(starts[layout->sizesColumnOf(column)]),
            .chars = starts[column],
            .rows = num_rows,
            .chars_bytes = variable_bytes[layout->variableOrdinalOf(column)],
        };
    }
    return DeviceFixedColumn{.element_type = layout->elementTypeOf(column), .data = starts[column], .rows = num_rows};
}

std::vector<DeviceColumnView> GPUDeviceBatch::keys() const
{
    std::vector<DeviceColumnView> columns;
    columns.reserve(layout->keys.size());
    for (size_t i = 0; i < layout->keys.size(); ++i)
        columns.push_back(column(i));
    return columns;
}

std::vector<DeviceColumnView> GPUDeviceBatch::values() const
{
    std::vector<DeviceColumnView> columns;
    columns.reserve(layout->values.size());
    for (size_t i = 0; i < layout->values.size(); ++i)
        columns.push_back(column(layout->keys.size() + i));
    return columns;
}

std::vector<DeviceFixedColumn> GPUDeviceBatch::filterColumns() const
{
    std::vector<DeviceFixedColumn> columns;
    columns.reserve(layout->filter_types.size());
    for (size_t i = 0; i < layout->filter_types.size(); ++i)
    {
        const size_t c = layout->firstFilterColumn() + i;
        columns.push_back(fixedOrThrow(column(layout->isAliased(c) ? layout->aliasOf(c) : c)));
    }
    return columns;
}

void GPUDeviceBatch::moveTo(cuda::stream_ref stream)
{
    if (consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch handed to a stream twice");

    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for a GPU batch");
    consumed = createEvent();
    consumer = stream.get();
}

GPUBatchUploader::GPUBatchUploader(std::shared_ptr<const GPUCompressedLayout> layout_)
    : stream(std::make_shared<StreamPtr>(createStream()))
    , layout(std::move(layout_))
    , decompression(cuda::stream_ref{stream->get()})
{
    carried.reserve(layout->numColumns());
    carried_size_tails.reserve(layout->numColumns());
    for (size_t c = 0; c < layout->numColumns(); ++c)
    {
        carried.emplace_back(cuda::stream_ref{stream->get()});
        carried_size_tails.emplace_back(cuda::stream_ref{stream->get()});
    }
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

std::optional<GPUDeviceBatch> GPUBatchUploader::expand(const GPUStagedBatch & staged)
{
    const cuda::stream_ref own{stream->get()};
    const size_t num_columns = layout->numColumns();
    const size_t first_sizes = layout->firstSizesColumn();
    const auto & variable_columns = layout->variableColumns();

    if (staged.columns.size() != num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A staged batch of {} columns for a GPU read of {}", staged.columns.size(), num_columns);

    GPUDeviceBatch batch(stream, layout);
    batch.buffers.reserve(num_columns);
    batch.starts.assign(num_columns, nullptr);

    std::vector<size_t> sizes(num_columns, 0);
    std::vector<size_t> carried_rows(num_columns, 0);
    std::vector<const char *> size_tails(num_columns, nullptr);
    std::vector<size_t> size_tail_bytes(num_columns, 0);

    /// nvcomp takes only aligned destinations: blocks of others expand elsewhere and are copied in place.
    std::vector<DeviceBuffer> expanded_elsewhere;
    expanded_elsewhere.reserve(num_columns);
    struct Move
    {
        const char * from;
        char * to;
        size_t bytes;
    };
    std::vector<Move> moves;

    size_t compressed_total = 0;
    for (const auto & column : staged.columns)
    {
        if (column.isCompressed())
            compressed_total += alignedUp(column.bytes.size());
    }
    DeviceBuffer uploaded(own);
    char * const uploaded_base = compressed_total ? uploaded.grow(compressed_total) : nullptr;
    size_t uploaded_at = 0;

    std::vector<std::pair<GPUCodec, std::vector<CompressedPiece>>> pieces;
    const auto pieces_of = [&](GPUCodec codec) -> std::vector<CompressedPiece> &
    {
        for (auto & [of, list] : pieces)
        {
            if (of == codec)
                return list;
        }
        return pieces.emplace_back(codec, std::vector<CompressedPiece>{}).second;
    };

    for (size_t c = 0; c < num_columns; ++c)
    {
        DeviceBuffer & buffer = batch.buffers.emplace_back(own);
        if (layout->isAliased(c))
            continue;

        const CompressedHostColumn & column = staged.columns[c];
        const DeviceBuffer & carried_of_column = carried[c];
        const size_t expanded = column.expandedBytes();

        char * destination = nullptr;
        if (layout->isSizes(c))
        {
            if (column.skipped_bytes != 0 || column.isCut())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
            const size_t carried_entries = carried_of_column.empty() ? 1 : carried_of_column.size() / sizeof(UInt64);
            const DeviceBuffer & tail = carried_size_tails[c];
            char * const base = buffer.grow(carried_entries * sizeof(UInt64) + tail.size() + expanded);
            if (carried_of_column.empty())
                checkCuda(cudaMemsetAsync(base, 0, sizeof(UInt64), own.get()), "Cannot start the offsets of a column of varying width");
            else
                checkCuda(
                    cudaMemcpyAsync(base, carried_of_column.data(), carried_of_column.size(), cudaMemcpyDeviceToDevice, own.get()),
                    "Cannot carry offsets on the device");
            /// After the carried offsets, the bytes of the size that the batch before ended within.
            char * const first_size = base + carried_entries * sizeof(UInt64);
            if (!tail.empty())
                checkCuda(
                    cudaMemcpyAsync(first_size, tail.data(), tail.size(), cudaMemcpyDeviceToDevice, own.get()),
                    "Cannot carry the bytes of a string size on the device");
            destination = first_size + tail.size();
            batch.starts[c] = base;
            carried_rows[c] = carried_entries - 1;
            const size_t size_bytes = tail.size() + column.kept_bytes;
            sizes[c] = carried_rows[c] + size_bytes / sizeof(UInt64);
            size_tail_bytes[c] = size_bytes % sizeof(UInt64);
            size_tails[c] = first_size + size_bytes - size_tail_bytes[c];
        }
        else
        {
            /// The carried bytes are copied over the skipped ones; the padding aligns the start of the column.
            const size_t carried_bytes = carried_of_column.size();
            const size_t lead = std::max(carried_bytes, column.skipped_bytes);
            const size_t element = layout->elementBytesOf(c);
            const size_t padding = (element - (lead - carried_bytes) % element) % element;
            char * const base = buffer.grow(padding + lead - column.skipped_bytes + expanded);
            destination = base + padding + lead - column.skipped_bytes;
            batch.starts[c] = base + padding + lead - carried_bytes;
            sizes[c] = carried_bytes + column.kept_bytes;
        }

        if (expanded == 0)
            continue;
        if (!column.isCompressed())
        {
            checkCuda(
                cudaMemcpyAsync(destination, column.bytes.data(), column.bytes.size(), cudaMemcpyHostToDevice, own.get()),
                "Cannot send raw bytes to the device");
            continue;
        }
        char * const at = uploaded_base + uploaded_at;
        checkCuda(
            cudaMemcpyAsync(at, column.bytes.data(), column.bytes.size(), cudaMemcpyHostToDevice, own.get()),
            "Cannot send compressed blocks to the device");
        char * expand_to = destination;
        if (reinterpret_cast<uintptr_t>(destination) % expand_alignment != 0)
        {
            expand_to = expanded_elsewhere.emplace_back(own).grow(expanded);
            moves.push_back({expand_to, destination, expanded});
        }
        pieces_of(*column.codec).push_back({
            .device_compressed = at,
            .compressed_bytes = column.bytes.size(),
            .blocks = column.blocks,
            .destination = expand_to,
        });
        uploaded_at += alignedUp(column.bytes.size());
    }

    /// The staged memory is free once the uploads are done; nvcomp waits for them as well.
    checkCuda(cudaEventRecord(staged_sent.get(), own.get()), "Cannot mark a point in an uploader's stream");
    for (auto & [codec, list] : pieces)
    {
        decompression.launch(codec, list, nullptr, staged_sent.get());
        decompression.waitAndCheck();
    }
    checkCuda(cudaEventSynchronize(staged_sent.get()), "Cannot wait for the uploads of a batch");
    for (const Move & move : moves)
        checkCuda(cudaMemcpyAsync(move.to, move.from, move.bytes, cudaMemcpyDeviceToDevice, own.get()), "Cannot move expanded bytes on the device");

    for (size_t c = 0; c < num_columns; ++c)
    {
        if (layout->isAliased(c) || layout->isSizes(c) || carried[c].empty())
            continue;
        checkCuda(
            cudaMemcpyAsync(batch.starts[c], carried[c].data(), carried[c].size(), cudaMemcpyDeviceToDevice, own.get()),
            "Cannot carry bytes on the device");
    }

    for (const size_t variable : variable_columns)
    {
        const size_t o = layout->sizesColumnOf(variable);
        auto * const offsets = reinterpret_cast<uint64_t *>(batch.starts[o]);
        const size_t fresh = sizes[o] - carried_rows[o];
        if (fresh == 0)
            continue;
        /// The sizes are untrusted: bounded as on the CPU before they become offsets.
        if (sumOfSizesUpTo(offsets + carried_rows[o] + 1, fresh, max_total_string_bytes, own) > max_total_string_bytes)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The sizes of a String column read on the GPU declare more than {} bytes, most likely the data is corrupted",
                max_total_string_bytes);
        offsetsFromSizes(offsets + carried_rows[o] + 1, fresh, offsets + carried_rows[o], own);
    }

    /// The rows every column has: of a `String`, those whose bytes are all there.
    size_t rows = std::numeric_limits<size_t>::max();
    for (size_t c = 0; c < first_sizes; ++c)
    {
        if (layout->isAliased(c) || layout->isVariable(c))
            continue;
        rows = std::min(rows, sizes[c] / layout->elementBytesOf(c));
    }
    std::vector<CoveredRows> covered(variable_columns.size());
    for (size_t j = 0; j < variable_columns.size(); ++j)
    {
        const size_t o = layout->sizesColumnOf(variable_columns[j]);
        covered[j] = rowsCoveredBy(reinterpret_cast<const uint64_t *>(batch.starts[o]), sizes[o], sizes[variable_columns[j]], own);
        rows = std::min(rows, covered[j].rows);
    }
    if (rows == std::numeric_limits<size_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch without columns");

    batch.variable_bytes.resize(variable_columns.size());
    for (size_t j = 0; j < variable_columns.size(); ++j)
    {
        const size_t o = layout->sizesColumnOf(variable_columns[j]);
        batch.variable_bytes[j] = covered[j].rows == rows
            ? covered[j].bytes
            : rowsCoveredBy(reinterpret_cast<const uint64_t *>(batch.starts[o]), rows, sizes[variable_columns[j]], own).bytes;
    }

    if (staged.part_rows)
    {
        const size_t part_rows = *staged.part_rows;
        if (part_rows < part_rows_sent || rows != part_rows - part_rows_sent)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A part of {} rows came to {} rows sent and {} on the device in every column of a GPU read",
                part_rows,
                part_rows_sent,
                rows);
        for (size_t c = 0; c < first_sizes; ++c)
        {
            if (layout->isAliased(c) || layout->isVariable(c))
                continue;
            if (sizes[c] != rows * layout->elementBytesOf(c))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Column {} of a part came to {} bytes on the device, where the part has {} rows", c, sizes[c], part_rows);
        }
        for (size_t j = 0; j < variable_columns.size(); ++j)
        {
            const size_t o = layout->sizesColumnOf(variable_columns[j]);
            if (size_tail_bytes[o] != 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "The sizes of a String column of a part read on the GPU end within a size, the data is corrupted");
            if (sizes[o] != rows || batch.variable_bytes[j] != sizes[variable_columns[j]])
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of varying width of a part came to {} sizes and {} bytes on the device, of which its {} rows take {}",
                    variable_columns[j],
                    sizes[o],
                    sizes[variable_columns[j]],
                    rows,
                    batch.variable_bytes[j]);
        }
    }

    /// What the batch does not take is carried to the next one.
    for (size_t c = 0; c < num_columns; ++c)
    {
        if (layout->isAliased(c))
            continue;

        DeviceBuffer next(own);
        if (layout->isSizes(c))
        {
            DeviceBuffer next_tail(own);
            if (size_tail_bytes[c] != 0)
                checkCuda(
                    cudaMemcpyAsync(next_tail.grow(size_tail_bytes[c]), size_tails[c], size_tail_bytes[c], cudaMemcpyDeviceToDevice, own.get()),
                    "Cannot carry the bytes of a string size on the device");
            carried_size_tails[c] = std::move(next_tail);

            const size_t left = sizes[c] - rows;
            if (left != 0)
            {
                auto * const to = reinterpret_cast<uint64_t *>(next.grow((left + 1) * sizeof(UInt64)));
                subtractFromOffsets(
                    reinterpret_cast<const uint64_t *>(batch.starts[c]) + rows, left + 1, batch.variable_bytes[layout->variableOrdinalOf(c)], to, own);
            }
        }
        else
        {
            const size_t taken = layout->isVariable(c) ? batch.variable_bytes[layout->variableOrdinalOf(c)] : rows * layout->elementBytesOf(c);
            const size_t left = sizes[c] - taken;
            if (left != 0)
                checkCuda(
                    cudaMemcpyAsync(next.grow(left), batch.starts[c] + taken, left, cudaMemcpyDeviceToDevice, own.get()),
                    "Cannot carry bytes on the device");
        }
        carried[c] = std::move(next);
    }

    part_rows_sent = staged.part_rows ? 0 : part_rows_sent + rows;

    if (rows == 0)
        return std::nullopt;
    batch.num_rows = rows;
    checkCuda(cudaEventRecord(batch.ready.get(), own.get()), "Cannot mark a point in an upload stream");
    return batch;
}

GPUGroupingLane::GPUGroupingLane(
    std::vector<GPUElementType> keys_,
    std::vector<GPUElementType> values_,
    std::vector<GPUGroupByAggregate> aggregates_,
    std::vector<GPUComputedArgument> computed_,
    std::optional<CompiledGPUExpression> where_,
    size_t num_parts)
    : keys(std::move(keys_))
    , values(std::move(values_))
    , aggregates(aggregatesOrThrow(values.size() + computed_.size(), std::move(aggregates_), !keys.empty()))
    , computed(computedArgumentsOrThrow(values, std::move(computed_)))
    , where(std::move(where_))
    , lane_stream(createStream())
{
    if (where && where->expression.result != GPUExpressionResult::Bitmask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A `WHERE` of a GPU aggregation that is not a predicate");

    if (aggregates.empty())
    {
        if (!computed.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Rows filtered on the GPU with computed arguments");
        row_filter = std::make_unique<CudfRowFilter>(values, stream());
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
    const std::vector<DeviceColumnView> key_columns = batch.keys();
    const std::vector<DeviceColumnView> value_columns = batch.values();
    const std::vector<DeviceFixedColumn> filter_columns = batch.filterColumns();

    const size_t num_where_columns = where ? where->columns.size() : 0;
    if (key_columns.size() != keys.size() || value_columns.size() != values.size() || filter_columns.size() != num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU batch of {} keys, {} values and {} filter columns for a lane of {}, {} and {}",
            key_columns.size(),
            value_columns.size(),
            filter_columns.size(),
            keys.size(),
            values.size(),
            num_where_columns);

    const size_t rows = !key_columns.empty() ? key_columns.front().rows() : (!value_columns.empty() ? value_columns.front().rows() : 0);
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
    const std::vector<DeviceColumnView> value_columns = batch.values();
    const std::vector<DeviceFixedColumn> filter_columns = batch.filterColumns();
    const size_t num_where_columns = where ? where->columns.size() : 0;
    if (value_columns.size() != values.size() || filter_columns.size() != num_where_columns)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU batch of {} values and {} filter columns for a lane of {} and {}",
            value_columns.size(),
            filter_columns.size(),
            values.size(),
            num_where_columns);

    Stopwatch watch;
    MutableColumns columns;
    const size_t rows = row_filter->filter(value_columns, filter_columns, where ? &where->expression : nullptr);
    if (rows != 0)
    {
        std::vector<DeviceColumnView> from;
        std::vector<IColumn *> to;
        columns.reserve(types.size());
        for (size_t i = 0; i < types.size(); ++i)
        {
            from.push_back(row_filter->value(i));
            columns.push_back(types[i]->createColumn());
            to.push_back(columns.back().get());
        }
        copyDeviceToHost(from, to, stream());
    }

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, batch.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return columns;
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
    const std::vector<DeviceColumnView> columns = batch.values();
    if (columns.size() != reductions.size() || !batch.keys().empty() || !batch.filterColumns().empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} values, keys or a `WHERE` for {} reductions", columns.size(), reductions.size());

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
