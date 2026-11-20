#include <GPU/GPUAccumulator.h>

#if USE_GPU

#include <GPU/GPUColumns.h>
#include <GPU/GPUDevice.h>
#include <GPU/OffsetsKernels.cuh>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <cstring>
#include <numeric>

namespace ProfileEvents
{
    extern const Event GPUAggregationRows;
    extern const Event GPUAggregationBatches;
    extern const Event GPUAggregationMicroseconds;
    extern const Event GPUDecompressionMicroseconds;
    extern const Event GPUDecompressionBytes;
    extern const Event GPUDeviceWaitMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

/// The result of a `sum` is wider than what it sums; a `min` or a `max` is of the type it takes.
GPUElementType resultTypeFor(GPUElementType element_type, GPUAggregationKind aggregation)
{
    return aggregation == GPUAggregationKind::Sum ? sumResultTypeFor(element_type) : element_type;
}

/// What the bytes of a column are held as on the device: those of a column of values of varying width as bytes.
GPUElementType dataElementTypeOf(GPUElementType column_type)
{
    return columnKindOf(column_type) == GPUColumnKind::Variable ? GPUElementType::UInt8 : column_type;
}

/// A batch of fixed-width values holds whole values, and at most as many as the device takes at once.
size_t batchBytesFor(size_t batch_bytes, GPUElementType element_type, size_t max_batch_rows)
{
    if (columnKindOf(element_type) == GPUColumnKind::Variable)
        return std::max<size_t>(batch_bytes, 1);
    const size_t element_size = sizeOf(element_type);
    return std::clamp(batch_bytes, element_size, max_batch_rows * element_size);
}

}

GPUAccumulator::GPUAccumulator(
    const IDataType & argument_type, const IDataType & result_type_, GPUAggregationKind aggregation_, size_t batch_bytes_)
    : element_type(reducibleElementTypeOrThrow(argument_type, result_type_, aggregation_))
    , result_type(resultTypeFor(element_type, aggregation_))
    , result_data_type(result_type_.getPtr())
    , aggregation(aggregation_)
    , batch_bytes(batchBytesFor(batch_bytes_, element_type, max_batch_rows))
    , reduction(std::make_unique<CudfReduction>(element_type, result_type, aggregation))
    , pipe(argument_type, std::min(batch_bytes, max_stage_bytes))
{
}

void GPUAccumulator::add(const IColumn & column)
{
    pipe.stage(column);
    if (pipe.stagedBytes() >= batch_bytes)
        reduceBatchOnDevice();
}

void GPUAccumulator::reduceBatchOnDevice()
{
    if (pipe.stagedRows() == 0)
        return;

    Stopwatch watch;
    const DeviceColumnView values = pipe.flush().view();
    if (values.rows() != 0)
        reduction->addBatch(values);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, values.rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches, 1);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    pipe.reset();
}

Field GPUAccumulator::finalize()
{
    reduceBatchOnDevice();

    Stopwatch watch;
    const DeviceColumnView result = reduction->finalize();
    if (result.rows() == 0)
        return result_data_type->getDefault();

    MutableColumnPtr column = result_data_type->createColumn();
    IColumn * const to = column.get();
    copyDeviceToHost(std::span<const DeviceColumnView>(&result, 1), std::span<IColumn * const>(&to, 1), GPUResources::get().compute.get());
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());

    return (*column)[0];
}

namespace
{

/// Of rows collected rather than grouped, the values are carried by their bytes, see `collectElementTypeOf`.
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

std::vector<GPUGroupByAggregate> aggregatesOrThrow(size_t num_columns, std::vector<GPUGroupByAggregate> aggregates, bool keyed)
{
    /// Without aggregates, the rows are collected, not aggregated, which they are not by keys.
    if (keyed && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation with keys and without aggregates");

    for (const auto & aggregate : aggregates)
    {
        if (aggregate.column != GPUGroupByAggregate::no_column && aggregate.column >= num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregate of value {} of {}", aggregate.column, num_columns);
    }
    return aggregates;
}

std::vector<GPUElementType> withDerived(std::vector<GPUElementType> values, const std::vector<GPUDerivedValue> & derived)
{
    for (const auto & value : derived)
        values.push_back(value.expression.value_type);
    return values;
}

size_t rowBytesOf(
    const std::vector<GPUElementType> & key_element_types,
    const std::vector<GPUElementType> & values,
    const std::vector<GPUElementType> & filter_element_types)
{
    size_t bytes = 0;
    for (const GPUElementType key_element_type : key_element_types)
        bytes += sizeOf(key_element_type);
    for (const GPUElementType value : values)
        bytes += sizeOf(dataElementTypeOf(value));
    for (const GPUElementType filter_element_type : filter_element_types)
        bytes += sizeOf(filter_element_type);
    return bytes;
}

std::vector<ColumnUploadPipe> pipesFor(const DataTypes & types, size_t batch_rows)
{
    std::vector<ColumnUploadPipe> pipes;
    pipes.reserve(types.size());
    for (const auto & type : types)
    {
        const GPUElementType column_type = uploadTypeOrThrow(*type);
        const size_t stage_bytes = columnKindOf(column_type) == GPUColumnKind::Fixed
            ? std::min(batch_rows * sizeOf(column_type), GPUAccumulator::max_stage_bytes)
            : GPUAccumulator::max_stage_bytes;
        pipes.emplace_back(*type, stage_bytes);
    }
    return pipes;
}

std::vector<GPUElementType> keyElementTypesOf(const std::vector<GPUElementType> & keys)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(keys.size());
    for (const auto & key : keys)
        element_types.push_back(dataElementTypeOf(key));
    return element_types;
}

/// The keys and then the values that are of varying width, by their place among the columns.
std::vector<size_t> variableColumnIndicesOf(const std::vector<GPUElementType> & keys, const std::vector<GPUElementType> & values)
{
    std::vector<size_t> indices;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (columnKindOf(keys[i]) == GPUColumnKind::Variable)
            indices.push_back(i);
    }
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (columnKindOf(values[i]) == GPUColumnKind::Variable)
            indices.push_back(keys.size() + i);
    }
    return indices;
}


}

GroupByGPUAccumulator::GroupByGPUAccumulator(
    const DataTypes & key_types,
    const DataTypes & value_types,
    std::vector<GPUGroupByAggregate> aggregates_,
    size_t batch_bytes)
    : batch_rows(std::clamp(
          batch_bytes / rowBytesOf(keyElementTypesOf(groupByKeysOrThrow(key_types)), valueTypesOrThrow(value_types, false), {}),
          size_t{1},
          max_batch_rows))
    , key_pipes(pipesFor(key_types, batch_rows))
    , value_pipes(pipesFor(value_types, batch_rows))
    , lane(groupByKeysOrThrow(key_types), valueTypesOrThrow(value_types, /*collect=*/false), std::move(aggregates_), {}, std::nullopt, /*num_parts=*/ 1)
{
}

void GroupByGPUAccumulator::add(const Columns & key_columns, const Columns & value_columns)
{
    if (key_columns.size() != key_pipes.size() || value_columns.size() != value_pipes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A block of {} key and {} value columns for a GPU aggregation over {} and {}",
            key_columns.size(),
            value_columns.size(),
            key_pipes.size(),
            value_pipes.size());

    const size_t num_rows = value_columns.front()->size();
    if (num_rows == 0)
        return;

    if (stagedRows() + num_rows > max_batch_rows)
        sendBatchToDevice();

    for (size_t i = 0; i < key_columns.size(); ++i)
        key_pipes[i].stage(*key_columns[i]);

    for (size_t i = 0; i < value_columns.size(); ++i)
        value_pipes[i].stage(*value_columns[i]);

    if (stagedRows() >= batch_rows || stagedVariableBytes() >= GPUAccumulator::max_stage_bytes)
        sendBatchToDevice();
}

size_t GroupByGPUAccumulator::stagedVariableBytes() const
{
    size_t bytes = 0;
    for (const auto * pipes : {&key_pipes, &value_pipes})
    {
        for (const auto & pipe : *pipes)
        {
            if (columnKindOf(pipe.type()) == GPUColumnKind::Variable)
                bytes += pipe.stagedBytes();
        }
    }
    return bytes;
}

void GroupByGPUAccumulator::sendBatchToDevice()
{
    if (stagedRows() == 0)
        return;

    std::vector<DeviceColumnView> keys;
    keys.reserve(key_pipes.size());
    for (auto & pipe : key_pipes)
        keys.push_back(pipe.flush().view());

    std::vector<DeviceColumnView> value_columns;
    value_columns.reserve(value_pipes.size());
    for (auto & pipe : value_pipes)
        value_columns.push_back(pipe.flush().view());

    /// The pipes send their columns in the order of the compute stream, and stage the next batch into them once the
    /// lane is done with them.
    const cuda::stream_ref compute = GPUResources::get().compute;
    const EventPtr filled = createEvent();
    checkCuda(cudaEventRecord(filled.get(), compute.get()), "Cannot mark a point in the compute stream");
    checkCuda(cudaStreamWaitEvent(lane.stream().get(), filled.get(), 0), "Cannot make a lane wait for the compute stream");
    lane.add(keys, value_columns, {});
    const EventPtr grouped = createEvent();
    checkCuda(cudaEventRecord(grouped.get(), lane.stream().get()), "Cannot mark a point in a lane's stream");
    checkCuda(cudaStreamWaitEvent(compute.get(), grouped.get(), 0), "Cannot make the compute stream wait for a lane");

    for (auto & pipe : key_pipes)
        pipe.reset();
    for (auto & pipe : value_pipes)
        pipe.reset();
}

size_t GroupByGPUAccumulator::finalize()
{
    sendBatchToDevice();
    return lane.finalize();
}

namespace
{

/// The rows of a batch on the device, at most: what cuDF takes as the rows of a column.
constexpr size_t max_compressed_batch_rows = (1UL << 31) - 1;

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
    , variable_columns(variableColumnIndicesOf(keys, values))
    , batch_rows(std::clamp(batch_bytes / rowBytesOf(key_element_types, values, filter_types), size_t{1}, max_compressed_batch_rows))
{
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

void GPUStagedBatch::Column::clear()
{
    staged.clear();
    blocks.clear();
    packed.clear();
    codec.reset();
    raw = false;
    decompressed_bytes = 0;
    skipped_bytes = 0;
    kept_bytes = 0;
    cut = false;
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

GPUStagedBatch::Column & GPUBlockStager::columnOrThrow(size_t column)
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
        const GPUStagedBatch::Column & column = current->columns[c];
        if (column.staged.size() >= max_column_bytes)
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
    const auto codec = deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A block of compression method {:#x} for the GPU, which the device does not expand", static_cast<UInt16>(method));

    if (skipped_bytes + kept_bytes > decompressed_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "{} bytes kept after {} skipped of a block of {} for the GPU", kept_bytes, skipped_bytes, decompressed_bytes);

    /// A batch has one run of bytes of each column: it starts at a block that skips bytes, and ends at one cut short.
    {
        const GPUStagedBatch::Column & column = columnOrThrow(column_index);
        if (column.raw || column.cut || (!column.blocks.empty() && (skipped_bytes != 0 || column.codec != codec)))
            seal();
    }

    GPUStagedBatch::Column & column = current->columns[column_index];
    if (column.blocks.empty())
    {
        column.skipped_bytes = skipped_bytes;
        column.staged.reserve(std::min(max_column_bytes, std::max(expected_bytes, payload.size())));
    }
    column.kept_bytes += kept_bytes;
    column.cut = skipped_bytes + kept_bytes != decompressed_bytes;
    column.codec = codec;
    stageCompressedBlock(column.staged, column.blocks, column.packed, method, payload, decompressed_bytes);
    column.decompressed_bytes += decompressed_bytes;

    if (isFull())
        seal();
}

std::span<char> GPUBlockStager::reserveRawBytes(size_t column_index, size_t max_bytes, size_t expected_bytes)
{
    {
        const GPUStagedBatch::Column & column = columnOrThrow(column_index);
        if (!column.blocks.empty() || column.staged.size() >= max_column_bytes)
            seal();
    }

    GPUStagedBatch::Column & column = current->columns[column_index];
    column.raw = true;
    column.staged.reserve(std::min(max_column_bytes, column.staged.size() + std::max<size_t>(expected_bytes, 1)));
    return {
        column.staged.data() + column.staged.size(),
        std::min({max_bytes, max_column_bytes - column.staged.size(), column.staged.capacity() - column.staged.size()})};
}

void GPUBlockStager::commitRawBytes(size_t column_index, size_t bytes)
{
    GPUStagedBatch::Column & column = columnOrThrow(column_index);
    growBy(column.staged, bytes);
    column.decompressed_bytes += bytes;
    column.kept_bytes += bytes;

    if (isFull())
        seal();
}

void GPUBlockStager::finishPart(size_t num_rows)
{
    seal(num_rows);
}

DeviceColumnView GPUDeviceBatch::column(size_t column) const
{
    if (column >= layout->firstSizesColumn() || layout->isAliased(column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of a GPU batch is not one of its values", column);

    if (layout->isVariable(column))
    {
        const size_t ordinal = layout->variableOrdinalOf(column);
        return DeviceVariableColumn{
            .offsets = reinterpret_cast<const uint64_t *>(starts[layout->sizesColumnOf(column)]),
            .chars = starts[column],
            .rows = num_rows,
            .chars_bytes = variable_bytes[ordinal],
        };
    }
    return DeviceFixedColumn{.element_type = layout->elementTypeOf(column), .data = starts[column], .rows = num_rows};
}

std::vector<DeviceColumnView> GPUDeviceBatch::keys() const
{
    std::vector<DeviceColumnView> columns;
    for (size_t i = 0; i < layout->keys.size(); ++i)
        columns.push_back(column(i));
    return columns;
}

std::vector<DeviceColumnView> GPUDeviceBatch::values() const
{
    std::vector<DeviceColumnView> columns;
    for (size_t i = 0; i < layout->values.size(); ++i)
        columns.push_back(column(layout->keys.size() + i));
    return columns;
}

std::vector<DeviceFixedColumn> GPUDeviceBatch::filterColumns() const
{
    /// A column of the `WHERE` that is a key or a value as well reads the bytes of that column.
    std::vector<DeviceFixedColumn> columns;
    for (size_t i = 0; i < layout->filter_types.size(); ++i)
    {
        const size_t c = layout->firstFilterColumn() + i;
        columns.push_back(column(layout->isAliased(c) ? layout->aliasOf(c) : c).fixed);
    }
    return columns;
}

void GPUDeviceBatch::moveTo(cuda::stream_ref stream)
{
    const EventPtr ready = createEvent();
    checkCuda(cudaEventRecord(ready.get(), ready_stream.get()), "Cannot mark a point in an uploader's stream");
    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for a batch");
    for (auto & buffer : buffers)
        buffer.setStream(stream);
    ready_stream = stream;
}

GPUBatchUploader::GPUBatchUploader(std::shared_ptr<const GPUCompressedLayout> layout_)
    : layout(std::move(layout_)), decompression(cuda::stream_ref{stream.get()})
{
    carried.reserve(layout->numColumns());
    for (size_t c = 0; c < layout->numColumns(); ++c)
        carried.emplace_back(cuda::stream_ref{stream.get()});
}

std::optional<GPUDeviceBatch> GPUBatchUploader::upload(const GPUStagedBatch & staged)
{
    const cuda::stream_ref own{stream.get()};
    const size_t num_columns = layout->numColumns();
    const size_t first_sizes = layout->firstSizesColumn();
    const auto & variable_columns = layout->variableColumns();

    if (staged.columns.size() != num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A staged batch of {} columns for a GPU read of {}", staged.columns.size(), num_columns);

    Stopwatch expansion_watch;

    GPUDeviceBatch batch(layout, own);
    batch.buffers.reserve(num_columns);
    batch.starts.assign(num_columns, nullptr);

    /// Of each column, the bytes in its buffer: what was carried, then what the staged blocks expand to. Of a column of
    /// sizes, the rows whose offsets it holds.
    std::vector<size_t> sizes(num_columns, 0);
    std::vector<size_t> carried_rows(num_columns, 0);

    size_t compressed_total = 0;
    for (const auto & column : staged.columns)
        if (!column.raw)
            compressed_total += column.staged.size();
    DeviceBuffer uploaded(own);
    char * const uploaded_base = compressed_total ? uploaded.grow(compressed_total) : nullptr;
    size_t uploaded_at = 0;

    std::vector<std::pair<GPUCodec, std::vector<CompressedPiece>>> pieces;
    const auto pieces_of = [&](GPUCodec codec) -> std::vector<CompressedPiece> &
    {
        for (auto & [of, list] : pieces)
            if (of == codec)
                return list;
        return pieces.emplace_back(codec, std::vector<CompressedPiece>{}).second;
    };

    for (size_t c = 0; c < num_columns; ++c)
    {
        DeviceBuffer & buffer = batch.buffers.emplace_back(own);
        if (layout->isAliased(c))
            continue;

        const GPUStagedBatch::Column & column = staged.columns[c];
        const DeviceBuffer & carried_of_column = carried[c];
        const size_t expanded = column.raw ? column.decompressed_bytes : valueBytesOf(column.blocks, column.packed);
        if (column.raw && column.staged.size() != expanded)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} raw bytes of column {} staged for the GPU, {} counted", column.staged.size(), c, expanded);

        char * destination = nullptr;
        if (layout->isSizes(c))
        {
            if (column.skipped_bytes != 0 || column.cut)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
            const size_t carried_entries = carried_of_column.empty() ? 1 : carried_of_column.size() / sizeof(UInt64);
            char * const base = buffer.grow(carried_entries * sizeof(UInt64) + expanded);
            if (carried_of_column.empty())
                checkCuda(cudaMemsetAsync(base, 0, sizeof(UInt64), own.get()), "Cannot start the offsets of a column of varying width");
            else
                checkCuda(
                    cudaMemcpyAsync(base, carried_of_column.data(), carried_of_column.size(), cudaMemcpyDeviceToDevice, own.get()),
                    "Cannot carry offsets on the device");
            destination = base + carried_entries * sizeof(UInt64);
            batch.starts[c] = base;
            carried_rows[c] = carried_entries - 1;
            sizes[c] = carried_rows[c] + column.kept_bytes / sizeof(UInt64);
        }
        else
        {
            /// The blocks expand right after where the carried bytes end, the bytes they skip over the end of those, which
            /// are copied in once they are expanded.
            const size_t carried_bytes = carried_of_column.size();
            const size_t lead = std::max(carried_bytes, column.skipped_bytes);
            char * const base = buffer.grow(lead - column.skipped_bytes + expanded);
            destination = base + lead - column.skipped_bytes;
            batch.starts[c] = base + lead - carried_bytes;
            sizes[c] = carried_bytes + column.kept_bytes;
        }

        if (expanded == 0)
            continue;
        if (column.raw)
        {
            checkCuda(
                cudaMemcpyAsync(destination, column.staged.data(), column.staged.size(), cudaMemcpyHostToDevice, own.get()),
                "Cannot send raw bytes to the device");
            continue;
        }
        char * const at = uploaded_base + uploaded_at;
        checkCuda(
            cudaMemcpyAsync(at, column.staged.data(), column.staged.size(), cudaMemcpyHostToDevice, own.get()),
            "Cannot send compressed blocks to the device");
        pieces_of(*column.codec).push_back({
            .device_compressed = at,
            .compressed_bytes = column.staged.size(),
            .blocks = column.blocks,
            .packed = column.packed,
            .destination = destination,
        });
        uploaded_at += column.staged.size();
    }

    /// The staged memory is free once the uploads are done; nvcomp waits for them as well.
    checkCuda(cudaEventRecord(staged_sent.get(), own.get()), "Cannot mark a point in an uploader's stream");
    for (auto & [codec, list] : pieces)
    {
        decompression.launch(codec, list, nullptr, staged_sent.get());
        decompression.waitAndCheck();
    }
    checkCuda(cudaEventSynchronize(staged_sent.get()), "Cannot wait for the uploads of a batch");

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
        if (fresh != 0)
            offsetsFromSizes(offsets + carried_rows[o] + 1, fresh, offsets + carried_rows[o], own);
    }

    ProfileEvents::increment(ProfileEvents::GPUDecompressionBytes, compressed_total);
    ProfileEvents::increment(ProfileEvents::GPUDecompressionMicroseconds, expansion_watch.elapsedMicroseconds());

    /// The rows every column has: of a `String`, those whose bytes are all there.
    Stopwatch wait_watch;
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
    ProfileEvents::increment(ProfileEvents::GPUDeviceWaitMicroseconds, wait_watch.elapsedMicroseconds());

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

    /// What the batch does not take is carried to the next one, copied in the order of this stream before the batch
    /// goes to another.
    for (size_t c = 0; c < num_columns; ++c)
    {
        if (layout->isAliased(c))
            continue;

        DeviceBuffer next(own);
        if (layout->isSizes(c))
        {
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
    return batch;
}

GPUGroupingLane::GPUGroupingLane(
    std::vector<GPUElementType> keys_,
    std::vector<GPUElementType> values_,
    std::vector<GPUGroupByAggregate> aggregates_,
    std::vector<GPUDerivedValue> derived_,
    std::optional<GPUExpression> filter_,
    size_t num_parts)
    : keys(std::move(keys_))
    , values(std::move(values_))
    , derived(std::move(derived_))
    , aggregates(aggregatesOrThrow(values.size() + derived.size(), std::move(aggregates_), !keys.empty()))
    , filter(std::move(filter_))
{
    for (const auto & value : derived)
    {
        for (const uint32_t column : value.columns)
        {
            if (column >= values.size() || columnKindOf(values[column]) != GPUColumnKind::Fixed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A value the GPU derives from value {} of {}", column, values.size());
        }
    }

    if (aggregates.empty())
        collector = std::make_unique<CudfRowCollector>(values, stream());
    else
        group_by = std::make_unique<CudfGroupBy>(keys, withDerived(values, derived), derived, aggregates, num_parts, stream());
}

void GPUGroupingLane::add(GPUDeviceBatch batch)
{
    batch.moveTo(stream());
    add(batch.keys(), batch.values(), batch.filterColumns());
}

void GPUGroupingLane::add(
    const std::vector<DeviceColumnView> & key_columns,
    const std::vector<DeviceColumnView> & value_columns,
    const std::vector<DeviceFixedColumn> & filter_columns)
{
    if (filter_columns.size() != (filter ? filter->num_columns : 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A batch of {} columns of a `WHERE` for a GPU lane", filter_columns.size());

    Stopwatch watch;
    if (collector)
        collector->addBatch(value_columns, filter_columns, filter ? &*filter : nullptr);
    else
        group_by->addBatch(key_columns, value_columns, filter_columns, filter ? &*filter : nullptr);

    ProfileEvents::increment(ProfileEvents::GPUAggregationRows, value_columns.empty() ? 0 : value_columns.front().rows());
    ProfileEvents::increment(ProfileEvents::GPUAggregationBatches);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

MutableColumns GPUGroupingLane::takeCollected(const DataTypes & types)
{
    if (types.size() != values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} types of {} columns collected on the GPU", types.size(), values.size());

    MutableColumns columns;
    const size_t rows = collector->take();
    if (rows == 0)
        return columns;

    Stopwatch watch;
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    columns.reserve(types.size());
    from.reserve(types.size());
    to.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i)
    {
        const DeviceColumnView value = collector->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of column {} of {} collected", value.rows(), i, rows);
        columns.push_back(types[i]->createColumn());
        from.push_back(value);
        to.push_back(columns.back().get());
    }

    copyDeviceToHost(from, to, stream());
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return columns;
}

void GPUGroupingLane::absorbPart(const GPUGroupingLane & other, size_t part)
{
    Stopwatch watch;
    group_by->absorbPart(*other.group_by, part);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

size_t GPUGroupingLane::finalize()
{
    Stopwatch watch;
    num_groups = group_by->finalize();
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

size_t GPUGroupingLane::filterGroups(const GPUExpression & expression, const std::vector<uint32_t> & columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were filtered before it was finalized");

    Stopwatch watch;
    num_groups = group_by->filterGroups(expression, columns);
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
    return *num_groups;
}

void GPUGroupingLane::copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns)
{
    if (!num_groups)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The groups of a GPU aggregation were asked for before it was finalized");

    const size_t rows = *num_groups;
    const size_t num_values = aggregates.empty() ? values.size() : aggregates.size();
    if (key_columns.size() != keys.size() || value_columns.size() != num_values)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} key and {} aggregate columns for the groups of a GPU aggregation of {} keys and {} aggregates",
            key_columns.size(),
            value_columns.size(),
            keys.size(),
            num_values);

    if (rows == 0)
        return;

    Stopwatch watch;
    std::vector<DeviceColumnView> from;
    std::vector<IColumn *> to;
    from.reserve(keys.size() + num_values);
    to.reserve(keys.size() + num_values);
    for (size_t i = 0; i < keys.size(); ++i)
    {
        const DeviceColumnView key = group_by->key(i);
        if (key.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of key {} of {} groups", key.rows(), i, rows);
        from.push_back(key);
        to.push_back(key_columns[i].get());
    }
    for (size_t i = 0; i < num_values; ++i)
    {
        const DeviceColumnView value = group_by->value(i);
        if (value.rows() != rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The device holds {} rows of aggregate {} of {} groups", value.rows(), i, rows);
        from.push_back(value);
        to.push_back(value_columns[i].get());
    }

    copyDeviceToHost(from, to, stream());
    ProfileEvents::increment(ProfileEvents::GPUAggregationMicroseconds, watch.elapsedMicroseconds());
}

}

#endif
