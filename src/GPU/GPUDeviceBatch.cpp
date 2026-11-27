#include <GPU/GPUDeviceBatch.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/OffsetsKernels.cuh>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

/// The bytes the sizes of a `String` column may add up to, as `MAX_TOTAL_STRING_SIZE` of `SerializationString`.
constexpr UInt64 max_total_string_bytes = 1ULL << 48;

bool isVariable(GPUElementType type)
{
    return columnKindOf(type) == GPUColumnKind::Variable;
}

}

GPUDeviceBatch::GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_)
    : upload_stream(std::move(upload_stream_))
{
}

GPUDeviceBatch::~GPUDeviceBatch()
{
    if (!released || !upload_stream)
        return;

    /// The uploader's stream frees the memory, so it must wait for the consumer's stream.
    const cuda::stream_ref consumer = released->on();
    try
    {
        std::move(*released).fulfill().waitOn(cuda::stream_ref{upload_stream->get()});
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        cudaStreamSynchronize(consumer.get());
    }
}

void GPUDeviceBatch::useOn(cuda::stream_ref stream)
{
    if (released)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch taken on a stream twice");
    released.emplace(stream);
}

GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream)
{
    GPUDeviceBatch batch = std::move(future).waitOn(stream);
    batch.useOn(stream);
    return batch;
}

GPUBatchUploader::GPUBatchUploader(std::vector<GPUElementType> column_types_)
    : stream(std::make_shared<StreamPtr>(createStream()))
    , column_types(std::move(column_types_))
    , decompression(cuda::stream_ref{stream->get()})
{
    const cuda::stream_ref own{stream->get()};
    carried.reserve(column_types.size());
    for (const GPUElementType type : column_types)
        carried.push_back(GPUDeviceColumn(type, own));
}

std::optional<GPUFuture<GPUDeviceBatch>> GPUBatchUploader::sendHostToDevice(const GPUHostBatch & staged)
{
    if (failed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A batch for a GPU uploader that failed on one before");

    /// What is carried does not follow the rows sent anymore if it throws.
    failed = true;
    std::optional<GPUFuture<GPUDeviceBatch>> batch;
    waitOnFailure(cuda::stream_ref{stream->get()}, [&]
    {
        if (auto expanded = expand(staged))
            batch.emplace(std::move(*expanded));
    });
    failed = false;
    return batch;
}

GPUDeviceColumn::GPUDeviceColumn(GPUElementType type_, cuda::stream_ref stream)
    : type(type_)
    , values_memory(stream)
    , offsets_memory(stream)
    , tail_memory(stream)
{
}

const DeviceColumnView & GPUDeviceColumn::view() const
{
    if (!column_view)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The view of a GPU column before it is split from what is carried");
    return *column_view;
}

bool GPUDeviceColumn::isVariable() const
{
    return GPU::isVariable(type);
}

GPUDeviceColumn::GPUDeviceColumn(
    const GPUDeviceColumn & carried, const GPUHostColumn & staged, DecompressionBatch & decompression, cuda::stream_ref stream)
    : GPUDeviceColumn(carried.type, stream)
{
    /// The carried bytes are copied over the skipped ones; the padding aligns the start of the column.
    const CompressedHostColumn & staged_values = staged.values;
    const size_t carried_bytes = carried.values_bytes;
    const size_t lead = std::max(carried_bytes, staged_values.skipped_bytes);
    const size_t element = isVariable() ? 1 : sizeOf(type);
    const size_t padding = (element - (lead - carried_bytes) % element) % element;
    char * const base = values_memory.grow(padding + lead - staged_values.skipped_bytes + staged_values.expandedBytes());
    values = base + padding + lead - carried_bytes;
    values_bytes = carried_bytes + staged_values.kept_bytes;
    decompression.add(staged_values, base + padding + lead - staged_values.skipped_bytes);

    if (!isVariable())
        return;

    /// The offsets carried, then the bytes of the size that the batch before ended within, then the sizes.
    const CompressedHostColumn & staged_sizes = staged.sizes;
    if (staged_sizes.skipped_bytes != 0 || staged_sizes.isCut())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
    const size_t carried_entries = carried.offsets_memory.empty() ? 1 : carried.offsets_memory.size() / sizeof(UInt64);
    const DeviceBuffer & carried_tail = carried.tail_memory;
    char * const sizes_base = offsets_memory.grow(carried_entries * sizeof(UInt64) + carried_tail.size() + staged_sizes.expandedBytes());
    if (carried.offsets_memory.empty())
        checkCuda(cudaMemsetAsync(sizes_base, 0, sizeof(UInt64), stream.get()), "Cannot start the offsets of a column of varying width");
    else
        checkCuda(
            cudaMemcpyAsync(sizes_base, carried.offsets_memory.data(), carried.offsets_memory.size(), cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot carry offsets on the device");
    char * const first_size = sizes_base + carried_entries * sizeof(UInt64);
    if (!carried_tail.empty())
        checkCuda(
            cudaMemcpyAsync(first_size, carried_tail.data(), carried_tail.size(), cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot carry the bytes of a string size on the device");

    offsets = reinterpret_cast<uint64_t *>(sizes_base);
    carried_rows = carried_entries - 1;
    const size_t size_bytes = carried_tail.size() + staged_sizes.kept_bytes;
    sized_rows = carried_rows + size_bytes / sizeof(UInt64);
    tail_bytes = size_bytes % sizeof(UInt64);
    tail = first_size + size_bytes - tail_bytes;
    decompression.add(staged_sizes, first_size + carried_tail.size());
}

void GPUDeviceColumn::finish(const GPUDeviceColumn & carried, cuda::stream_ref stream)
{
    if (carried.values_bytes != 0)
        checkCuda(
            cudaMemcpyAsync(values, carried.values, carried.values_bytes, cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot carry bytes on the device");

    if (!isVariable() || sized_rows == carried_rows)
        return;
    /// The sizes are untrusted: bounded as on the CPU, from the carried offset, before they become offsets.
    const size_t fresh = sized_rows - carried_rows;
    uint64_t * const fresh_offsets = offsets + carried_rows;
    if (endOfSizesUpTo(fresh_offsets, fresh_offsets + 1, fresh, max_total_string_bytes, stream) > max_total_string_bytes)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The sizes of a String column read on the GPU declare more than {} bytes, most likely the data is corrupted",
            max_total_string_bytes);
    offsetsFromSizes(fresh_offsets + 1, fresh, fresh_offsets, stream);
}

size_t GPUDeviceColumn::completeRows(cuda::stream_ref stream)
{
    if (!isVariable())
        return values_bytes / sizeOf(type);
    const CoveredRows covered = rowsCoveredBy(offsets, sized_rows, values_bytes, stream);
    covered_rows = covered.rows;
    covered_bytes = covered.bytes;
    return covered_rows;
}

GPUDeviceColumn GPUDeviceColumn::splitAt(size_t rows, bool part_ends, size_t index, cuda::stream_ref stream)
{
    size_t taken_bytes = rows * (isVariable() ? 1 : sizeOf(type));
    if (isVariable())
        taken_bytes = covered_rows == rows ? covered_bytes : rowsCoveredBy(offsets, rows, values_bytes, stream).bytes;

    if (part_ends)
        checkPartEnds(rows, taken_bytes, index);

    GPUDeviceColumn rest(type, stream);
    if (const size_t left = values_bytes - taken_bytes; left != 0)
    {
        rest.values = rest.values_memory.grow(left);
        rest.values_bytes = left;
        checkCuda(cudaMemcpyAsync(rest.values, values + taken_bytes, left, cudaMemcpyDeviceToDevice, stream.get()), "Cannot carry bytes on the device");
    }

    if (!isVariable())
    {
        column_view.emplace(DeviceFixedColumn{.element_type = type, .data = values, .rows = rows});
        return rest;
    }

    if (tail_bytes != 0)
        checkCuda(
            cudaMemcpyAsync(rest.tail_memory.grow(tail_bytes), tail, tail_bytes, cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot carry the bytes of a string size on the device");
    if (const size_t sizes_left = sized_rows - rows; sizes_left != 0)
        subtractFromOffsets(
            offsets + rows, sizes_left + 1, taken_bytes, reinterpret_cast<uint64_t *>(rest.offsets_memory.grow((sizes_left + 1) * sizeof(UInt64))), stream);

    column_view.emplace(DeviceVariableColumn{.offsets = offsets, .chars = values, .rows = rows, .chars_bytes = taken_bytes});
    return rest;
}

void GPUDeviceColumn::checkPartEnds(size_t rows, size_t taken_bytes, size_t index) const
{
    if (!isVariable())
    {
        if (values_bytes != taken_bytes)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Column {} of a part came to {} bytes on the device, of which its {} rows take {}", index, values_bytes, rows, taken_bytes);
        return;
    }
    if (tail_bytes != 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "The sizes of a String column of a part read on the GPU end within a size, the data is corrupted");
    if (sized_rows != rows || taken_bytes != values_bytes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of varying width of a part came to {} sizes and {} bytes on the device, of which its {} rows take {}",
            index,
            sized_rows,
            values_bytes,
            rows,
            taken_bytes);
}

std::optional<GPUFuture<GPUDeviceBatch>> GPUBatchUploader::expand(const GPUHostBatch & staged)
{
    const cuda::stream_ref own{stream->get()};
    const size_t num_columns = column_types.size();
    if (staged.columns.size() != num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A staged batch of {} columns for a GPU read of {}", staged.columns.size(), num_columns);

    GPUPromise<GPUDeviceBatch> promise(GPUDeviceBatch(stream), own);

    std::vector<GPUDeviceColumn> columns;
    columns.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        columns.push_back(GPUDeviceColumn(carried[c], staged.columns[c], decompression, own));

    /// The host batch is free once it returns.
    decompression.expand().waitOn(own);
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
    std::vector<GPUDeviceColumn> rest;
    rest.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        rest.push_back(columns[c].splitAt(rows, staged.part_rows.has_value(), c, own));
    carried = std::move(rest);
    GPUDeviceBatch & batch = promise.value();
    batch.device_columns = std::move(columns);
    batch.num_rows = rows;

    part_rows_sent = staged.part_rows ? 0 : part_rows_sent + rows;
    if (rows == 0)
        return std::nullopt;
    return std::move(promise).fulfill();
}

}

#endif
