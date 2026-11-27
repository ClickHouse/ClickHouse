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
    for (size_t c = 0; c < column_types.size(); ++c)
        carried.push_back({.values = DeviceBuffer(own), .offsets = DeviceBuffer(own), .size_tail = DeviceBuffer(own)});
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

/// A column of a batch while the device expands it: the rows carried from the batch before, then those of its staged
/// blocks, of a `String` its bytes so and the offsets of its rows, carried and from its sizes. `take` makes it the
/// `GPUDeviceColumn` of the rows of the batch.
class GPUBatchUploader::ExpandingColumn
{
public:
    /// Places the column in memory of its own, and adds its blocks to `expansion`.
    ExpandingColumn(
        GPUElementType type_, const GPUHostColumn & staged, const Carried & carried_column, DecompressionBatch & expansion, cuda::stream_ref own)
        : type(type_)
        , values_buffer(own)
        , sizes_buffer(own)
    {
        /// The carried bytes are copied over the skipped ones; the padding aligns the start of the column.
        const CompressedHostColumn & staged_values = staged.values;
        const size_t carried_bytes = carried_column.values.size();
        const size_t lead = std::max(carried_bytes, staged_values.skipped_bytes);
        const size_t element = isVariable(type) ? 1 : sizeOf(type);
        const size_t padding = (element - (lead - carried_bytes) % element) % element;
        char * const base = values_buffer.grow(padding + lead - staged_values.skipped_bytes + staged_values.expandedBytes());
        values = base + padding + lead - carried_bytes;
        values_bytes = carried_bytes + staged_values.kept_bytes;
        expansion.add(staged_values, base + padding + lead - staged_values.skipped_bytes);

        if (!isVariable(type))
            return;

        /// The offsets carried, then the bytes of the size that the batch before ended within, then the sizes.
        const CompressedHostColumn & staged_sizes = staged.sizes;
        if (staged_sizes.skipped_bytes != 0 || staged_sizes.isCut())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a column of varying width for the GPU read in ranges of marks");
        const size_t carried_entries = carried_column.offsets.empty() ? 1 : carried_column.offsets.size() / sizeof(UInt64);
        const DeviceBuffer & tail = carried_column.size_tail;
        char * const sizes_base = sizes_buffer.grow(carried_entries * sizeof(UInt64) + tail.size() + staged_sizes.expandedBytes());
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
        expansion.add(staged_sizes, first_size + tail.size());
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

    /// The column of the first `rows`, the rest of which goes to `carried_column`. Of the end of a part, nothing may be
    /// left.
    GPUDeviceColumn take(size_t rows, bool part_ends, size_t index, Carried & carried_column, cuda::stream_ref own) &&
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
            return GPUDeviceColumn(
                std::move(values_buffer), std::move(sizes_buffer), DeviceFixedColumn{.element_type = type, .data = values, .rows = rows});

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

        const DeviceVariableColumn view{.offsets = sizes->offsets, .chars = values, .rows = rows, .chars_bytes = taken_bytes};
        return GPUDeviceColumn(std::move(values_buffer), std::move(sizes_buffer), view);
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
    DeviceBuffer values_buffer;
    /// Of a `String` only: its offsets.
    DeviceBuffer sizes_buffer;
    /// The values, or the bytes of a `String`, from its first row, in `values_buffer`.
    char * values = nullptr;
    size_t values_bytes = 0;
    std::optional<Sizes> sizes;
};

std::optional<GPUFuture<GPUDeviceBatch>> GPUBatchUploader::expand(const GPUHostBatch & staged)
{
    const cuda::stream_ref own{stream->get()};
    const size_t num_columns = column_types.size();
    if (staged.columns.size() != num_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A staged batch of {} columns for a GPU read of {}", staged.columns.size(), num_columns);

    GPUPromise<GPUDeviceBatch> promise(GPUDeviceBatch(stream), own);

    std::vector<ExpandingColumn> columns;
    columns.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        columns.emplace_back(column_types[c], staged.columns[c], carried[c], decompression, own);

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
    GPUDeviceBatch & batch = promise.value();
    batch.device_columns.reserve(num_columns);
    for (size_t c = 0; c < num_columns; ++c)
        batch.device_columns.push_back(std::move(columns[c]).take(rows, staged.part_rows.has_value(), c, carried[c], own));
    batch.num_rows = rows;

    part_rows_sent = staged.part_rows ? 0 : part_rows_sent + rows;
    if (rows == 0)
        return std::nullopt;
    return std::move(promise).fulfill();
}

}

#endif
