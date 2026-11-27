#include <GPU/GPUDeviceBatch.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/OffsetsKernels.cuh>

#include <Common/Exception.h>

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

/// What cuDF takes as the rows of a column.
constexpr size_t max_batch_rows = (1UL << 31) - 1;

}

GPUDeviceBatch::GPUDeviceBatch(std::shared_ptr<StreamPtr> copy_stream_)
    : copy_stream(std::move(copy_stream_))
{
}

GPUDeviceBatch::~GPUDeviceBatch()
{
    if (!released || !copy_stream)
        return;

    /// The stream it was copied on frees the memory, so it must wait for the stream it was taken on.
    const cuda::stream_ref consumer = released->on();
    try
    {
        std::move(*released).fulfill().waitOn(cuda::stream_ref{copy_stream->get()});
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

const DeviceColumnView & GPUDeviceColumn::view() const
{
    if (!column_view)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The view of a GPU column before its blocks are expanded");
    return *column_view;
}

bool GPUDeviceColumn::isVariable() const
{
    return columnKindOf(type) == GPUColumnKind::Variable;
}

GPUDeviceColumn::GPUDeviceColumn(const GPUHostColumn & host, DecompressionBatch & decompression, cuda::stream_ref stream)
    : type(host.type)
    , values_memory(stream)
    , offsets_memory(stream)
{
    /// The first block expands whole: the rows start after the bytes it skips, which the padding aligns.
    const CompressedHostColumn & host_values = host.values;
    const size_t element = isVariable() ? 1 : sizeOf(type);
    const size_t padding = (element - host_values.skippedBytes() % element) % element;
    char * const base = values_memory.grow(padding + host_values.expandedBytes());
    values = base + padding + host_values.skippedBytes();
    values_bytes = host_values.keptBytes();
    decompression.add(host_values, base + padding);

    if (!isVariable())
        return;

    /// The sizes expand so that, after the bytes the first block skips, they follow an offset of zero, which `finish`
    /// writes over what is skipped, aligned as offsets.
    const CompressedHostColumn & host_sizes = host.sizes;
    const size_t before = sizeof(UInt64) + (sizeof(UInt64) - host_sizes.skippedBytes() % sizeof(UInt64)) % sizeof(UInt64);
    char * const sizes_base = offsets_memory.grow(before + host_sizes.expandedBytes());
    offsets = reinterpret_cast<uint64_t *>(sizes_base + before + host_sizes.skippedBytes()) - 1;
    sizes_bytes = host_sizes.keptBytes();
    decompression.add(host_sizes, sizes_base + before);
}

void GPUDeviceColumn::finish(size_t rows, cuda::stream_ref stream)
{
    if (!isVariable())
    {
        if (values_bytes != rows * sizeOf(type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU column of {} bytes of a batch of {} rows of {} bytes", values_bytes, rows, sizeOf(type));
        column_view.emplace(DeviceFixedColumn{.element_type = type, .data = values, .rows = rows});
        return;
    }

    if (sizes_bytes != rows * sizeof(UInt64))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sizes of a GPU column of {} bytes of a batch of {} rows", sizes_bytes, rows);

    /// The sizes are untrusted: bounded as on the CPU, and by the bytes the blocks have, before they become offsets.
    checkCuda(cudaMemsetAsync(offsets, 0, sizeof(UInt64), stream.get()), "Cannot start the offsets of a column of varying width");
    const UInt64 chars_bytes = endOfSizesUpTo(offsets, offsets + 1, rows, max_total_string_bytes, stream);
    if (chars_bytes > max_total_string_bytes || chars_bytes > values_bytes)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The sizes of a String column read on the GPU declare {} bytes, where its blocks have {}, most likely the data is corrupted",
            chars_bytes,
            values_bytes);
    offsetsFromSizes(offsets + 1, rows, offsets, stream);
    column_view.emplace(DeviceVariableColumn{.offsets = offsets, .chars = values, .rows = rows, .chars_bytes = chars_bytes});
}

GPUFuture<GPUDeviceBatch> copyHostToDevice(const GPUHostBatch & host, std::shared_ptr<StreamPtr> stream)
{
    if (host.rows > max_batch_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU batch of {} rows, more than a column of cuDF takes", host.rows);

    const cuda::stream_ref own{stream->get()};
    GPUPromise<GPUDeviceBatch> promise(GPUDeviceBatch(stream), own);
    GPUDeviceBatch & batch = promise.value();
    waitOnFailure(own, [&]
    {
        DecompressionBatch decompression(own);
        batch.device_columns.reserve(host.columns.size());
        for (const GPUHostColumn & column : host.columns)
            batch.device_columns.push_back(GPUDeviceColumn(column, decompression, own));

        /// The host batch is free once it returns.
        decompression.expand().waitOn(own);
        for (auto & column : batch.device_columns)
            column.finish(host.rows, own);
        batch.num_rows = host.rows;
    });
    return std::move(promise).fulfill();
}

}

#endif
