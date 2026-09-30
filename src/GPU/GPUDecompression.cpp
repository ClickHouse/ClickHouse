#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>

#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

#include <algorithm>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

void checkNvcomp(nvcompStatus_t status, const char * what)
{
    if (status != nvcompSuccess)
        throw Exception(ErrorCodes::GPU_ERROR, "{}: nvcomp status {}", what, static_cast<int>(status));
}

nvcompStatus_t decompressTempBytes(GPUCodec codec, size_t num_blocks, size_t max_decompressed, size_t decompressed_total, size_t * temp_bytes)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressGetTempSizeAsync(
                num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, temp_bytes, decompressed_total);
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressGetTempSizeAsync(
                num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, temp_bytes, decompressed_total);
    }
    throw Exception(ErrorCodes::GPU_ERROR, "Unknown GPU codec {}", static_cast<int>(codec));
}

nvcompStatus_t decompressAsync(
    GPUCodec codec,
    const void * const * device_compressed_ptrs,
    const size_t * device_compressed_bytes,
    const size_t * device_decompressed_bytes,
    size_t * device_actual_bytes,
    size_t num_blocks,
    void * device_temp,
    size_t temp_bytes,
    void * const * device_value_ptrs,
    nvcompStatus_t * device_statuses,
    rmm::cuda_stream_view stream)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedLZ4DecompressDefaultOpts,
                device_statuses,
                stream);
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedZstdDecompressDefaultOpts,
                device_statuses,
                stream);
    }
    throw Exception(ErrorCodes::GPU_ERROR, "Unknown GPU codec {}", static_cast<int>(codec));
}

template <typename T>
T * arrayAt(char * base, size_t & offset, size_t count)
{
    T * const values = reinterpret_cast<T *>(base + offset);
    offset += count * sizeof(T);
    return values;
}

}

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks)
{
    size_t total = 0;
    for (const CompressedBlock & block : blocks)
        total += block.decompressed_bytes;
    return total;
}

void SyncDecompressor::decompress(
    GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination)
{
    size_t compressed_total = 0;
    for (const CompressedBlock & block : blocks)
        compressed_total = std::max(compressed_total, block.offset + block.compressed_bytes);

    if (compressed_total > host_compressed.size())
        throw Exception(
            ErrorCodes::GPU_ERROR,
            "The compressed blocks reach {} bytes into a staging buffer of {}",
            compressed_total,
            host_compressed.size());

    if (blocks.empty())
        return;

    const EventPtr destination_ready = createEvent();
    checkCuda(cudaEventRecord(destination_ready.get(), StreamRegistry::get().compute), "Cannot mark a point in the compute stream");
    checkCuda(
        cudaStreamWaitEvent(StreamRegistry::get().decompression, destination_ready.get(), 0),
        "Cannot make the decompression stream wait for the compute stream");

    device_compressed.clear();
    device_compressed.append(host_compressed.substr(0, compressed_total));

    const CompressedPiece piece{
        .device_compressed = device_compressed.data(),
        .compressed_bytes = compressed_total,
        .blocks = blocks,
    };
    batch.launch(codec, std::span<const CompressedPiece>(&piece, 1), device_destination);
    batch.waitAndCheck();
}

AsyncDecompressor::AsyncDecompressor()
{
    checkCuda(cudaEventCreateWithFlags(&values_copied_out, cudaEventDisableTiming), "Cannot create a CUDA event");
}

AsyncDecompressor::AsyncDecompressor(AsyncDecompressor && other) noexcept
    : batch(std::move(other.batch))
    , in_flight(std::exchange(other.in_flight, false))
    , values(std::move(other.values))
    , values_copied_out(std::exchange(other.values_copied_out, nullptr))
    , values_in_use(std::exchange(other.values_in_use, false))
{
}

AsyncDecompressor::~AsyncDecompressor()
{
    try
    {
        if (in_flight)
            batch.wait();
        /// `values` is freed on the decompression stream, so the compute stream must be done reading it. If the
        /// owner unwound between `wait` and `release`, the event still marks the last release and not the copies
        /// queued since, so it is recorded here to cover the current use.
        if (values_in_use)
            checkCuda(cudaEventRecord(values_copied_out, StreamRegistry::get().compute), "Cannot mark a point in the compute stream");
        if (values_copied_out != nullptr)
            checkCuda(cudaEventSynchronize(values_copied_out), "Cannot wait for the expanded values to be copied out");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (values_copied_out != nullptr)
        cudaEventDestroy(values_copied_out);
}

void AsyncDecompressor::launch(GPUCodec codec, std::span<const CompressedPiece> pieces)
{
    if (in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression was launched while the last one was not waited for");
    if (values_in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression buffer was taken again before being released");

    size_t total = 0;
    for (const CompressedPiece & piece : pieces)
        total += decompressedBytesOf(piece.blocks);

    checkCuda(
        cudaStreamWaitEvent(StreamRegistry::get().decompression, values_copied_out, 0),
        "Cannot make the decompression stream wait for the expanded values to be copied out");
    values.clear();

    batch.launch(codec, pieces, values.grow(total));
    in_flight = true;
}

const char * AsyncDecompressor::wait()
{
    if (!in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression was waited for without being launched");

    in_flight = false;
    batch.waitAndCheck();
    values_in_use = true;
    return values.data();
}

void AsyncDecompressor::release()
{
    if (!values_in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression buffer was released without being taken");

    checkCuda(cudaEventRecord(values_copied_out, StreamRegistry::get().compute), "Cannot mark a point in the compute stream");
    values_in_use = false;
}

void DecompressionBatch::launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * destination)
{
    const rmm::cuda_stream_view stream = StreamRegistry::get().decompression;

    size_t num_blocks = 0;
    size_t decompressed_total = 0;
    size_t max_decompressed = 0;

    for (const CompressedPiece & piece : pieces)
    {
        size_t piece_compressed = 0;
        for (const CompressedBlock & block : piece.blocks)
        {
            piece_compressed = std::max(piece_compressed, block.offset + block.compressed_bytes);
            decompressed_total += block.decompressed_bytes;
            max_decompressed = std::max(max_decompressed, block.decompressed_bytes);
        }

        if (piece_compressed > piece.compressed_bytes)
            throw Exception(
                ErrorCodes::GPU_ERROR,
                "The compressed blocks reach {} bytes into an upload of {}",
                piece_compressed,
                piece.compressed_bytes);

        num_blocks += piece.blocks.size();

        if (piece.uploaded)
            checkCuda(cudaStreamWaitEvent(stream, piece.uploaded, 0), "Cannot make the decompression stream wait for an upload");
    }

    expected_bytes.clear();
    expected_bytes.reserve(num_blocks);

    if (num_blocks == 0)
    {
        checkCuda(cudaEventRecord(expanded.get(), stream), "Cannot mark a point in the decompression stream");
        return;
    }

    const size_t arguments_bytes = num_blocks * (2 * sizeof(void *) + 2 * sizeof(size_t));
    host_arguments.clear();
    char * const arguments = host_arguments.grow(arguments_bytes);

    size_t offset = 0;
    const void ** compressed_ptrs = arrayAt<const void *>(arguments, offset, num_blocks);
    void ** value_ptrs = arrayAt<void *>(arguments, offset, num_blocks);
    size_t * compressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);
    size_t * decompressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);

    size_t block_index = 0;
    size_t at = 0;
    for (const CompressedPiece & piece : pieces)
    {
        for (const CompressedBlock & block : piece.blocks)
        {
            compressed_ptrs[block_index] = piece.device_compressed + block.offset;
            value_ptrs[block_index] = destination + at;
            compressed_bytes[block_index] = block.compressed_bytes;
            decompressed_bytes[block_index] = block.decompressed_bytes;
            expected_bytes.push_back(block.decompressed_bytes);
            at += block.decompressed_bytes;
            ++block_index;
        }
    }

    device_arguments.clear();
    device_arguments.append(host_arguments.bytes());

    offset = 0;
    const void * const * d_compressed_ptrs = arrayAt<const void *>(device_arguments.data(), offset, num_blocks);
    void * const * d_value_ptrs = arrayAt<void *>(device_arguments.data(), offset, num_blocks);
    const size_t * d_compressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_blocks);
    const size_t * d_decompressed_bytes = arrayAt<size_t>(device_arguments.data(), offset, num_blocks);

    const size_t results_bytes = num_blocks * (sizeof(size_t) + sizeof(nvcompStatus_t));
    device_results.clear();
    device_results.grow(results_bytes);

    offset = 0;
    size_t * d_actual_bytes = arrayAt<size_t>(device_results.data(), offset, num_blocks);
    nvcompStatus_t * d_statuses = arrayAt<nvcompStatus_t>(device_results.data(), offset, num_blocks);

    size_t temp_bytes = 0;
    checkNvcomp(
        decompressTempBytes(codec, num_blocks, max_decompressed, decompressed_total, &temp_bytes),
        "Cannot size nvcomp's scratch space");

    device_temp.clear();
    device_temp.grow(temp_bytes);

    checkNvcomp(
        decompressAsync(
            codec,
            d_compressed_ptrs,
            d_compressed_bytes,
            d_decompressed_bytes,
            d_actual_bytes,
            num_blocks,
            device_temp.data(),
            temp_bytes,
            d_value_ptrs,
            d_statuses,
            stream),
        "Cannot decompress on the device");

    host_results.clear();
    char * const results = host_results.grow(results_bytes);

    checkCuda(
        cudaMemcpyAsync(results, device_results.data(), results_bytes, cudaMemcpyDeviceToHost, stream),
        "Cannot copy the decompression statuses back");

    checkCuda(cudaEventRecord(expanded.get(), stream), "Cannot mark a point in the decompression stream");
}

void DecompressionBatch::wait()
{
    checkCuda(cudaEventSynchronize(expanded.get()), "Cannot wait for a decompression");
}

void DecompressionBatch::waitAndCheck()
{
    wait();

    const size_t num_blocks = expected_bytes.size();
    if (num_blocks == 0)
        return;

    size_t offset = 0;
    const size_t * actual_bytes = arrayAt<size_t>(host_results.data(), offset, num_blocks);
    const nvcompStatus_t * statuses = arrayAt<nvcompStatus_t>(host_results.data(), offset, num_blocks);

    for (size_t block_index = 0; block_index < num_blocks; ++block_index)
    {
        if (statuses[block_index] != nvcompSuccess)
            throw Exception(
                ErrorCodes::GPU_ERROR, "Block {} did not decompress: nvcomp status {}", block_index, static_cast<int>(statuses[block_index]));
        if (actual_bytes[block_index] != expected_bytes[block_index])
            throw Exception(
                ErrorCodes::GPU_ERROR,
                "Block {} expanded to {} bytes, expected {}",
                block_index,
                actual_bytes[block_index],
                expected_bytes[block_index]);
    }
}

}

#endif
