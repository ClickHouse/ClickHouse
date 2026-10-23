#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>

#include <GPU/BitUnpack.cuh>
#include <GPU/GPUGDeflate.h>

#include <nvcomp/gdeflate.h>
#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

#include <algorithm>
#include <utility>
#include <vector>

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

nvcompStatus_t decompressTempBytes(
    GPUCodec codec, size_t num_blocks, size_t max_decompressed, size_t decompressed_total, size_t * temp_bytes, cuda::stream_ref stream)
{
    switch (codec)
    {
        case GPUCodec::LZ4:
            return nvcompBatchedLZ4DecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedLZ4DecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
        case GPUCodec::ZSTD:
            return nvcompBatchedZstdDecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedZstdDecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
        case GPUCodec::GDEFLATE:
            return nvcompBatchedGdeflateDecompressGetTempSize(
                num_blocks, max_decompressed, nvcompBatchedGdeflateDecompressDefaultOpts, temp_bytes, decompressed_total, stream.get());
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
    cuda::stream_ref stream)
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
                stream.get());
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
                stream.get());
        case GPUCodec::GDEFLATE:
            return nvcompBatchedGdeflateDecompressAsync(
                device_compressed_ptrs,
                device_compressed_bytes,
                device_decompressed_bytes,
                device_actual_bytes,
                num_blocks,
                device_temp,
                temp_bytes,
                device_value_ptrs,
                nvcompBatchedGdeflateDecompressDefaultOpts,
                device_statuses,
                stream.get());
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

void stageCompressedBlock(
    PinnedBuffer & staged, std::vector<CompressedBlock> & blocks, GPUCodec codec, std::string_view payload, size_t decompressed_bytes)
{
    /// The device buffer the staged bytes go to starts at a multiple of 256 bytes.
    constexpr size_t alignment = 4;
    if (const size_t misaligned = staged.size() % alignment)
        staged.resize_fill(staged.size() + alignment - misaligned, 0);

    const size_t start = staged.size();
    if (codec == GPUCodec::GDEFLATE)
    {
        forEachGDeflateChunk(payload, decompressed_bytes, [&](const GDeflateChunk & chunk)
        {
            blocks.push_back({
                .offset = start + chunk.offset,
                .compressed_bytes = chunk.compressed_bytes,
                .decompressed_bytes = chunk.decompressed_bytes,
                .packed_bytes = chunk.expanded_bytes,
                .packed_bits = chunk.packed_bits,
                .element_size = chunk.element_size,
                .reference = chunk.reference,
            });
        });
    }
    else
    {
        blocks.push_back({.offset = start, .compressed_bytes = payload.size(), .decompressed_bytes = decompressed_bytes});
    }
    appendHostBytes(staged, payload);
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

    waitOnFailure(GPUResources::get().decompression, [&]
    {
        device_compressed.clear();
        device_compressed.append(host_compressed.substr(0, compressed_total));

        const CompressedPiece piece{
            .device_compressed = device_compressed.data(),
            .compressed_bytes = compressed_total,
            .blocks = blocks,
        };
        batch.launch(codec, std::span<const CompressedPiece>(&piece, 1), device_destination);
    });
    batch.waitAndCheck();
}

AsyncDecompressor::AsyncDecompressor(AsyncDecompressor && other) noexcept
    : batch(std::move(other.batch))
    , in_flight(std::exchange(other.in_flight, false))
{
}

AsyncDecompressor::~AsyncDecompressor()
{
    if (!in_flight)
        return;

    try
    {
        batch.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void AsyncDecompressor::launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * device_destination, cudaEvent_t destination_ready)
{
    if (in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression was launched while the last one was not waited for");

    batch.launch(codec, pieces, device_destination, destination_ready);
    in_flight = true;
}

void AsyncDecompressor::wait()
{
    if (!in_flight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A decompression was waited for without being launched");

    in_flight = false;
    batch.waitAndCheck();
}

DecompressionBatch::DecompressionBatch(cuda::stream_ref stream_)
    : stream(stream_)
{
}

void DecompressionBatch::launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * destination, cudaEvent_t destination_ready_event)
{
    /// The destination is the caller's, which may have just allocated or written it on the compute stream.
    if (!destination_ready_event)
    {
        checkCuda(cudaEventRecord(destination_ready.get(), GPUResources::get().compute.get()), "Cannot mark a point in the compute stream");
        destination_ready_event = destination_ready.get();
    }
    checkCuda(cudaStreamWaitEvent(stream.get(), destination_ready_event, 0), "Cannot make the decompression stream wait for its destination");

    size_t num_blocks = 0;
    size_t decompressed_total = 0;
    size_t max_decompressed = 0;

    for (const CompressedPiece & piece : pieces)
    {
        size_t piece_compressed = 0;
        for (const CompressedBlock & block : piece.blocks)
        {
            piece_compressed = std::max(piece_compressed, block.offset + block.compressed_bytes);
            decompressed_total += block.expandedBytes();
            max_decompressed = std::max(max_decompressed, block.expandedBytes());
        }

        if (piece_compressed > piece.compressed_bytes)
            throw Exception(
                ErrorCodes::GPU_ERROR,
                "The compressed blocks reach {} bytes into an upload of {}",
                piece_compressed,
                piece.compressed_bytes);

        num_blocks += piece.blocks.size();

        if (piece.uploaded)
            checkCuda(cudaStreamWaitEvent(stream.get(), piece.uploaded, 0), "Cannot make the decompression stream wait for an upload");
    }

    expected_bytes.clear();
    expected_bytes.reserve(num_blocks);

    if (num_blocks == 0)
    {
        checkCuda(cudaEventRecord(expanded.get(), stream.get()), "Cannot mark a point in the decompression stream");
        return;
    }

    const size_t arguments_bytes = num_blocks * (2 * sizeof(void *) + 2 * sizeof(size_t));
    host_arguments.clear();
    char * const arguments = growBy(host_arguments, arguments_bytes);

    size_t offset = 0;
    const void ** compressed_ptrs = arrayAt<const void *>(arguments, offset, num_blocks);
    void ** value_ptrs = arrayAt<void *>(arguments, offset, num_blocks);
    size_t * compressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);
    size_t * decompressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);

    /// Packed chunks expand into `device_packed`, each with 8 bytes to spare that `unpackChunks` reads past them, and their
    /// integers are unpacked into the destination after.
    size_t packed_total = 0;
    for (const CompressedPiece & piece : pieces)
        for (const CompressedBlock & block : piece.blocks)
            if (block.packed_bits)
                packed_total += (block.packed_bytes + 15) / 8 * 8;
    device_packed.clear();
    char * const packed_base = packed_total ? device_packed.grow(packed_total) : nullptr;

    std::vector<PackedChunk> packed_chunks;
    size_t max_packed_count = 0;
    size_t packed_at = 0;

    size_t block_index = 0;
    size_t at = 0;
    for (const CompressedPiece & piece : pieces)
    {
        for (const CompressedBlock & block : piece.blocks)
        {
            compressed_ptrs[block_index] = piece.device_compressed + block.offset;
            compressed_bytes[block_index] = block.compressed_bytes;
            decompressed_bytes[block_index] = block.expandedBytes();
            expected_bytes.push_back(block.expandedBytes());
            if (block.packed_bits)
            {
                value_ptrs[block_index] = packed_base + packed_at;
                const auto count = static_cast<uint32_t>(block.decompressed_bytes / block.element_size);
                packed_chunks.push_back({
                    .source = reinterpret_cast<const unsigned char *>(packed_base + packed_at),
                    .destination = reinterpret_cast<unsigned char *>(destination + at),
                    .count = count,
                    .bits = block.packed_bits,
                    .element_size = block.element_size,
                    .reference = block.reference,
                });
                max_packed_count = std::max<size_t>(max_packed_count, count);
                packed_at += (block.packed_bytes + 15) / 8 * 8;
            }
            else
            {
                value_ptrs[block_index] = destination + at;
            }
            at += block.decompressed_bytes;
            ++block_index;
        }
    }

    waitOnFailure(stream, [&]
    {
        device_arguments.clear();
        device_arguments.append(bytesOf(host_arguments));

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
            decompressTempBytes(codec, num_blocks, max_decompressed, decompressed_total, &temp_bytes, stream),
            "Cannot size nvcomp's scratch space");

        device_temp.clear();
        device_temp.grow(temp_bytes);

        host_results.clear();
        char * const results = growBy(host_results, results_bytes);

        if (!packed_chunks.empty())
        {
            /// Zeroes in the 8 spare bytes of every chunk past what nvcomp writes, so that what `unpackChunks` reads
            /// there is defined, then the integers.
            checkCuda(cudaMemsetAsync(packed_base, 0, packed_total, stream.get()), "Cannot clear the packed chunks");
            host_packed_chunks.clear();
            appendHostBytes(
                host_packed_chunks,
                std::string_view(reinterpret_cast<const char *>(packed_chunks.data()), packed_chunks.size() * sizeof(PackedChunk)));
            device_packed_chunks.clear();
            device_packed_chunks.append(bytesOf(host_packed_chunks));
        }

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

        if (!packed_chunks.empty())
            unpackChunks(
                reinterpret_cast<const PackedChunk *>(device_packed_chunks.data()), packed_chunks.size(), max_packed_count, stream);

        checkCuda(
            cudaMemcpyAsync(results, device_results.data(), results_bytes, cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the decompression statuses back");

        checkCuda(cudaEventRecord(expanded.get(), stream.get()), "Cannot mark a point in the decompression stream");
    });
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
