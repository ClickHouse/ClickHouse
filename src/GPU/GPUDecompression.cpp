#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/CompressedHostColumn.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <Compression/CompressionCodecGDeflate.h>
#include <base/unaligned.h>

#include <nvcomp/gdeflate.h>
#include <nvcomp/lz4.h>
#include <nvcomp/zstd.h>

#include <algorithm>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

/// The alignment of the addresses that nvcomp expands blocks to: 8 bytes for ZSTD, 4 for LZ4.
constexpr size_t expand_alignment = 8;

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

void addCompressedBlocksOf(
    GPUCodec codec, std::string_view payload, size_t offset, size_t decompressed_bytes, std::vector<CompressedBlock> & blocks)
{
    if (codec != GPUCodec::GDEFLATE)
    {
        blocks.push_back({.offset = offset, .compressed_bytes = payload.size(), .decompressed_bytes = decompressed_bytes});
        return;
    }

    /// The number of chunks, the compressed size of each, then the chunks, each from a multiple of 4 bytes.
    constexpr size_t chunk_bytes = CompressionCodecGDeflate::chunk_bytes;
    constexpr size_t chunk_alignment = 4;
    if (payload.size() < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", payload.size());
    const size_t num_chunks = unalignedLoadLittleEndian<UInt32>(payload.data());
    const size_t header_bytes = sizeof(UInt32) * (1 + num_chunks);
    if (num_chunks != (decompressed_bytes + chunk_bytes - 1) / chunk_bytes || header_bytes > payload.size())
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            payload.size(),
            num_chunks,
            decompressed_bytes,
            (decompressed_bytes + chunk_bytes - 1) / chunk_bytes);

    size_t at = header_bytes;
    for (size_t i = 0; i < num_chunks; ++i)
    {
        const size_t compressed_bytes = unalignedLoadLittleEndian<UInt32>(payload.data() + sizeof(UInt32) * (1 + i));
        if (at + compressed_bytes > payload.size())
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "Chunk {} of a GDeflate block of {} bytes ends at {}", i, payload.size(), at + compressed_bytes);
        blocks.push_back({
            .offset = offset + at,
            .compressed_bytes = compressed_bytes,
            .decompressed_bytes = std::min(chunk_bytes, decompressed_bytes - i * chunk_bytes),
        });
        at = (at + compressed_bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
    }
}

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks)
{
    size_t total = 0;
    for (const CompressedBlock & block : blocks)
        total += block.decompressed_bytes;
    return total;
}

DecompressionBatch::DecompressionBatch(cuda::stream_ref stream_)
    : stream(stream_)
{
}

std::vector<DecompressionBatch::Piece> & DecompressionBatch::piecesOf(GPUCodec codec)
{
    for (auto & [of, list] : pieces_by_codec)
    {
        if (of == codec)
            return list;
    }
    return pieces_by_codec.emplace_back(codec, std::vector<Piece>{}).second;
}

void DecompressionBatch::add(const CompressedHostColumn & column, char * destination)
{
    const size_t expanded_bytes = column.expandedBytes();
    if (expanded_bytes == 0)
        return;

    char * const at = uploaded.emplace_back(stream).grow(column.bytes.size());
    checkCuda(
        cudaMemcpyAsync(at, column.bytes.data(), column.bytes.size(), cudaMemcpyHostToDevice, stream.get()),
        "Cannot send compressed blocks to the device");

    char * expand_to = destination;
    if (reinterpret_cast<uintptr_t>(destination) % expand_alignment != 0)
    {
        expand_to = expanded_elsewhere.emplace_back(stream).grow(expanded_bytes);
        moves.push_back({expand_to, destination, expanded_bytes});
    }
    piecesOf(*column.codec).push_back({.device_compressed = at, .compressed_bytes = column.bytes.size(), .blocks = column.blocks, .destination = expand_to});
}

GPUFuture<void> DecompressionBatch::expand()
{
    /// The buffers are freed in the order of the stream, after what is queued on it.
    SCOPE_EXIT({
        pieces_by_codec.clear();
        uploaded.clear();
        expanded_elsewhere.clear();
        moves.clear();
    });

    for (auto & [codec, pieces] : pieces_by_codec)
        check(launch(codec, pieces));
    for (const Move & move : moves)
        checkCuda(cudaMemcpyAsync(move.to, move.from, move.bytes, cudaMemcpyDeviceToDevice, stream.get()), "Cannot move expanded bytes on the device");
    return GPUPromise<void>(stream).fulfill();
}

GPUFuture<void> DecompressionBatch::launch(GPUCodec codec, std::span<const Piece> pieces)
{
    size_t num_blocks = 0;
    size_t decompressed_total = 0;
    size_t max_decompressed = 0;

    for (const Piece & piece : pieces)
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

    }

    expected_bytes.clear();
    expected_bytes.reserve(num_blocks);

    if (num_blocks == 0)
        return GPUPromise<void>(stream).fulfill();

    const size_t arguments_bytes = num_blocks * (2 * sizeof(void *) + 2 * sizeof(size_t));
    host_arguments.clear();
    char * const arguments = growBy(host_arguments, arguments_bytes);

    size_t offset = 0;
    const void ** compressed_ptrs = arrayAt<const void *>(arguments, offset, num_blocks);
    void ** value_ptrs = arrayAt<void *>(arguments, offset, num_blocks);
    size_t * compressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);
    size_t * decompressed_bytes = arrayAt<size_t>(arguments, offset, num_blocks);

    size_t block_index = 0;
    size_t at = 0;
    for (const Piece & piece : pieces)
    {
        char * const destination = piece.destination;
        at = 0;
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

        checkCuda(
            cudaMemcpyAsync(results, device_results.data(), results_bytes, cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy the decompression statuses back");

    });
    return GPUPromise<void>(stream).fulfill();
}

void DecompressionBatch::check(GPUFuture<void> expanded)
{
    std::move(expanded).get();

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
