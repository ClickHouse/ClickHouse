#include <GPU/GPUDecompression.h>

#if USE_GPU

#include <GPU/GPUDevice.h>
#include <GPU/GPUMemory.h>

#include <GPU/GPUTypeMapping.h>

#include <Compression/BitPackingFormat.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

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
    extern const int CANNOT_DECOMPRESS;
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

/// Adds what nvcomp expands of a block of `codec` to `blocks`: the block, or each chunk of a `GDeflate` one.
void addBlocksOf(std::vector<CompressedBlock> & blocks, GPUCodec codec, size_t start, std::string_view payload, size_t decompressed_bytes)
{
    if (codec == GPUCodec::GDEFLATE)
    {
        forEachGDeflateChunk(payload, decompressed_bytes, [&](const GDeflateChunk & chunk)
        {
            blocks.push_back({
                .offset = start + chunk.offset,
                .compressed_bytes = chunk.compressed_bytes,
                .decompressed_bytes = chunk.decompressed_bytes,
            });
        });
    }
    else
    {
        blocks.push_back({.offset = start, .compressed_bytes = payload.size(), .decompressed_bytes = decompressed_bytes});
    }
}

/// Whether `method` with the list of methods `payload` starts with is a chain of `BitPacking` and one other codec,
/// see `CompressionCodecMultiple`.
bool isBitPackingChain(UInt8 method, std::string_view payload)
{
    return method == static_cast<UInt8>(CompressionMethodByte::Multiple) && payload.size() >= 3 && payload[0] == 2
        && static_cast<UInt8>(payload[1]) == static_cast<UInt8>(CompressionMethodByte::BitPacking);
}

/// The 8-byte reads of `unpackStreams` stay in a stream, but each stream starts at a multiple of 8 bytes anyway.
constexpr size_t scratch_alignment = 8;

}

size_t expandedBytesOf(std::span<const CompressedBlock> blocks)
{
    size_t total = 0;
    for (const CompressedBlock & block : blocks)
        total += block.decompressed_bytes;
    return total;
}

size_t valueBytesOf(std::span<const CompressedBlock> blocks, std::span<const BitPackingBlock> packed)
{
    size_t total = expandedBytesOf(blocks);
    for (const BitPackingBlock & pack : packed)
    {
        if (pack.first_block + pack.num_blocks > blocks.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "A BitPacking block of blocks {}..{} among {}", pack.first_block, pack.first_block + pack.num_blocks, blocks.size());
        total -= expandedBytesOf(blocks.subspan(pack.first_block, pack.num_blocks));
        total += pack.unpacked_bytes;
    }
    return total;
}

std::optional<GPUCodec> deviceCodecOf(UInt8 method, std::string_view payload)
{
    if (const auto codec = codecOf(method))
        return codec;
    if (isBitPackingChain(method, payload))
        return codecOf(static_cast<UInt8>(payload[2]));
    return {};
}

GPUCodec stageCompressedBlock(
    PinnedBuffer & staged,
    std::vector<CompressedBlock> & blocks,
    std::vector<BitPackingBlock> & packed,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes)
{
    const auto codec = deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of compression method {:#x} staged for the device", static_cast<UInt16>(method));

    /// The device buffer the staged bytes go to starts at a multiple of 256 bytes.
    constexpr size_t alignment = 4;
    if (const size_t misaligned = staged.size() % alignment)
        staged.resize_fill(staged.size() + alignment - misaligned, 0);

    const size_t start = staged.size();
    if (!isBitPackingChain(method, payload))
    {
        addBlocksOf(blocks, *codec, start, payload, decompressed_bytes);
        appendHostBytes(staged, payload);
        return *codec;
    }

    /// After the list of methods, the block of the second codec with its header, which holds the block of `BitPacking`.
    constexpr size_t header_bytes = ICompressionCodec::getHeaderSize();
    const std::string_view inner = payload.substr(3);
    if (inner.size() < header_bytes)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A block of a chain of codecs of {} bytes", payload.size());

    const size_t inner_compressed = unalignedLoadLittleEndian<UInt32>(inner.data() + 1);
    const size_t inner_decompressed = unalignedLoadLittleEndian<UInt32>(inner.data() + 5);
    if (inner[0] != payload[2] || inner_compressed != inner.size() || inner_decompressed <= header_bytes)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A block of a chain of codecs holds a block of method {:#x} of {} bytes to {}, where the chain says {:#x} and the block has {} bytes",
            static_cast<UInt16>(static_cast<UInt8>(inner[0])),
            inner_compressed,
            inner_decompressed,
            static_cast<UInt16>(static_cast<UInt8>(payload[2])),
            inner.size());

    const size_t first_block = blocks.size();
    addBlocksOf(blocks, *codec, start, inner.substr(header_bytes), inner_decompressed);
    packed.push_back({.first_block = first_block, .num_blocks = blocks.size() - first_block, .unpacked_bytes = decompressed_bytes});
    appendHostBytes(staged, inner.substr(header_bytes));
    return *codec;
}

SyncDecompressor::SyncDecompressor(cuda::stream_ref stream_)
    : stream(stream_)
    , batch(stream_)
    , device_compressed(stream_)
    , device_expanded(stream_)
{
}

void SyncDecompressor::decompressToHost(
    GPUCodec codec,
    std::string_view host_compressed,
    std::span<const CompressedBlock> blocks,
    std::span<const BitPackingBlock> packed,
    char * host_destination)
{
    const size_t bytes = valueBytesOf(blocks, packed);
    if (blocks.empty() || bytes == 0)
        return;

    /// The expanded bytes are allocated in the order of this stream, and the expansion waits for that alone, not for
    /// the compute stream as it would without an event.
    device_expanded.clear();
    char * const destination = device_expanded.grow(bytes);
    checkCuda(cudaEventRecord(expanded_ready.get(), stream.get()), "Cannot mark a point in a decompression stream");

    size_t compressed_total = 0;
    for (const CompressedBlock & block : blocks)
        compressed_total = std::max(compressed_total, block.offset + block.compressed_bytes);
    if (compressed_total > host_compressed.size())
        throw Exception(
            ErrorCodes::GPU_ERROR, "The compressed blocks reach {} bytes into a staging buffer of {}", compressed_total, host_compressed.size());

    waitOnFailure(stream, [&]
    {
        device_compressed.clear();
        device_compressed.append(host_compressed.substr(0, compressed_total));

        const CompressedPiece piece{
            .device_compressed = device_compressed.data(),
            .compressed_bytes = compressed_total,
            .blocks = blocks,
            .packed = packed,
        };
        batch.launch(codec, std::span<const CompressedPiece>(&piece, 1), destination, expanded_ready.get());
        checkCuda(
            cudaMemcpyAsync(host_destination, destination, bytes, cudaMemcpyDeviceToHost, stream.get()),
            "Cannot copy expanded bytes back from the device");
    });
    batch.waitAndCheck();
    checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for expanded bytes to come back from the device");
}

void SyncDecompressor::decompress(
    GPUCodec codec,
    std::string_view host_compressed,
    std::span<const CompressedBlock> blocks,
    std::span<const BitPackingBlock> packed,
    char * device_destination)
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

    waitOnFailure(stream, [&]
    {
        device_compressed.clear();
        device_compressed.append(host_compressed.substr(0, compressed_total));

        const CompressedPiece piece{
            .device_compressed = device_compressed.data(),
            .compressed_bytes = compressed_total,
            .blocks = blocks,
            .packed = packed,
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
    num_streams = 0;
    size_t scratch_total = 0;
    size_t max_chunks = 0;

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

        for (const BitPackingBlock & pack : piece.packed)
        {
            if (pack.first_block + pack.num_blocks > piece.blocks.size() || pack.num_blocks == 0)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "A BitPacking block of blocks {}..{} of a piece of {}",
                    pack.first_block,
                    pack.first_block + pack.num_blocks,
                    piece.blocks.size());
            scratch_total += (expandedBytesOf(piece.blocks.subspan(pack.first_block, pack.num_blocks)) + scratch_alignment - 1) / scratch_alignment * scratch_alignment;
            max_chunks = std::max(max_chunks, BitPackingFormat::numChunks(pack.unpacked_bytes));
        }
        num_streams += piece.packed.size();

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

    /// The blocks of `BitPacking` expand into `device_scratch`, each block of it after the header of the compressed block
    /// it was in, and `unpackStreams` puts their integers where the destination has them.
    device_scratch.clear();
    char * const scratch_base = scratch_total ? device_scratch.grow(scratch_total) : nullptr;
    host_streams.clear();
    BitPackingStream * const streams
        = num_streams ? reinterpret_cast<BitPackingStream *>(growBy(host_streams, num_streams * sizeof(BitPackingStream))) : nullptr;
    constexpr size_t header_bytes = ICompressionCodec::getHeaderSize();

    size_t block_index = 0;
    size_t stream_index = 0;
    size_t scratch_at = 0;
    size_t at = 0;
    for (const CompressedPiece & piece : pieces)
    {
        size_t next_pack = 0;
        for (size_t i = 0; i < piece.blocks.size(); ++i)
        {
            const CompressedBlock & block = piece.blocks[i];
            compressed_ptrs[block_index] = piece.device_compressed + block.offset;
            compressed_bytes[block_index] = block.compressed_bytes;
            decompressed_bytes[block_index] = block.decompressed_bytes;
            expected_bytes.push_back(block.decompressed_bytes);

            if (next_pack < piece.packed.size() && i == piece.packed[next_pack].first_block)
            {
                const BitPackingBlock & pack = piece.packed[next_pack++];
                const size_t pack_bytes = expandedBytesOf(piece.blocks.subspan(pack.first_block, pack.num_blocks));
                streams[stream_index++] = {
                    .stream = reinterpret_cast<const unsigned char *>(scratch_base + scratch_at + header_bytes),
                    .stream_bytes = pack_bytes - header_bytes,
                    .destination = reinterpret_cast<unsigned char *>(destination + at),
                    .unpacked_bytes = pack.unpacked_bytes,
                };
                size_t within = 0;
                for (size_t j = 0; j < pack.num_blocks; ++j)
                {
                    value_ptrs[block_index + j] = scratch_base + scratch_at + within;
                    within += piece.blocks[i + j].decompressed_bytes;
                }
                scratch_at += (pack_bytes + scratch_alignment - 1) / scratch_alignment * scratch_alignment;
                at += pack.unpacked_bytes;
            }
            else if (next_pack > 0 && i < piece.packed[next_pack - 1].first_block + piece.packed[next_pack - 1].num_blocks)
            {
                /// Another block of the pack before, placed with its first.
            }
            else
            {
                value_ptrs[block_index] = destination + at;
                at += block.decompressed_bytes;
            }
            ++block_index;
        }
        if (next_pack != piece.packed.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The BitPacking blocks of a piece are not in the order of its blocks");
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

        const size_t results_bytes = num_blocks * (sizeof(size_t) + sizeof(nvcompStatus_t)) + num_streams * sizeof(uint32_t);
        device_results.clear();
        device_results.grow(results_bytes);

        offset = 0;
        size_t * d_actual_bytes = arrayAt<size_t>(device_results.data(), offset, num_blocks);
        nvcompStatus_t * d_statuses = arrayAt<nvcompStatus_t>(device_results.data(), offset, num_blocks);
        uint32_t * d_stream_statuses = arrayAt<uint32_t>(device_results.data(), offset, num_streams);

        size_t temp_bytes = 0;
        checkNvcomp(
            decompressTempBytes(codec, num_blocks, max_decompressed, decompressed_total, &temp_bytes, stream),
            "Cannot size nvcomp's scratch space");

        device_temp.clear();
        device_temp.grow(temp_bytes);

        host_results.clear();
        char * const results = growBy(host_results, results_bytes);

        if (num_streams)
        {
            device_streams.clear();
            device_streams.append(bytesOf(host_streams));
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

        if (num_streams)
            unpackStreams(
                reinterpret_cast<const BitPackingStream *>(device_streams.data()), num_streams, max_chunks, d_stream_statuses, stream);

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
    const uint32_t * stream_statuses = arrayAt<uint32_t>(host_results.data(), offset, num_streams);

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

    for (size_t stream_index = 0; stream_index < num_streams; ++stream_index)
        if (stream_statuses[stream_index] != 0)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "BitPacking block {} did not unpack on the device: its header does not match what was read of it, status {}",
                stream_index,
                stream_statuses[stream_index]);
}

}

#endif
