#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <base/types.h>

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB::GPU
{

/// What one call of nvcomp expands: a block of `LZ4` or `ZSTD`, or a chunk of a `GDeflate` block.
struct CompressedBlock
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    size_t decompressed_bytes = 0;
};

/// A block of the `BitPacking` codec that the `num_blocks` blocks from `first_block` of a piece expand to, after the
/// header of a compressed block, see `BitPackingFormat`: the device unpacks it into `unpacked_bytes` of integers, which
/// are what the column takes of it, where the blocks' own expanded bytes are only read.
struct BitPackingBlock
{
    size_t first_block = 0;
    size_t num_blocks = 0;
    size_t unpacked_bytes = 0;
};

/// What nvcomp expands the blocks to.
size_t expandedBytesOf(std::span<const CompressedBlock> blocks);

/// What the column takes of the blocks: the integers of the packed ones, and the expanded bytes of the rest.
size_t valueBytesOf(std::span<const CompressedBlock> blocks, std::span<const BitPackingBlock> packed);

/// The codec the device expands a compressed block of `method` with: of the codecs of `GPUCodec`, the block's own, or
/// the second of a chain of `BitPacking` and one of them, whose list `payload` starts with. Nothing for any other.
std::optional<GPUCodec> deviceCodecOf(UInt8 method, std::string_view payload);

/// Stages the payload of a compressed block of `method`, see `deviceCodecOf`, at the end of `staged`, from a multiple
/// of 4 bytes, as nvcomp reads some codecs from, and adds what nvcomp expands of it to `blocks`: the block, or each
/// chunk of a `GDeflate` one, and, of a chain of `BitPacking`, what they expand to for `packed`. Returns the codec.
GPUCodec stageCompressedBlock(
    PinnedBuffer & staged,
    std::vector<CompressedBlock> & blocks,
    std::vector<BitPackingBlock> & packed,
    UInt8 method,
    std::string_view payload,
    size_t decompressed_bytes);

struct CompressedPiece
{
    const char * device_compressed = nullptr;
    size_t compressed_bytes = 0;
    std::span<const CompressedBlock> blocks;
    std::span<const BitPackingBlock> packed;
    cudaEvent_t uploaded = nullptr;
};

/// One batched nvcomp call on the decompression stream, shared by both decompressors. Only the codecs of `GPUCodec`
/// are expanded on the device; `codecOf` gives nothing for the others, whose columns are read decompressed by the CPU
/// and sent through `ColumnUploadPipe` instead.
class DecompressionBatch
{
public:
    explicit DecompressionBatch(cuda::stream_ref stream_ = GPUResources::get().decompression);

    /// The destination is written once `destination_ready` has happened, or, without it, once all that the compute
    /// stream has before has.
    void launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * destination, cudaEvent_t destination_ready_event = nullptr);

    void wait();

    void waitAndCheck();

private:
    cuda::stream_ref stream;

    PinnedBuffer host_arguments;
    PinnedBuffer host_results;

    DeviceBuffer device_arguments{stream};
    DeviceBuffer device_results{stream};
    DeviceBuffer device_temp{stream};
    /// What nvcomp expands the blocks of `BitPacking` to, which `unpackStreams` reads, and the streams it is told of.
    DeviceBuffer device_scratch{stream};
    PinnedBuffer host_streams;
    DeviceBuffer device_streams{stream};
    /// Of nvcomp's statuses and sizes, then a status per stream unpacked.
    size_t num_streams = 0;

    EventPtr destination_ready = createEvent();
    EventPtr expanded = createEvent();
    std::vector<size_t> expected_bytes;
};

/// Both decompressors expand into a device buffer of the caller, which keeps it, and clears or frees it, as any other of
/// its own. They differ only in when the bytes are there: `SyncDecompressor` blocks until they are, and
/// `AsyncDecompressor` returns at once and they are there when its `wait` returns. Until then, the buffer must live
/// and must not be read, written or grown.

/// Uploads compressed bytes from the host and expands them into the caller's device buffer, blocking until done.
class SyncDecompressor
{
public:
    /// On `stream_`, of the device's decompression stream by default, which every other user of it shares.
    explicit SyncDecompressor(cuda::stream_ref stream_ = GPUResources::get().decompression);

    void decompress(
        GPUCodec codec,
        std::string_view host_compressed,
        std::span<const CompressedBlock> blocks,
        std::span<const BitPackingBlock> packed,
        char * device_destination);

    /// Expands into a device buffer of its own and copies what the blocks expand to, `valueBytesOf` them, to
    /// `host_destination`, which should be pinned for the copy to be fast, blocking until both are done.
    void decompressToHost(
        GPUCodec codec,
        std::string_view host_compressed,
        std::span<const CompressedBlock> blocks,
        std::span<const BitPackingBlock> packed,
        char * host_destination);

private:
    cuda::stream_ref stream;
    DecompressionBatch batch;
    DeviceBuffer device_compressed;
    DeviceBuffer device_expanded;
    EventPtr expanded_ready = createEvent();
};

/// Expands already uploaded pieces into the caller's device buffer without blocking: `launch` queues the work, and
/// `wait` blocks until it is done. The destructor waits as well, but the caller must still keep the buffer until then.
class AsyncDecompressor
{
public:
    explicit AsyncDecompressor(cuda::stream_ref stream = GPUResources::get().decompression) : batch(stream) { }
    ~AsyncDecompressor();

    AsyncDecompressor(AsyncDecompressor && other) noexcept;

    AsyncDecompressor(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(AsyncDecompressor &&) = delete;

    void launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * device_destination, cudaEvent_t destination_ready = nullptr);

    void wait();

private:
    DecompressionBatch batch;
    bool in_flight = false;
};

}

#endif
