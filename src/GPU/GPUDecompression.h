#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <cstddef>
#include <span>
#include <string_view>
#include <vector>

namespace DB::GPU
{

struct CompressedBlock
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    size_t decompressed_bytes = 0;
};

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks);

struct CompressedPiece
{
    const char * device_compressed = nullptr;
    size_t compressed_bytes = 0;
    std::span<const CompressedBlock> blocks;
    cudaEvent_t uploaded = nullptr;
    /// Where the piece expands to, the bytes of its blocks one after another; without it, after the piece before, from
    /// the destination of the call.
    char * destination = nullptr;
};

/// One batched nvcomp call on a stream, the decompression stream unless the caller gives one of its own, shared by both
/// decompressors and `GPUBatchUploader`. Only the codecs of `GPUCodec` are expanded on the device; `codecOf` gives
/// nothing for the others, whose columns are read decompressed by the CPU and sent through `ColumnUploadPipe` instead.
class DecompressionBatch
{
public:
    explicit DecompressionBatch(cuda::stream_ref stream_ = GPUResources::get().decompression);

    /// The destination is written once `destination_ready_event` has happened, or, without it, once all that the compute
    /// stream has queued before has.
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
    void decompress(GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination);

private:
    DecompressionBatch batch;
    DeviceBuffer device_compressed{GPUResources::get().decompression};
};

/// Expands already uploaded pieces into the caller's device buffer without blocking: `launch` queues the work, and
/// `wait` blocks until it is done. The destructor waits as well, but the caller must still keep the buffer until then.
class AsyncDecompressor
{
public:
    AsyncDecompressor() = default;
    ~AsyncDecompressor();

    AsyncDecompressor(AsyncDecompressor && other) noexcept;

    AsyncDecompressor(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(AsyncDecompressor &&) = delete;

    void launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * device_destination);

    void wait();

private:
    DecompressionBatch batch;
    bool in_flight = false;
};

}

#endif
