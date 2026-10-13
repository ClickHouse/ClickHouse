#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUStreams.cuh>
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
};

/// One batched nvcomp call on the decompression stream, shared by both decompressors. Only the codecs of `GPUCodec`
/// are expanded on the device; `codecOf` gives nothing for the others, whose columns are read decompressed by the CPU
/// and sent through `ColumnUploadPipe` instead.
class DecompressionBatch
{
public:
    void launch(GPUCodec codec, std::span<const CompressedPiece> pieces, char * destination);

    void wait();

    void waitAndCheck();

private:
    PinnedBuffer host_arguments;
    PinnedBuffer host_results;

    DeviceBuffer device_arguments{StreamRegistry::get().decompression};
    DeviceBuffer device_results{StreamRegistry::get().decompression};
    DeviceBuffer device_temp{StreamRegistry::get().decompression};

    EventPtr expanded = createEvent();
    std::vector<size_t> expected_bytes;
};

/// The two decompressors differ in who owns the expanded bytes, as they differ in when the bytes are there:
///  - `SyncDecompressor` expands into a device buffer of the caller, and the bytes are there when `decompress`
///    returns, so the caller keeps the buffer, and clears or frees it, as any other of its own.
///  - `AsyncDecompressor` returns before the bytes are there, so it owns the buffer they go to, which must live until
///    the device is done writing it and then reading it. The caller only borrows it between `wait` and `release`.

/// Uploads compressed bytes from the host and expands them into the caller's device buffer, blocking until done.
class SyncDecompressor
{
public:
    void decompress(GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination);

private:
    DecompressionBatch batch;
    DeviceBuffer device_compressed{StreamRegistry::get().decompression};
};

/// Expands already uploaded pieces into an owned buffer without blocking:
/// `launch` queues the work, `wait` blocks and returns the expanded bytes, which stay valid until `release`,
/// `release` hands the buffer back once the work the caller queued on the compute stream to read it is queued, and
/// the next `launch` waits for that work before it writes the buffer again.
class AsyncDecompressor
{
public:
    AsyncDecompressor();
    ~AsyncDecompressor();

    AsyncDecompressor(AsyncDecompressor && other) noexcept;

    AsyncDecompressor(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(const AsyncDecompressor &) = delete;
    AsyncDecompressor & operator=(AsyncDecompressor &&) = delete;

    void launch(GPUCodec codec, std::span<const CompressedPiece> pieces);

    const char * wait();

    void release();

private:
    DecompressionBatch batch;

    bool in_flight = false;

    DeviceBuffer values{StreamRegistry::get().decompression};
    cudaEvent_t values_copied_out = nullptr;
    bool values_in_use = false;
};

}

#endif
