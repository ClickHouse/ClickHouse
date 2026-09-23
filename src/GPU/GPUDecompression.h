#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.h>

#include <cstddef>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::GPU
{

/// One compressed block of a column as it sits in a `MergeTree` part, within a staging buffer.
struct CompressedBlock
{
    size_t offset = 0;
    size_t compressed_bytes = 0;
    size_t decompressed_bytes = 0;
};

size_t decompressedBytesOf(std::span<const CompressedBlock> blocks);

/** Expands compressed blocks on the device with nvcomp, so that what crosses the PCIe link is the
  * compressed column rather than the values.
  *
  * nvcomp takes its arguments - a pointer and a size per block, in and out - from device memory and
  * leaves a status and a size per block there, so a call is one upload, the kernel, one copy back
  * and one wait. The buffers for all of that are kept between calls: a column is decompressed one
  * staging buffer at a time, and each call would otherwise allocate seven of them.
  *
  * Everything runs on the decompression stream, which the default stream does not wait for. So
  * the wait at the end of a call is for the expansion alone, and the kernels the default stream
  * has queued over the last batch run through it.
  */
class Decompressor
{
public:
    /// One run of compressed blocks already in device memory.
    struct Piece
    {
        const char * device_compressed = nullptr;
        size_t compressed_bytes = 0;
        std::span<const CompressedBlock> blocks;
        /// When the run has landed on the device; recorded on the stream that copied it. Null for
        /// a run the host has already waited for.
        const DeviceEvent * uploaded = nullptr;
    };

    Decompressor();
    ~Decompressor();

    Decompressor(Decompressor && other) noexcept;

    Decompressor(const Decompressor &) = delete;
    Decompressor & operator=(const Decompressor &) = delete;
    Decompressor & operator=(Decompressor &&) = delete;

    /// Sends the blocks as they are and expands them one after another at `device_destination`,
    /// memory the default stream allocated, after everything the default stream has queued.
    /// Complete when it returns.
    void decompress(GPUCodec codec, std::string_view host_compressed, std::span<const CompressedBlock> blocks, char * device_destination);

    /// Queues the expansion of several runs into a buffer of the decompressor's own, one run after
    /// another in the order given, each `decompressedBytesOf` its blocks long, and returns at once.
    /// One expansion is in flight at a time. The buffer is one of two: it is written again by the
    /// expansion after the next, and only once the default stream has passed the point `release`
    /// marks.
    void start(GPUCodec codec, std::span<const Piece> pieces);

    /// Waits for the started expansion, checks that every block came out whole and answers where
    /// the values are.
    const char * finish();

    /// Marks, on the default stream, that everything queued so far may read the last answered
    /// buffer, and nothing queued later needs to. Called once the copies out of it are queued.
    void release();

private:
    struct Slot
    {
        DeviceBuffer values;
        DeviceEvent copied_out;
        bool in_use = false;
    };

    static constexpr size_t num_slots = 2;

    /// Queues the expansion of the pieces to `destination` on the decompression stream.
    void queue(GPUCodec codec, std::span<const Piece> pieces, char * destination);

    /// Waits for the queued expansion and checks that every block came out whole.
    void waitAndCheck();

    bool in_flight = false;
    char * started_destination = nullptr;

    PinnedBuffer host_arguments;
    PinnedBuffer host_results;

    DeviceBuffer device_compressed;
    DeviceBuffer device_arguments;
    DeviceBuffer device_results;
    DeviceBuffer device_temp;

    DeviceEvent expanded;
    std::vector<size_t> expected_bytes;

    Slot slots[num_slots];
    size_t current_slot = 0;
};

}

#endif
