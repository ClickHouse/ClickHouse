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
    /// Without it, the piece expands after the piece before.
    char * destination = nullptr;
};

/// One batched nvcomp call, on the decompression stream unless the caller gives its own. Codecs other than those of
/// `GPUCodec` are decompressed by the CPU.
class DecompressionBatch
{
public:
    explicit DecompressionBatch(cuda::stream_ref stream_ = GPUResources::get().decompression);

    /// Writes the destination after `destination_ready_event`, or else after what the compute stream has queued.
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

}

#endif
