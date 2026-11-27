#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <base/types.h>

#include <optional>

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

/// A block of `BitPacking`, see `BitPackingFormat`, that the `num_blocks` blocks from `first_block` expand to after the
/// header of a compressed block: the device unpacks it into `unpacked_bytes` of integers, which are what the column
/// takes of it, where the blocks' own expanded bytes are only read.
struct BitPackingBlock
{
    size_t first_block = 0;
    size_t num_blocks = 0;
    size_t unpacked_bytes = 0;
};

/// What the column takes of the blocks: the integers of the packed ones, and the expanded bytes of the rest.
size_t valueBytesOf(std::span<const CompressedBlock> blocks, std::span<const BitPackingBlock> packed);

/// The codec the device expands a block of `method` with: of the codecs of `GPUCodec`, the block's own, or the second of
/// a chain of `BitPacking` and one of them, whose list of methods `payload` starts with. Nothing for any other.
std::optional<GPUCodec> deviceCodecOf(UInt8 method, std::string_view payload);

/// Adds what nvcomp expands of a block of `method`, `payload`, staged at `offset`, to `blocks`, in order: the block, or
/// each chunk of a `GDeflate` one, and of a chain of `BitPacking` the block it packs to `packed`. Returns the bytes to
/// stage at `offset`: the payload, or of a chain the block of its second codec. Throws on a block that does not hold
/// what it declares.
std::string_view addCompressedBlocksOf(
    UInt8 method,
    std::string_view payload,
    size_t offset,
    size_t decompressed_bytes,
    std::vector<CompressedBlock> & blocks,
    std::vector<BitPackingBlock> & packed);

struct CompressedPiece
{
    const char * device_compressed = nullptr;
    size_t compressed_bytes = 0;
    std::span<const CompressedBlock> blocks;
    std::span<const BitPackingBlock> packed;
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
    /// Where the blocks of `BitPacking` expand to, which the device unpacks into the destination.
    DeviceBuffer device_scratch{stream};
    PinnedBuffer host_streams;
    DeviceBuffer device_streams{stream};
    size_t num_streams = 0;

    EventPtr destination_ready = createEvent();
    EventPtr expanded = createEvent();
    std::vector<size_t> expected_bytes;
};

}

#endif
