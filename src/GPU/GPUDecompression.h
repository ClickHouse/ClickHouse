#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUFuture.h>
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

/// Adds what nvcomp expands of a block of `codec`, `payload`, staged at `offset`, to `blocks`: the block, or each chunk
/// of a `GDeflate` one, in order. Throws on a block that does not hold the chunks it declares.
void addCompressedBlocksOf(
    GPUCodec codec, std::string_view payload, size_t offset, size_t decompressed_bytes, std::vector<CompressedBlock> & blocks);

struct CompressedHostColumn;

/// The compressed blocks of a batch, which the device expands on `stream`: `add` sends the blocks of a stream to the
/// device, to be expanded to a destination there, and `expand` expands all of them, a call of nvcomp per codec.
class DecompressionBatch
{
public:
    explicit DecompressionBatch(cuda::stream_ref stream_);

    /// Sends the blocks of `column`, from pinned memory, to be expanded to `destination`, which is of this stream.
    void add(const CompressedHostColumn & column, char * destination);

    /// Expands what was added and checks what each block expanded to, which waits for the device; the host memory of
    /// what was added is read until then. The future is of the expanded bytes in their destinations.
    GPUFuture<void> expand();

private:
    /// The blocks of a stream on the device, and where they expand to.
    struct Piece
    {
        const char * device_compressed = nullptr;
        size_t compressed_bytes = 0;
        std::span<const CompressedBlock> blocks;
        char * destination = nullptr;
    };

    struct Move
    {
        const char * from;
        char * to;
        size_t bytes;
    };

    std::vector<Piece> & piecesOf(GPUCodec codec);

    /// One batched call of nvcomp, whose future is of the statuses back on the host, which `check` reads.
    GPUFuture<void> launch(GPUCodec codec, std::span<const Piece> pieces);
    void check(GPUFuture<void> expanded);

    const cuda::stream_ref stream;

    std::vector<DeviceBuffer> uploaded;
    std::vector<std::pair<GPUCodec, std::vector<Piece>>> pieces_by_codec;
    /// nvcomp takes only aligned destinations: the blocks of others expand elsewhere and are copied in place.
    std::vector<DeviceBuffer> expanded_elsewhere;
    std::vector<Move> moves;

    PinnedBuffer host_arguments;
    PinnedBuffer host_results;

    DeviceBuffer device_arguments{stream};
    DeviceBuffer device_results{stream};
    DeviceBuffer device_temp{stream};

    std::vector<size_t> expected_bytes;
};

}

#endif
