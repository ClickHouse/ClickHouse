#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDecompression.h>
#include <GPU/GPUMemory.h>

#include <Compression/CompressedReadBufferBase.h>
#include <IO/ReadBuffer.h>
#include <IO/ByteRangeSet.h>
#include <IO/ReadBufferFromFileBase.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/** Reads a file of compressed blocks, as `CompressedReadBufferFromFile` does, but expands them on the GPU: it reads a
  * batch of blocks ahead, expands them in one call on the device, on a stream of its own, and copies what they expand to
  * back to pinned host memory, from which it gives a block at a time without another copy. With a request map, a batch
  * takes only the blocks within its ranges, so that a read of sparse ranges of marks expands only what it needs, in
  * batches as large as of a whole file. Every block of a codec the
  * device expands goes to it - `LZ4`, `ZSTD`, `GDeflate`, alone or after `BitPacking`; only a block of a codec it does
  * not, such as `Delta` or `NONE`, is decompressed on the CPU. Seeks, as `CompressedReadBufferFromFile`'s, are lazy, and
  * cost nothing within the batch.
  */
class GPUCompressedReadBuffer final : public CompressedReadBufferBase, public ReadBuffer
{
public:
    GPUCompressedReadBuffer(std::unique_ptr<ReadBufferFromFileBase> file_in_, bool allow_different_codecs_);

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block) override;

    off_t getPosition() const override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type = CLOCK_MONOTONIC_COARSE)
    {
        file_in->setProfileCallback(profile_callback, clock_type);
    }

    void setReadUntilPosition(size_t position) override { file_in->setReadUntilPosition(position); }
    void setReadUntilEnd() override { file_in->setReadUntilEnd(); }
    /// The ranges of the file that will be read: a batch takes only the blocks within them, and goes past the others.
    void setRequestMap(ByteRangeSet ranges) override;

private:
    bool nextImpl() override;

    /// Reads the next blocks of the file, up to `batch_limit` of what they expand to, and expands them.
    bool readBatch();

    /// Of a request map, moves the file to the next range of it unless it is in one. False past the last range.
    bool skipToRequestedRange();

    /// A block of the batch: where it starts in the file, and what it expands to in `expanded`.
    struct Block
    {
        size_t offset_in_file = 0;
        size_t offset_in_expanded = 0;
        size_t decompressed_bytes = 0;
    };

    std::unique_ptr<ReadBufferFromFileBase> file_in;

    std::vector<Block> batch;
    size_t next_block = 0;
    /// What the blocks of the batch expand to, one after another, and the compressed blocks staged for the device.
    GPU::PinnedBuffer expanded;
    GPU::PinnedBuffer staged;
    std::vector<GPU::CompressedBlock> device_blocks;
    std::vector<GPU::BitPackingBlock> device_packed;
    /// Of the batch being read: what its blocks for the device expand to, which are all of it until a block for the CPU.
    size_t device_blocks_expanded = 0;
    /// See `setRequestMap`: nothing for a read of the whole file.
    ByteRangeSet request_map;

    GPU::StreamPtr stream;
    std::optional<GPU::SyncDecompressor> decompressor;
};

}

#endif
