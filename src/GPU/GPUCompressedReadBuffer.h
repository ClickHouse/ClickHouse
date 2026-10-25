#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDecompression.h>
#include <GPU/GPUMemory.h>

#include <Compression/CompressedReadBufferBase.h>
#include <IO/ByteRangeSet.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

/** Reads a file of compressed blocks, as `CompressedReadBufferFromFile` does, but expands them on the GPU: it reads a
  * batch of blocks ahead, expands them in one call on the device, on a stream of its own, and copies what they expand to
  * back to pinned host memory, from which it gives a block at a time without another copy. Every block of a codec the
  * device expands goes to it - `LZ4`, `ZSTD`, `GDeflate`, alone or after `BitPacking`; only a block of a codec it does
  * not, such as `Delta` or `NONE`, is decompressed on the CPU. Seeks, as `CompressedReadBufferFromFile`'s, are lazy, and
  * cost nothing within the batch. With a request map, a batch takes only the blocks within its ranges, so that a read of
  * sparse ranges of marks expands only what it needs, in batches as large as of a whole file.
  *
  * Two batches are in two slots: as the blocks of one are given out, the device expands the next, so that the time of
  * the device and that of whoever reads the blocks overlap. The next is read ahead only after a full batch, as a column
  * that ends within its first batch has nothing to read ahead. The slots, with their streams and their memory on the
  * device and pinned, are taken from a pool of the server and given back at the end, so that a read pays neither for
  * making a stream nor for the first allocations of its memory, which cost a small read more than its blocks.
  *
  * `ZSTD` blocks are decompressed on the CPU unless `zstd_on_device_` says otherwise: the device expands them slower
  * than the disk delivers them, and far slower than the cores do, see `gpu_read_decompression_zstd`.
  */
class GPUCompressedReadBuffer final : public CompressedReadBufferBase, public ReadBuffer
{
public:
    GPUCompressedReadBuffer(std::unique_ptr<ReadBufferFromFileBase> file_in_, bool allow_different_codecs_, bool zstd_on_device_ = false);
    ~GPUCompressedReadBuffer() override;

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

    /// A block of a batch: where it starts in the file, and what it expands to in the slot's `expanded`.
    struct Block
    {
        size_t offset_in_file = 0;
        size_t offset_in_expanded = 0;
        size_t decompressed_bytes = 0;
    };

    /// A batch of blocks, and what the device is given and gives back for it, on a stream of its own.
    struct Slot
    {
        Slot();

        GPU::StreamPtr stream;

        std::vector<Block> batch;
        /// What the blocks of the batch expand to, one after another.
        GPU::PinnedBuffer expanded;
        /// The compressed blocks staged for the device, and their description.
        GPU::PinnedBuffer staged;
        std::vector<GPU::CompressedBlock> device_blocks;
        std::vector<GPU::BitPackingBlock> device_packed;
        std::optional<GPU::GPUCodec> device_codec;
        size_t device_compressed_bytes = 0;
        /// What the blocks for the device expand to, which come first in `expanded`.
        size_t device_blocks_expanded = 0;

        GPU::DeviceBuffer device_compressed;
        GPU::DeviceBuffer device_expanded;
        GPU::DecompressionBatch decompression;
        /// In the stream, after the expanded bytes are back in `expanded`.
        GPU::EventPtr landed = GPU::createEvent();
        /// Whether the device works on the batch: `finish` waits for it.
        bool in_flight = false;
        /// Whether the batch stopped at the batch size, not at the end of what is read: a next batch may follow.
        bool full = false;
    };

    /// Reads the next blocks of the file into `slot`, up to `max_batch_bytes` of what they expand to, and starts their
    /// expansion on the device without waiting for it. False where there is nothing more to read.
    bool startBatch(Slot & slot);

    /// Waits for the device to be done with `slot`'s batch, and checks what it did.
    void finish(Slot & slot);

    /// Makes the next batch the current one, which is the one read ahead where there is one, and reads the one after
    /// it ahead. False at the end of the file.
    bool advance();

    /// Of a request map, moves the file to the next range of it unless it is in one. False past the last range.
    bool skipToRequestedRange();

    /// The slots of the server not in use: a read takes its slots here and gives them back at its end.
    class SlotPool;

    std::unique_ptr<ReadBufferFromFileBase> file_in;
    const bool zstd_on_device;

    std::unique_ptr<Slot> slots[2];
    /// The batch whose blocks are given out, and the one read ahead, if any: each is one of `slots` or nothing.
    Slot * current = nullptr;
    Slot * ahead = nullptr;
    size_t next_block = 0;

    /// See `setRequestMap`: nothing for a read of the whole file.
    ByteRangeSet request_map;
};

}

#endif
