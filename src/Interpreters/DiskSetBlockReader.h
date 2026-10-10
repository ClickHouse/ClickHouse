#pragma once

#include <Interpreters/DiskSetImpl.h>

#include <Compression/CompressedReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <optional>
#include <span>

namespace ProfileEvents
{
extern const Event ExternalSetReadBlocks;
}

namespace DB
{

/// Reads the blocks of the file through its own buffer. Each lookup call creates one, so concurrent calls
/// share only the file and the directory, which are immutable, without serializing reads behind a mutex.
template <typename Key>
class DiskSetImpl<Key>::BlockReader
{
public:
    /// Prepares to read `needed_blocks` blocks, which span `range_blocks` blocks of the file.
    BlockReader(const DiskSetImpl & set_, size_t needed_blocks, size_t range_blocks)
        : set(set_)
        , in(set.file->readRaw(getBufferSize(set, needed_blocks, range_blocks)))
        , compressed(*in, /*allow_different_codecs_=*/true)
    {
    }

    ~BlockReader() { ProfileEvents::increment(ProfileEvents::ExternalSetReadBlocks, read_blocks); }

    /// Returns the keys of `block`, which stay valid until the next call. It reads and decodes the block
    /// unless the previous call already did. The calls come in ascending order of blocks, so that a coalesced
    /// read serves the blocks after the one that it starts at.
    std::span<const Key> read(size_t block)
    {
        if (block != current_block)
        {
            /// The decoder has read the whole previous block, so the compressed buffer takes the next frame
            /// from the new position, which the buffer of the file may already hold.
            in->seek(set.directory.getOffset(block), SEEK_SET);
            compressed.next();
            current_keys = decoder.decode(compressed, set.directory.getFirstKey(block));

            ++read_blocks;
            current_block = block;
        }
        return current_keys;
    }

private:
    /// A coalesced read takes the frames of this many neighboring blocks. It saves reads when the needed
    /// blocks are on average closer than this, and it costs more than it saves when they are farther apart,
    /// because it copies all the frames between them.
    static constexpr size_t frames_per_coalesced_read = 16;

    /// Returns the size of the buffer: the largest frame of a block, with the bytes that its codec needs
    /// after it, or the frames of a coalesced read when the needed blocks are close enough.
    static size_t getBufferSize(const DiskSetImpl & set, size_t needed_blocks, size_t range_blocks)
    {
        const size_t max_frame_bytes = std::max(
            CompressedReadBufferBase::getInPlaceInputBufferSize(*set.compressible_block_codec, BlockFormat::max_block_bytes_with_header),
            CompressedReadBufferBase::getInPlaceInputBufferSize(*set.incompressible_block_codec, BlockFormat::max_block_bytes_with_header));
        const bool coalesce_reads = range_blocks <= needed_blocks * frames_per_coalesced_read;
        return (coalesce_reads ? frames_per_coalesced_read : 1) * max_frame_bytes;
    }

    /// The set whose file and directory the reader reads. It outlives the reader, which exists for one call.
    const DiskSetImpl & set;

    /// The buffer holds whole frames, so that the frame of a block is decompressed where it is read instead
    /// of being copied first, except a frame that the end of a coalesced read cuts.
    std::unique_ptr<SeekableReadBuffer> in;

    /// One compressed buffer serves all the blocks of the call, so that it allocates its memory once. It
    /// accepts frames of both codecs, because the file compresses only the blocks that a codec can shrink.
    CompressedReadBuffer compressed;

    typename BlockFormat::Decoder decoder;

    /// The reader remembers the block that it decoded last and its keys, so that the probes of one block
    /// share one read.
    std::optional<size_t> current_block;
    std::span<const Key> current_keys;

    /// The reader reports the blocks that it reads once, because all lookup threads increment the same
    /// counters of profile events, which makes an increment for each block a noticeable part of reading it.
    size_t read_blocks = 0;
};

}
