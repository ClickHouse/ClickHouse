#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/SeekableReadBuffer.h>
#include <base/sort.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <functional>

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

template <typename Key>
DiskSetImpl<Key>::DiskSetImpl(TemporaryDataOnDiskScopePtr tmp_data, size_t min_free_disk_space)
    : file(std::make_unique<TemporaryDataBuffer>(std::move(tmp_data), min_free_disk_space))
    , encoder(std::in_place)
    , compressible_block_codec(file->getCompressedWriteBuffer().getCodec())
    , incompressible_block_codec(CompressionCodecFactory::instance().get("NONE"))
{
}

template <typename Key>
void DiskSetImpl<Key>::add(std::span<const Key> keys)
{
    chassert(file && !keys.empty());
    chassert(!rows || keys.front() > last_key);
    chassert(std::ranges::adjacent_find(keys, std::greater_equal<>()) == keys.end());

    last_key = keys.back();
    rows += keys.size();
    encoder->add(keys, *this);
}

template <typename Key>
WriteBuffer & DiskSetImpl<Key>::startBlock(const Key & first_key, bool compressible)
{
    /// `next` must end the frame of the previous block before `setCodec` picks the codec of this one. The
    /// file writes into the memory of its compressed buffer but keeps its own position there, so only `next`
    /// hands the bytes of the previous block to the compressed buffer.
    file->next();
    auto & compressed = file->getCompressedWriteBuffer();
    compressed.setCodec(compressible ? compressible_block_codec : incompressible_block_codec);
    directory.addBlock(first_key, compressed.getCompressedBytes());
    return *file;
}

template <typename Key>
void DiskSetImpl<Key>::finishWriting()
{
    if (!file)
        return;

    /// The encoder keeps the keys of the last block pending until now. Lookups need none of its buffers.
    encoder->finish(*this);
    encoder.reset();

    file->finishWriting();
    LOG_TRACE(
        getLogger("DiskSet"),
        "Created set on disk with {} keys of {} bytes, {} blocks, {} bytes of directory, {} bytes in temporary file {}",
        rows,
        sizeof(Key),
        directory.size(),
        getTotalByteCount(),
        file->getStat().compressed_size,
        file->describeFilePath());
}

template <typename Key>
void DiskSetImpl<Key>::containsBatch(const IColumn & keys_column, std::span<UInt8> found) const
{
    const auto & keys = assert_cast<const ColumnVector<Key> &>(keys_column).getData();
    chassert(keys.size() == found.size());
    std::fill(found.begin(), found.end(), 0);

    struct Probe
    {
        size_t block;
        size_t row;
    };

    VectorWithMemoryTracking<Probe> probes;
    probes.reserve(keys.size());
    for (size_t row = 0; row < keys.size(); ++row)
    {
        const auto & key = keys[row];
        if (key > last_key)
            continue;

        if (const auto block = directory.findBlock(key))
            probes.push_back({*block, row});
    }

    if (probes.empty())
        return;

    /// Probes from one input chunk share a read when they land in the same block. Results are
    /// scattered back to their original rows, so evaluating `IN` does not reorder the input.
    ::sort(probes.begin(), probes.end(), [](const Probe & lhs, const Probe & rhs) { return lhs.block < rhs.block; });

    /// The reader sizes its reads from how many blocks the probes need and how far apart they are.
    size_t needed_blocks = 1;
    for (size_t i = 1; i < probes.size(); ++i)
        needed_blocks += probes[i].block != probes[i - 1].block;

    BlockReader blocks(*this, needed_blocks, /*range_blocks=*/probes.back().block - probes.front().block + 1);
    for (const Probe & probe : probes)
    {
        const auto block_keys = blocks.read(probe.block);
        found[probe.row] = std::binary_search(block_keys.begin(), block_keys.end(), keys[probe.row]);
    }
}

template class DiskSetImpl<UInt8>;
template class DiskSetImpl<UInt16>;
template class DiskSetImpl<UInt32>;
template class DiskSetImpl<UInt64>;
template class DiskSetImpl<UInt128>;
template class DiskSetImpl<UInt256>;

}
