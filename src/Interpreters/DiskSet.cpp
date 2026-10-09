#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
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
    explicit BlockReader(const DiskSetImpl & set_)
        : set(set_)
        , in(set.file->readRaw(BlockFormat::block_bytes))
    {
    }

    /// Returns the keys of `block`, which stay valid until the next call. It reads and decodes the block
    /// unless the previous call already did.
    std::span<const Key> read(size_t block)
    {
        if (block != current_block)
        {
            in->seek(set.directory.getOffset(block), SEEK_SET);
            CompressedReadBuffer compressed(*in);
            current_keys = decoder.decode(compressed, set.directory.getFirstKey(block));

            ProfileEvents::increment(ProfileEvents::ExternalSetReadBlocks);
            current_block = block;
        }
        return current_keys;
    }

private:
    /// The set whose file and directory the reader reads. It outlives the reader, which exists for one call.
    const DiskSetImpl & set;

    /// The reader reads the file through its own buffer of the size of a block. A frame that does not
    /// compress, such as one of hashes, is slightly larger, so it takes two reads.
    std::unique_ptr<SeekableReadBuffer> in;

    typename BlockFormat::Decoder decoder;

    /// The reader remembers the block that it decoded last and its keys, so that the probes of one block
    /// share one read.
    std::optional<size_t> current_block;
    std::span<const Key> current_keys;
};

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
WriteBuffer & DiskSetImpl<Key>::startBlock(const Key & first_key)
{
    file->next();
    directory.addBlock(first_key, file->getCompressedWriteBuffer().getCompressedBytes());
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

    BlockReader blocks(*this);
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
