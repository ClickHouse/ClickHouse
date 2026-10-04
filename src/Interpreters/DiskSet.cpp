#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <base/sort.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>

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
        , in(set.file->readRaw(block_bytes))
    {
    }

    /// Returns the keys of `block`, which stay valid until the next call. It reads and decompresses the block
    /// unless the previous call already did.
    std::span<const Key> read(size_t block)
    {
        if (block != current_block)
        {
            num_keys = std::min(keys_per_block, set.rows - block * keys_per_block);
            in->seek(set.directory.getOffset(block), SEEK_SET);

            CompressedReadBuffer compressed(*in);
            compressed.readStrict(reinterpret_cast<char *>(keys.data()), num_keys * sizeof(Key));

            ProfileEvents::increment(ProfileEvents::ExternalSetReadBlocks);
            current_block = block;
        }
        return {keys.data(), num_keys};
    }

private:
    /// The set whose file and directory the reader reads. It outlives the reader, which exists for one call.
    const DiskSetImpl & set;

    /// The reader's own buffer over the file, of `block_bytes`. A frame that does not compress, such as one
    /// of hashes, is slightly larger and takes two reads.
    std::unique_ptr<SeekableReadBuffer> in;

    /// The decompressed keys of `current_block`.
    std::array<Key, keys_per_block> keys{};

    /// The block in `keys`, or nothing before the first read.
    std::optional<size_t> current_block;

    /// The number of keys in `current_block`, fewer than `keys_per_block` only in the last block of the file.
    size_t num_keys = 0;
};

template <typename Key>
void DiskSetImpl<Key>::finishWriting()
{
    if (!file)
        return;

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
