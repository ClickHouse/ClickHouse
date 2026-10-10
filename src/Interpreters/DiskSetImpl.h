#pragma once

#include <Compression/ICompressionCodec.h>
#include <Interpreters/DiskSet.h>
#include <Interpreters/DiskSetBlockFilters.h>
#include <Interpreters/DiskSetBlockFormat.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <optional>

namespace DB
{

/// Implements `DiskSet` for keys of type `Key`, an unsigned integer of 8 to 256 bits. It stores the distinct
/// keys in ascending order, in the blocks of one temporary file that `DiskSetBlockFormat` defines. Each block
/// is a frame of its own, so a lookup can read a block without reading the others. A directory in memory
/// keeps the first key and the file offset of each block, so a lookup reads only the blocks that its keys can
/// be in. The Bloom filters of the blocks, which are in memory too, let lookups of keys that the set does not
/// have skip reading blocks. The set is written in ascending batches of keys, and lookups start after
/// `finishWriting` completes the file.
template <typename Key>
class DiskSetImpl final : public DiskSet, private DiskSetBlockFormat<Key>::BlockSink
{
public:
    /// A set without keys has no file.
    DiskSetImpl() = default;

    /// Creates the file of the set in `tmp_data`. The size of the file is unknown until the merge ends, so
    /// creating it checks only that `min_free_disk_space` is free. The set keeps Bloom filters of its blocks
    /// in at most `max_bytes_for_bloom_filter` bytes, sized for about `expected_keys` keys, as
    /// `DiskSetBlockFilters::create` decides.
    DiskSetImpl(TemporaryDataOnDiskScopePtr tmp_data, size_t min_free_disk_space, size_t expected_keys, size_t max_bytes_for_bloom_filter);

    /// Adds `keys`, which ascend and are greater than the keys added before.
    void add(std::span<const Key> keys);

    /// Writes the last block and completes the file.
    void finishWriting();

    /// Probes of one call that fall into the same block share a single read of it.
    void containsBatch(const IColumn & keys, std::span<UInt8> found) const override;

    size_t getTotalRowCount() const override { return rows; }

    /// Returns the memory of the directory, which grows with each block that the set writes. The filters do
    /// not count, because they have a budget of their own.
    size_t getTotalByteCount() const override { return directory.allocatedBytes(); }

private:
    using BlockFormat = DiskSetBlockFormat<Key>;

    /// The encoder calls this before it writes each block, and writes the block into the returned buffer. It
    /// ends the frame of the previous block, so that a lookup can seek to each block and decompress it alone.
    /// It also records the first key of the block and the position of its frame in the directory, and adds
    /// the filter of the block. An unpacked block holds the keys as they are, so its frame uses the codec of
    /// the file, which can shrink them. A block of packed gaps is already dense, so its frame is not
    /// compressed, which saves the lookups the work of decompressing it.
    WriteBuffer & startBlock(std::span<const Key> block_keys, bool compressible) override;

    /// The file holds the blocks in ascending order of their keys, one frame per block.
    TemporaryDataBufferPtr file;

    /// The first key and the file position of each block, in file order. A block holds the keys up to the
    /// first key of the next block, so a lookup searches the block with the greatest first key that does not
    /// exceed the probe.
    class Directory
    {
    public:
        /// Appends the next block, whose first key is greater than the keys of the blocks before it.
        void addBlock(const Key & first_key, size_t offset) { entries.push_back({first_key, offset}); }

        /// Returns the only block that can hold `key`, which is not below the first key of block `from`.
        /// Lookups of ascending keys pass the block of the previous key, so that a key in the same block
        /// takes one comparison, and the search for another block covers only the blocks after it.
        size_t findBlock(const Key & key, size_t from) const
        {
            chassert(from < entries.size() && entries[from].first_key <= key);
            if (from + 1 == entries.size() || key < entries[from + 1].first_key)
                return from;

            const auto it = std::upper_bound(
                entries.begin() + from + 2, entries.end(), key, [](const Key & value, const Entry & entry) { return value < entry.first_key; });
            return static_cast<size_t>(it - entries.begin() - 1);
        }

        const Key & getFirstKey(size_t block) const { return entries[block].first_key; }

        /// Returns the position of the compressed frame of `block` in the file.
        size_t getOffset(size_t block) const { return entries[block].offset; }

        size_t size() const { return entries.size(); }
        size_t allocatedBytes() const { return entries.capacity() * sizeof(Entry); }

    private:
        struct Entry
        {
            /// The smallest key of the block.
            Key first_key;
            size_t offset;
        };

        VectorWithMemoryTracking<Entry> entries;
    };

    Directory directory;

    /// The Bloom filters of the blocks, which rule out keys without reading the blocks. The set has none when
    /// `max_bytes_for_bloom_filter` is 0, or too small to give the filters at least 1 bit per key, as
    /// `DiskSetBlockFilters::create` decides.
    std::optional<DiskSetBlockFilters<Key>> filters;

    std::optional<typename BlockFormat::Encoder> encoder;

    /// The frames of the blocks that a codec can shrink use the codec of the temporary data scope of the
    /// file. The frames of the other blocks use `NONE`.
    CompressionCodecPtr compressible_block_codec;
    CompressionCodecPtr incompressible_block_codec;

    /// Reads the blocks of the file for one lookup call. `DiskSetBlockReader.h` defines it.
    class BlockReader;

    /// The set counts the keys of its pending block too, so that the limit on rows sees every key.
    size_t rows = 0;

    /// Lookups skip the keys greater than the largest key of the set, and `add` requires new keys to be
    /// greater.
    Key last_key{};
};

}
