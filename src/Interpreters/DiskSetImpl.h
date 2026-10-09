#pragma once

#include <Interpreters/DiskSet.h>
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
/// keeps the first key and the file offset of each block, so a lookup reads only the blocks that its keys
/// can be in. The set is written in ascending batches of keys, and lookups start after `finishWriting`.
template <typename Key>
class DiskSetImpl final : public DiskSet, private DiskSetBlockFormat<Key>::BlockSink
{
public:
    /// A set without keys has no file.
    DiskSetImpl() = default;

    /// Creates the file of the set in `tmp_data`. The number of distinct keys is unknown until the merge
    /// ends, so creating the file checks only that `min_free_disk_space` is free.
    DiskSetImpl(TemporaryDataOnDiskScopePtr tmp_data, size_t min_free_disk_space)
        : file(std::make_unique<TemporaryDataBuffer>(std::move(tmp_data), min_free_disk_space))
        , encoder(std::in_place)
    {
    }

    /// Adds `keys`, which ascend and are greater than the keys added before.
    void add(std::span<const Key> keys);

    /// Writes the last block and completes the file.
    void finishWriting();

    /// Probes of one call that fall into the same block share a single read of it.
    void containsBatch(const IColumn & keys, std::span<UInt8> found) const override;

    size_t getTotalRowCount() const override { return rows; }

    /// Returns the memory of the directory, which grows with each block that the set writes. Once the set is
    /// written, the directory is all that it keeps in memory.
    size_t getTotalByteCount() const override { return directory.allocatedBytes(); }

private:
    using BlockFormat = DiskSetBlockFormat<Key>;

    /// The encoder calls this before it writes each block, and writes the block into the returned buffer. It
    /// ends the frame of the previous block, so that a lookup can seek to each block and decompress it
    /// alone. It also records the first key of the block and the position of its frame in the directory.
    WriteBuffer & startBlock(const Key & first_key) override;

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

        /// Returns the only block that can hold `key`, or nothing when `key` is below the first block.
        std::optional<size_t> findBlock(const Key & key) const
        {
            const auto it = std::upper_bound(
                entries.begin(), entries.end(), key, [](const Key & value, const Entry & entry) { return value < entry.first_key; });
            if (it == entries.begin())
                return {};
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

    std::optional<typename BlockFormat::Encoder> encoder;

    /// Reads the blocks of the file for one lookup call.
    class BlockReader;

    /// The set counts the keys of its pending block too, so that the limit on rows sees every key.
    size_t rows = 0;

    /// Lookups skip the keys greater than the largest key of the set, and `add` requires new keys to be
    /// greater.
    Key last_key{};
};

}
