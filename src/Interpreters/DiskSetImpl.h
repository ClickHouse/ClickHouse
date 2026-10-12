#pragma once

#include <Interpreters/DiskSet.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <functional>
#include <optional>

namespace DB
{

/// The `DiskSet` of keys of type `Key`, an unsigned integer of 8 to 256 bits. It stores the distinct
/// keys in ascending order, in independently compressed blocks of one temporary file. A directory in
/// memory keeps the first key and the file offset of each block, so a lookup reads only the blocks
/// that its keys can be in. The set is written in ascending batches of keys, and lookups start after
/// `finishWriting`.
template <typename Key>
class DiskSetImpl final : public DiskSet
{
public:
    /// A set without keys has no file.
    DiskSetImpl() = default;

    /// Creates the file of the set in `tmp_data`. The number of distinct keys is unknown until the merge
    /// ends, so creating the file checks only that `min_free_disk_space` is free.
    DiskSetImpl(TemporaryDataOnDiskScopePtr tmp_data, size_t min_free_disk_space)
        : file(std::make_unique<TemporaryDataBuffer>(std::move(tmp_data), min_free_disk_space))
    {
    }

    /// Writes `keys`, which ascend and are greater than the keys written before.
    void add(std::span<const Key> keys)
    {
        chassert(file && !keys.empty());
        chassert(!rows || keys.front() > last_key);
        chassert(std::ranges::adjacent_find(keys, std::greater_equal<>()) == keys.end());

        last_key = keys.back();
        while (!keys.empty())
        {
            const size_t row_in_block = rows % keys_per_block;
            if (row_in_block == 0)
            {
                /// Each block starts a compressed frame, so a lookup can decompress it from its offset alone.
                file->next();
                directory.addBlock(keys.front(), file->getCompressedWriteBuffer().getCompressedBytes());
            }

            /// One write copies the keys that fit in the rest of the block.
            const size_t count = std::min(keys.size(), keys_per_block - row_in_block);
            file->write(reinterpret_cast<const char *>(keys.data()), count * sizeof(Key));
            rows += count;
            keys = keys.subspan(count);
        }
    }

    /// Completes the file. Call once, after the last `add`.
    void finishWriting();

    /// Probes of one call that fall into the same block share a single read of it.
    void containsBatch(const IColumn & keys, std::span<UInt8> found) const override;

    size_t getTotalRowCount() const override { return rows; }

    /// The directory is all that the set keeps in memory.
    size_t getTotalByteCount() const override { return directory.allocatedBytes(); }

private:
    /// Every block except the last holds this many bytes of keys. A lookup reads and decompresses whole
    /// blocks, so the size trades the bytes read per probe against the number of directory entries.
    static constexpr size_t block_bytes = 4096;
    static constexpr size_t keys_per_block = block_bytes / sizeof(Key);

    /// The keys in ascending order, one compressed frame per block.
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

    /// Reads the blocks of the file for one lookup call.
    class BlockReader;

    /// The number of keys in the file. Since every block but the last holds `keys_per_block` keys, it also
    /// gives the block boundaries.
    size_t rows = 0;

    /// The largest key in the file. Lookups skip greater keys, and `add` requires new keys to be greater.
    Key last_key{};
};

}
