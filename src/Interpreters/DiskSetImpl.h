#pragma once

#include <Interpreters/DiskSet.h>
#include <Interpreters/DiskSetBlockCache.h>
#include <Interpreters/DiskSetBlockFilters.h>
#include <Interpreters/DiskSetBlockFormat.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/TemporaryFileFrameReader.h>
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

    /// Creates the file of the set in `tmp_data`, as `createFileScope` describes. The size of the file is
    /// unknown until the merge ends, so creating it checks only that `min_free_disk_space` is free. The set
    /// keeps Bloom filters of its blocks in at most `max_bytes_for_bloom_filter` bytes, sized for about
    /// `expected_keys` keys, as `DiskSetBlockFilters::create` decides, and a cache of the blocks that its
    /// lookups read in at most `max_bytes_for_cache` bytes.
    DiskSetImpl(
        TemporaryDataOnDiskScopePtr tmp_data,
        size_t min_free_disk_space,
        size_t expected_keys,
        size_t max_bytes_for_bloom_filter,
        size_t max_bytes_for_cache);

    /// Returns about the memory that a set of `keys` keys keeps besides its cache: its directory, and the
    /// Bloom filters that the constructor creates for `keys` expected keys with `max_bytes_for_bloom_filter`.
    static size_t estimateMemory(size_t keys, size_t max_bytes_for_bloom_filter);

    /// Adds `keys`, which ascend and are greater than the keys added before.
    void add(std::span<const Key> keys);

    /// Writes the last block and completes the file.
    void finishWriting();

    /// Probes of one call that fall into the same block share a single read of it.
    void containsBatch(const IColumn & keys, std::span<UInt8> found) const override;

    size_t getTotalRowCount() const override { return rows; }

    /// Returns the bytes of the keys at the width of `Key`. The blocks pack close keys, but the keys count at
    /// their full width, so the limit on bytes keeps the same number of keys whatever their values are. The
    /// memory that lookups use does not count. The directory is small next to the keys: it takes 16 to 40
    /// bytes per block, and every block but the last holds at least 4 KiB of keys. The filters and the cache
    /// have budgets of their own.
    size_t getTotalByteCount() const override { return rows * sizeof(Key); }

private:
    using BlockFormat = DiskSetBlockFormat<Key>;

    /// The encoder calls this before it writes each block, and writes the block into the returned buffer. It
    /// ends the frame of the previous block, so that a lookup can read each block alone. It also records the
    /// first key of the block and the position of its frame in the directory, and adds the filter of the
    /// block.
    WriteBuffer & startBlock(std::span<const Key> block_keys) override;

    /// Returns the scope of the file of the set: a child of `tmp_data` that writes frames without compression
    /// (see `file`) and has a buffer that holds a whole block. The file also ends a frame when its buffer
    /// fills, so a smaller buffer would split a block into several frames, while a lookup reads each block as
    /// one frame.
    static TemporaryDataOnDiskScopePtr createFileScope(const TemporaryDataOnDiskScopePtr & tmp_data);

    /// The file holds the blocks in ascending order of their keys, one frame per block. The frames are not
    /// compressed, whatever the codec of the temporary data is. The encoder already packs the gaps between
    /// close keys, so a codec shrinks only the unpacked blocks of keys with repeating bytes, such as short
    /// `FixedString` values with their padding. Decompressing each block that a lookup reads costs more than
    /// reading the larger file from the page cache. With `LZ4`, the lookups that read the file are up to 2
    /// times slower for such keys, and up to 17% slower for hashes, which `LZ4` makes slightly larger.
    TemporaryDataBufferPtr file;

    /// The lookups of all threads read the blocks of `file` through this, which `finishWriting` creates
    /// when it completes the file.
    std::optional<TemporaryFileFrameReader::SharedFile> shared_file;

    /// The first key and the file position of each block, in file order, and the size of the file. A block
    /// holds the keys up to the first key of the next block, so a lookup searches the block with the greatest
    /// first key that does not exceed the probe.
    class Directory
    {
    public:
        /// Appends the next block, whose first key is greater than the keys of the blocks before it.
        void addBlock(const Key & first_key, size_t offset) { entries.push_back({first_key, offset}); }

        /// Records the size of the complete file, where the frame of the last block ends.
        void setFileSize(size_t file_size_) { file_size = file_size_; }

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

        /// Returns the position of the frame of `block` in the file.
        size_t getOffset(size_t block) const { return entries[block].offset; }

        /// Returns the position in the file after the frame of `block`: the position of the frame of the next
        /// block, or the size of the file for the last block.
        size_t getEndOffset(size_t block) const { return block + 1 < entries.size() ? entries[block + 1].offset : file_size; }

        size_t size() const { return entries.size(); }
        size_t allocatedBytes() const { return entries.capacity() * sizeof(Entry); }

        /// Returns about the memory of a directory of `blocks` blocks.
        static size_t estimateBytes(size_t blocks) { return blocks * sizeof(Entry); }

    private:
        struct Entry
        {
            /// The smallest key of the block.
            Key first_key;
            size_t offset;
        };

        VectorWithMemoryTracking<Entry> entries;
        size_t file_size = 0;
    };

    Directory directory;

    /// The Bloom filters of the blocks, which rule out keys without reading the blocks. The set has none when
    /// `max_bytes_for_bloom_filter` is 0, or too small to give the filters at least 1 bit per key, as
    /// `DiskSetBlockFilters::create` decides.
    std::optional<DiskSetBlockFilters<Key>> filters;

    /// The cache of the set keeps in memory blocks that lookups read from the file. A lookup call takes the
    /// blocks that the cache holds from the cache instead of reading them from the file, and adds the blocks
    /// that it reads to the cache if the cache admits them. The lookup calls of all threads share the cache,
    /// which synchronizes them. The set has no cache when `max_bytes_for_cache` is 0.
    std::unique_ptr<DiskSetBlockCache> block_cache;

    std::optional<typename BlockFormat::Encoder> encoder;

    /// Reads the blocks of the file for one lookup call. `DiskSetBlockReader.h` defines it.
    class BlockReader;

    /// The set counts the keys of its pending block too, so that the limit on rows sees every key.
    size_t rows = 0;

    /// Lookups skip the keys greater than the largest key of the set, and `add` requires new keys to be
    /// greater.
    Key last_key{};
};

}
