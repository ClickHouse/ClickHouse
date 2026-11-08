#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Interpreters/DiskSetBlockReader.h>
#include <base/sort.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <functional>

namespace ProfileEvents
{
extern const Event ExternalSetLookupKeys;
extern const Event ExternalSetBloomFilterSkippedKeys;
}

namespace DB
{

template <typename Key>
DiskSetImpl<Key>::DiskSetImpl(
    TemporaryDataOnDiskScopePtr tmp_data,
    size_t min_free_disk_space,
    size_t expected_keys,
    size_t max_bytes_for_bloom_filter,
    size_t max_bytes_for_cache)
    : file(std::make_unique<TemporaryDataBuffer>(createFileScope(tmp_data), min_free_disk_space))
    , encoder(std::in_place)
{
    if (max_bytes_for_bloom_filter)
        filters = DiskSetBlockFilters<Key>::create(expected_keys, BlockFormat::keys_per_unpacked_block, max_bytes_for_bloom_filter);
    if (max_bytes_for_cache)
        block_cache = std::make_unique<DiskSetBlockCache>(max_bytes_for_cache);
}

template <typename Key>
size_t DiskSetImpl<Key>::estimateMemory(size_t keys, size_t max_bytes_for_bloom_filter)
{
    /// A set without keys has no file, so it has no directory and no filters either.
    if (!keys)
        return 0;

    /// Every block but the last holds at least the keys of an unpacked block.
    size_t bytes = Directory::estimateBytes(keys / BlockFormat::keys_per_unpacked_block + 1);
    if (max_bytes_for_bloom_filter)
        bytes += DiskSetBlockFilters<Key>::estimateBytes(keys, BlockFormat::keys_per_unpacked_block, max_bytes_for_bloom_filter);
    return bytes;
}

template <typename Key>
TemporaryDataOnDiskScopePtr DiskSetImpl<Key>::createFileScope(const TemporaryDataOnDiskScopePtr & tmp_data)
{
    const auto & settings = tmp_data->getSettings();
    return tmp_data->childScope(settings.metrics, std::max(settings.buffer_size, BlockFormat::max_block_bytes_with_header), "NONE");
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
WriteBuffer & DiskSetImpl<Key>::startBlock(std::span<const Key> block_keys)
{
    /// The file writes into the memory of its compressed buffer but keeps its own position there, so only
    /// `next` hands the bytes of the previous block to the compressed buffer, which writes them as one frame.
    /// The stat of the file then counts that frame, so it gives the position of the frame of this block.
    file->next();
    directory.addBlock(block_keys.front(), file->getStat().compressed_size);
    if (filters)
        filters->addBlock(block_keys);
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

    directory.setFileSize(file->finishWriting().compressed_size);
    shared_file.emplace(*file);
    LOG_TRACE(
        getLogger("DiskSet"),
        "Created set on disk with {} keys of {} bytes, {} blocks, {} bytes of directory, {} bytes of filters with {:.2f} bits "
        "per key, {} bytes in temporary file {}",
        rows,
        sizeof(Key),
        directory.size(),
        directory.allocatedBytes(),
        filters ? filters->allocatedBytes() : 0,
        filters ? filters->getBitsPerKey() : 0.0,
        file->getStat().compressed_size,
        file->describeFilePath());
}

template <typename Key>
void DiskSetImpl<Key>::containsBatch(const IColumn & keys_column, std::span<UInt8> found) const
{
    const auto & keys = assert_cast<const ColumnVector<Key> &>(keys_column).getData();
    chassert(keys.size() == found.size());
    std::fill(found.begin(), found.end(), 0);
    ProfileEvents::increment(ProfileEvents::ExternalSetLookupKeys, keys.size());

    /// A set without keys has no blocks and finds nothing.
    if (!rows)
        return;

    /// Step 1: Collect the probes: the rows whose keys are in the range of the set, except a row that repeats
    /// the key of the previous row, which takes that row's answer in step 5. So a run of equal keys, as in a
    /// sorted column, takes one probe.
    struct Probe
    {
        Key key;
        size_t row;
    };

    VectorWithMemoryTracking<Probe> probes;
    probes.reserve(keys.size());
    const Key & first_key = directory.getFirstKey(0);
    for (size_t row = 0; row < keys.size(); ++row)
    {
        const auto & key = keys[row];
        if ((row == 0 || key != keys[row - 1]) && first_key <= key && key <= last_key)
            probes.push_back({key, row});
    }

    if (probes.empty()) /// No key is in the range of the set
        return;

    /// Step 2: Sort the probes by key. Probes of equal keys then come together, so each distinct key is
    /// looked up once, and the probes of each block come together in block order, so they share a read.
    /// Results go back to the original rows, so evaluating `IN` does not reorder the input.
    ::sort(probes.begin(), probes.end(), [](const Probe & lhs, const Probe & rhs) { return lhs.key < rhs.key; });

    /// Step 3: Find the block of each distinct key, and add a lookup in that block unless the Bloom filter of
    /// the block rules the key out. The keys ascend, so the search for each block starts at the block of the
    /// previous key. The reader gets the needed blocks, each once and in ascending order.
    struct Lookup
    {
        size_t block;

        /// The probes of the key are those from `probes_begin` up to `probes_end`.
        size_t probes_begin;
        size_t probes_end;
    };

    /// Returns the number of rows that a probe answers: its own row, and the rows after it that repeat its
    /// key and take its answer in step 5.
    const auto count_rows_of_probe = [&](const Probe & probe)
    {
        size_t end = probe.row + 1;
        while (end < keys.size() && keys[end] == probe.key)
            ++end;
        return end - probe.row;
    };

    VectorWithMemoryTracking<Lookup> lookups;
    std::vector<size_t> needed_blocks;
    size_t block = 0;
    size_t skipped_rows = 0;
    for (size_t begin = 0; begin < probes.size();)
    {
        size_t end = begin + 1;
        while (end < probes.size() && probes[end].key == probes[begin].key)
            ++end;

        block = directory.findBlock(probes[begin].key, block);
        if (!filters || filters->mayContain(block, probes[begin].key))
        {
            if (lookups.empty() || block != lookups.back().block)
                needed_blocks.push_back(block); /// Each block once, in ascending order
            lookups.push_back({block, begin, end});
        }
        else
        {
            for (size_t i = begin; i < end; ++i)
                skipped_rows += count_rows_of_probe(probes[i]);
        }
        begin = end;
    }

    /// A skipped key counts once for each of its rows, as `ExternalSetLookupKeys` does, so that the two
    /// events give the share of the rows that the filters answer.
    ProfileEvents::increment(ProfileEvents::ExternalSetBloomFilterSkippedKeys, skipped_rows);

    if (lookups.empty()) /// The filters ruled out every key
        return;

    /// Step 4: Search each key in its block. The reader produces the keys of the needed blocks in ascending
    /// order, from the cache or the file, and a key found in its block marks all its probes as found. Then
    /// the reader adds the blocks that it read to the cache.
    BlockReader blocks(*this, std::move(needed_blocks));
    for (const Lookup & lookup : lookups)
    {
        const auto block_keys = blocks.read(lookup.block);
        if (std::binary_search(block_keys.begin(), block_keys.end(), probes[lookup.probes_begin].key))
            for (size_t i = lookup.probes_begin; i < lookup.probes_end; ++i)
                found[probes[i].row] = 1;
    }
    blocks.addReadBlocksToCache(); /// The blocks that the reader read since its last batch

    /// Step 5: Rows that repeat the key of the previous row take its answer.
    for (size_t row = 1; row < keys.size(); ++row)
        if (keys[row] == keys[row - 1])
            found[row] = found[row - 1];
}

template <typename Key>
size_t estimateDiskSetMemory(size_t keys, size_t max_bytes_for_bloom_filter)
{
    return DiskSetImpl<Key>::estimateMemory(keys, max_bytes_for_bloom_filter);
}

template class DiskSetImpl<UInt8>;
template class DiskSetImpl<UInt16>;
template class DiskSetImpl<UInt32>;
template class DiskSetImpl<UInt64>;
template class DiskSetImpl<UInt128>;
template class DiskSetImpl<UInt256>;

template size_t estimateDiskSetMemory<UInt8>(size_t, size_t);
template size_t estimateDiskSetMemory<UInt16>(size_t, size_t);
template size_t estimateDiskSetMemory<UInt32>(size_t, size_t);
template size_t estimateDiskSetMemory<UInt64>(size_t, size_t);
template size_t estimateDiskSetMemory<UInt128>(size_t, size_t);
template size_t estimateDiskSetMemory<UInt256>(size_t, size_t);

}
