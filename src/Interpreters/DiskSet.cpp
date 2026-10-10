#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressionFactory.h>
#include <Interpreters/DiskSetBlockReader.h>
#include <base/sort.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <functional>

namespace DB
{

template <typename Key>
DiskSetImpl<Key>::DiskSetImpl(
    TemporaryDataOnDiskScopePtr tmp_data, size_t min_free_disk_space, size_t expected_keys, size_t max_bytes_for_bloom_filter)
    : file(std::make_unique<TemporaryDataBuffer>(std::move(tmp_data), min_free_disk_space))
    , encoder(std::in_place)
    , compressible_block_codec(file->getCompressedWriteBuffer().getCodec())
    , incompressible_block_codec(CompressionCodecFactory::instance().get("NONE"))
{
    if (max_bytes_for_bloom_filter)
        filters = DiskSetBlockFilters<Key>::create(expected_keys, BlockFormat::keys_per_unpacked_block, max_bytes_for_bloom_filter);
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
WriteBuffer & DiskSetImpl<Key>::startBlock(std::span<const Key> block_keys, bool compressible)
{
    /// `next` must end the frame of the previous block before `setCodec` picks the codec of this one. The
    /// file writes into the memory of its compressed buffer but keeps its own position there, so only `next`
    /// hands the bytes of the previous block to the compressed buffer.
    file->next();
    auto & compressed = file->getCompressedWriteBuffer();
    compressed.setCodec(compressible ? compressible_block_codec : incompressible_block_codec);
    directory.addBlock(block_keys.front(), compressed.getCompressedBytes());
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

    file->finishWriting();
    LOG_TRACE(
        getLogger("DiskSet"),
        "Created set on disk with {} keys of {} bytes, {} blocks, {} bytes of directory, {} bytes of filters, {} bytes in "
        "temporary file {}",
        rows,
        sizeof(Key),
        directory.size(),
        getTotalByteCount(),
        filters ? filters->allocatedBytes() : 0,
        file->getStat().compressed_size,
        file->describeFilePath());
}

template <typename Key>
void DiskSetImpl<Key>::containsBatch(const IColumn & keys_column, std::span<UInt8> found) const
{
    const auto & keys = assert_cast<const ColumnVector<Key> &>(keys_column).getData();
    chassert(keys.size() == found.size());
    std::fill(found.begin(), found.end(), 0);

    /// A set without keys has no blocks and finds nothing.
    if (!rows)
        return;

    /// The rows with keys in the range of the set are probed, except a row with the key of the previous row,
    /// which takes the answer of that row at the end. So a run of equal keys, as in a sorted column, takes
    /// one probe.
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

    if (probes.empty())
        return;

    /// Sorted by key, the probes of equal keys come together, so that each distinct key is looked up once,
    /// and the probes of each block come together in the order of the blocks, so that they share a read.
    /// Results are scattered back to their original rows, so evaluating `IN` does not reorder the input.
    ::sort(probes.begin(), probes.end(), [](const Probe & lhs, const Probe & rhs) { return lhs.key < rhs.key; });

    /// Each distinct key gets its block, and a lookup in that block unless the filter of the block rules the
    /// key out. The keys ascend, so the search for each block starts at the block of the previous key.
    struct Lookup
    {
        size_t block;

        /// The probes of the key are those from `probes_begin` up to `probes_end`.
        size_t probes_begin;
        size_t probes_end;
    };

    VectorWithMemoryTracking<Lookup> lookups;
    size_t needed_blocks = 0;
    size_t block = 0;
    for (size_t begin = 0; begin < probes.size();)
    {
        size_t end = begin + 1;
        while (end < probes.size() && probes[end].key == probes[begin].key)
            ++end;

        block = directory.findBlock(probes[begin].key, block);
        if (!filters || filters->mayContain(block, probes[begin].key))
        {
            needed_blocks += lookups.empty() || block != lookups.back().block;
            lookups.push_back({block, begin, end});
        }
        begin = end;
    }

    if (lookups.empty())
        return;

    /// The reader sizes its reads from how many blocks the probes need and how far apart they are.
    BlockReader blocks(*this, needed_blocks, /*range_blocks=*/lookups.back().block - lookups.front().block + 1);
    for (const Lookup & lookup : lookups)
    {
        const auto block_keys = blocks.read(lookup.block);
        if (std::binary_search(block_keys.begin(), block_keys.end(), probes[lookup.probes_begin].key))
            for (size_t i = lookup.probes_begin; i < lookup.probes_end; ++i)
                found[probes[i].row] = 1;
    }

    /// The rows that repeat the key of the previous row take its answer.
    for (size_t row = 1; row < keys.size(); ++row)
        if (keys[row] == keys[row - 1])
            found[row] = found[row - 1];
}

template class DiskSetImpl<UInt8>;
template class DiskSetImpl<UInt16>;
template class DiskSetImpl<UInt32>;
template class DiskSetImpl<UInt64>;
template class DiskSetImpl<UInt128>;
template class DiskSetImpl<UInt256>;

}
