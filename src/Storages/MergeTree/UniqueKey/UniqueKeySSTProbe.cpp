#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <Common/Exception.h>

#include <base/unaligned.h>

#include "config.h"

#if USE_ROCKSDB
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int ROCKSDB_ERROR;
}

#if USE_ROCKSDB
namespace
{
    /// Decode the 4-byte big-endian row number written by `SSTIndexWriter`
    /// (`encodeRowNumberBE`), widened to UInt64 (== `_part_offset`).
    UInt64 decodeRowNumberBE(const char * data, size_t size)
    {
        if (size < sizeof(UInt32))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "SSTProbeTargetPart: SST value has {} bytes, expected >= 4", size);
        return unalignedLoadBigEndian<UInt32>(data);
    }
}
#endif

SSTReaderHandle openSSTReaderFromPath(const String & sst_path)
{
    SSTReaderHandle out;
    out.valid = false;

#if USE_ROCKSDB
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions block_based;
    block_based.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based));

    auto reader = std::make_shared<rocksdb::SstFileReader>(options);
    auto status = reader->Open(sst_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open UNIQUE KEY SST `{}`: {}", sst_path, status.ToString());

    out.reader = std::move(reader);
    out.valid = true;
#else
    (void)sst_path;
#endif

    return out;
}

#if USE_ROCKSDB
struct SSTProbeTargetPart::Impl
{
    std::unique_ptr<rocksdb::Iterator> cached_iter;
};
#else
struct SSTProbeTargetPart::Impl
{
};
#endif

SSTProbeTargetPart::SSTProbeTargetPart(
    const IMergeTreeDataPart * part_,
    std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
    SSTReaderHandle handle_)
    : part(part_)
    , pinned_bitmap(std::move(pinned_bitmap_))
    , handle(std::move(handle_))
    , impl(std::make_unique<Impl>())
{
}

SSTProbeTargetPart::~SSTProbeTargetPart() = default;

void SSTProbeTargetPart::ensureIterInited() const
{
#if USE_ROCKSDB
    std::call_once(iter_inited, [this]()
    {
        if (!handle.valid || !handle.reader)
            return;
        rocksdb::ReadOptions read_opts;
        impl->cached_iter.reset(handle.reader->NewIterator(read_opts));
    });
#endif
}

void SSTProbeTargetPart::findRowIndexBatch(
    const std::vector<std::string_view> & encoded_keys,
    std::vector<std::optional<UInt64>> & out) const
{
    out.assign(encoded_keys.size(), std::nullopt);

#if USE_ROCKSDB
    if (!handle.valid || !handle.reader)
        return;

    ensureIterInited();
    auto * it = impl->cached_iter.get();
    if (!it)
        return;

    /// Reused, re-seekable iterator. The SST's embedded bloom filter
    /// short-circuits absent keys inside RocksDB. `Seek` lands at >= key, so
    /// the exact-equality compare is required.
    for (size_t i = 0; i < encoded_keys.size(); ++i)
    {
        rocksdb::Slice key_slice(encoded_keys[i].data(), encoded_keys[i].size());
        it->Seek(key_slice);
        if (it->Valid() && it->key().compare(key_slice) == 0)
        {
            auto value_slice = it->value();
            out[i] = decodeRowNumberBE(value_slice.data(), value_slice.size());
        }
        else if (!it->status().ok())
        {
            /// A genuine miss leaves the iterator OK (positioned past the key or
            /// off the end); a non-OK status is an SST read error and must not be
            /// silently reported as "not found" — that could let a duplicate key
            /// through. Mirror `StorageEmbeddedRocksDB`'s throw-on-non-OK.
            throw Exception(ErrorCodes::ROCKSDB_ERROR,
                "SSTProbeTargetPart: error seeking UNIQUE KEY SST: {}", it->status().ToString());
        }
    }
#endif
}

bool SSTProbeTargetPart::isRowDead(UInt64 row_number) const
{
    return pinned_bitmap && pinned_bitmap->contains(row_number);
}

}
