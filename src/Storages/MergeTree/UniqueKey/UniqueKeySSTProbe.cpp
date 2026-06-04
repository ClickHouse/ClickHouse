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
    extern const int CANNOT_OPEN_FILE;
    extern const int ROCKSDB_ERROR;
    extern const int CORRUPTED_DATA;
}

#if USE_ROCKSDB
namespace
{
    /// Decode the 4-byte big-endian row number written by `SSTIndexWriter`
    /// (`encodeRowNumberBE`), widened to UInt64 (== `_part_offset`). The writer
    /// always emits exactly 4 bytes; any other size is a corrupt or
    /// incompatible sidecar, so fail closed rather than decode a prefix.
    UInt64 decodeRowNumberBE(const char * data, size_t size)
    {
        if (size != sizeof(UInt32))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "UNIQUE KEY SST value has {} bytes, expected exactly 4", size);
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

SSTProbeTargetPart::SSTProbeTargetPart(
    const IMergeTreeDataPart * part_,
    std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
    SSTReaderHandle handle_)
    : part(part_)
    , pinned_bitmap(std::move(pinned_bitmap_))
    , handle(std::move(handle_))
{
}

void SSTProbeTargetPart::findRowIndexBatch(
    const std::vector<std::string_view> & encoded_keys,
    std::vector<std::optional<UInt64>> & out) const
{
    out.assign(encoded_keys.size(), std::nullopt);

    /// Fail closed: a target that cannot read its index must not report misses.
    /// `NOT_FOUND` must mean "no active part holds the key", never "could not
    /// read this part" — a silent miss here could let a duplicate key through
    /// once INSERT enforcement is wired. (A catchable error, not a
    /// `LOGICAL_ERROR` abort: it surfaces an unreadable index, and on a build
    /// without RocksDB the handle is always invalid.)
    if (!handle.valid)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "UNIQUE KEY SST probe target has no readable index (invalid reader handle)");

#if USE_ROCKSDB
    /// A fresh iterator per call: `SSTProbeTargetPart` is shared as a
    /// `shared_ptr<const>`, so a cached/mutable iterator would race across
    /// concurrent probes on the same target. A per-call iterator keeps the
    /// target thread-safe by construction (the perf layer can revisit this).
    /// The SST's embedded bloom filter short-circuits absent keys inside
    /// RocksDB; `Seek` lands at >= key, so the exact-equality compare is required.
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(handle.reader->NewIterator(read_opts));

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
