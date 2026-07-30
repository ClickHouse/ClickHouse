#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>

#include "config.h"

/// This whole translation unit is RocksDB-only (see the header): the SST backend
/// is unavailable without RocksDB, and its only caller is guarded.
#if USE_ROCKSDB

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <Common/Exception.h>

#include <base/unaligned.h>

#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int ROCKSDB_ERROR;
    extern const int CORRUPTED_DATA;
}

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

SSTOpenResult tryOpenSSTReaderFromPath(const String & sst_path)
{
    /// The one place the read-open options live: bloom policy + table factory
    /// must match what `SSTIndexWriter` wrote.
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions block_based;
    block_based.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based));

    auto reader = std::make_shared<rocksdb::SstFileReader>(options);
    auto status = reader->Open(sst_path);
    if (!status.ok())
        return {nullptr, std::move(status)};
    return {std::move(reader), std::move(status)};
}

SSTReaderHandle openSSTReaderFromPath(const String & sst_path)
{
    auto opened = tryOpenSSTReaderFromPath(sst_path);
    if (!opened.status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open UNIQUE KEY SST `{}`: {}", sst_path, opened.status.ToString());

    return SSTReaderHandle{std::move(opened.reader)};
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

    /// Fail closed: `NOT_FOUND` must mean "no active part holds the key", never
    /// "could not read this part" — a silent miss could let a duplicate through.
    if (!handle.reader)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "UNIQUE KEY SST probe target has no readable index (invalid reader handle)");

    /// Fresh iterator per call: the target is shared as `shared_ptr<const>`, so a
    /// cached iterator would race across concurrent probes. `Seek` lands at >= key,
    /// so the exact-equality compare is required.
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(handle.reader->NewIterator(read_opts));

    for (size_t i = 0; i < encoded_keys.size(); ++i)
    {
        rocksdb::Slice key_slice(encoded_keys[i].data(), encoded_keys[i].size());
        it->Seek(key_slice);
        if (it->Valid() && it->key().compare(key_slice) == 0)
        {
            auto value_slice = it->value();
            const UInt64 row_number = decodeRowNumberBE(value_slice.data(), value_slice.size());
            /// `row_number` is used directly as `_part_offset`; an out-of-range
            /// offset (corrupt/incompatible SST) would read past the part, so fail
            /// closed. `part` is null only in isolation unit tests.
            /// TODO(unique-key): the production probe-target factory must pass a
            /// non-null part so this bound is enforced once INSERT probe is wired.
            if (part && row_number >= part->rows_count)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "UNIQUE KEY SST points at row {} but part '{}' has only {} rows",
                    row_number, part->name, part->rows_count);
            out[i] = row_number;
        }
        else if (!it->status().ok())
        {
            /// A genuine miss leaves the iterator OK; a non-OK status is an SST read
            /// error and must not be reported as "not found".
            throw Exception(ErrorCodes::ROCKSDB_ERROR,
                "SSTProbeTargetPart: error seeking UNIQUE KEY SST: {}", it->status().ToString());
        }
    }
}

bool SSTProbeTargetPart::isRowDead(UInt64 row_number) const
{
    return pinned_bitmap && pinned_bitmap->contains(row_number);
}

}

#endif
