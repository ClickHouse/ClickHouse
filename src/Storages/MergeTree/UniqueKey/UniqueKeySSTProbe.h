#pragma once

#include "config.h"

#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>

#include <base/types.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/// This whole translation unit is RocksDB-only: the SST backend is unavailable
/// without RocksDB, and its only caller (the probe factory / gtests) is guarded.
#if USE_ROCKSDB

#include <rocksdb/status.h>

namespace rocksdb
{
    class SstFileReader;
}

namespace DB
{

class IMergeTreeDataPart;
class DeleteBitmap;

/// Opened-reader handle for a part's `unique_key_index.sst` sidecar. Owns the
/// `SstFileReader` shared_ptr so the file descriptor outlives the open call.
/// A null `reader` denotes an invalid handle (a negative open).
///
/// TODO(unique-key): route handles through a reader cache (open once, share across probes).
struct SSTReaderHandle
{
    std::shared_ptr<rocksdb::SstFileReader> reader;
};

/// Result of a non-throwing open: the reader plus the raw RocksDB `Status`, so
/// callers can distinguish a corruption from a transient failure themselves.
/// `reader` is null when `status` is not ok.
struct SSTOpenResult
{
    std::shared_ptr<rocksdb::SstFileReader> reader;
    rocksdb::Status status;
};

/// Single source of truth for the SST read-open options (bloom policy + table
/// factory that must match what `SSTIndexWriter` produced). Opens the sidecar and
/// returns the reader + status without throwing.
SSTOpenResult tryOpenSSTReaderFromPath(const String & sst_path);

/// Open an SST sidecar directly by local filesystem path. SST-only: knows
/// nothing about caching, parts, or the dense-index block cache. Throws
/// `CANNOT_OPEN_FILE` if RocksDB cannot open the path.
///
/// TODO(unique-key): part-aware opener + dense-index block cache in the open options.
SSTReaderHandle openSSTReaderFromPath(const String & sst_path);

/// UNIQUE KEY `IProbeTargetPart` backed by an opened `unique_key_index.sst`.
/// The driver owns encoding and hands over encoded bytes; the SST's embedded
/// bloom filter short-circuits absent keys inside RocksDB.
///
/// TODO(unique-key): per-batch `MultiGet` + `keyRangeIntersects` range-pruning with the parallel driver.
class SSTProbeTargetPart : public IProbeTargetPart
{
public:
    /// `part_` may be nullptr (surfaced through `getUnderlyingPart`); the lookup
    /// itself needs only the reader handle.
    SSTProbeTargetPart(
        const IMergeTreeDataPart * part_,
        std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
        SSTReaderHandle handle_);

    void findRowIndexBatch(
        const std::vector<std::string_view> & encoded_keys,
        std::vector<std::optional<UInt64>> & out) const override;
    bool isRowDead(UInt64 row_number) const override;
    const IMergeTreeDataPart * getUnderlyingPart() const override { return part; }

private:
    const IMergeTreeDataPart * part;
    std::shared_ptr<const DeleteBitmap> pinned_bitmap;
    SSTReaderHandle handle;
};

}

#endif
