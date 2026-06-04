#pragma once

#include "config.h"

#include <Storages/MergeTree/UniqueKey/UniqueKeyProbe.h>

#include <base/types.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#if USE_ROCKSDB
namespace rocksdb
{
    class SstFileReader;
}
#endif

namespace DB
{

class IMergeTreeDataPart;
class DeleteBitmap;

/// Opened-reader handle for a part's `unique_key_index.sst` sidecar. Owns the
/// `SstFileReader` shared_ptr so the file descriptor outlives the open call.
/// `valid == false` means RocksDB is disabled in this build.
///
/// TODO: route handles through a reader cache so a part's reader is opened once
/// and shared across probes/queries instead of per use.
struct SSTReaderHandle
{
#if USE_ROCKSDB
    std::shared_ptr<rocksdb::SstFileReader> reader;
#endif
    bool valid = false;
};

/// Open an SST sidecar directly by local filesystem path. SST-only: knows
/// nothing about caching, parts, or the dense-index block cache. Returns an
/// throws (`CANNOT_OPEN_FILE`) if RocksDB cannot open the path.
///
/// TODO: a part-aware opener (resolve the sidecar path from the part + a
/// version-file gate) once parts carry an attached/loadable sidecar.
/// TODO: wire a dense-index block cache into the open options.
SSTReaderHandle openSSTReaderFromPath(const String & sst_path);

/// UNIQUE KEY `IProbeTargetPart` backed by an opened `unique_key_index.sst`.
///
/// Pure SST backend, no caching: the caller supplies an already-opened
/// `SSTReaderHandle`. `findRowIndexBatch` `Seek`s the SST for each
/// driver-encoded key (the embedded bloom filter short-circuits absent keys
/// inside RocksDB) and decodes the 4-byte big-endian row number. `isRowDead`
/// consults the pinned `DeleteBitmap`. The target does not encode — the driver
/// owns encoding and hands over encoded bytes.
///
/// TODO: a per-batch `MultiGet` path and `keyRangeIntersects` range-pruning can
/// land with the parallel/batched driver — the simple driver `Seek`s key-by-key.
class SSTProbeTargetPart : public IProbeTargetPart
{
public:
    /// `part_` may be nullptr (surfaced through `getUnderlyingPart`); the lookup
    /// itself needs only the reader handle.
    SSTProbeTargetPart(
        const IMergeTreeDataPart * part_,
        std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
        SSTReaderHandle handle_);

    ~SSTProbeTargetPart() override;

    void findRowIndexBatch(
        const std::vector<std::string_view> & encoded_keys,
        std::vector<std::optional<UInt64>> & out) const override;
    bool isRowDead(UInt64 row_number) const override;
    const IMergeTreeDataPart * getUnderlyingPart() const override { return part; }

    /// True if the handle holds a valid reader. Exposed for tests + callers
    /// that want to fail fast on a negative open.
    bool hasValidReader() const { return handle.valid; }

private:
    const IMergeTreeDataPart * part;
    std::shared_ptr<const DeleteBitmap> pinned_bitmap;
    SSTReaderHandle handle;

    /// Cached RocksDB iterator, lazily built once and reused across the keys of
    /// one `findRowIndexBatch` call (and across calls on the same target).
    struct Impl;
    mutable std::once_flag iter_inited;
    mutable std::unique_ptr<Impl> impl;
    void ensureIterInited() const;
};

}
