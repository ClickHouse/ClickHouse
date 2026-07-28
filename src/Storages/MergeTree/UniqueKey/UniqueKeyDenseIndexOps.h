#pragma once

#include "config.h"

#include <base/types.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <vector>


namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;
class IDataPartStorage;
struct StorageInMemoryMetadata;
struct DataPartsLock;

using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;


/// UNIQUE KEY dense-index operations for one storage. Groups the stateless
/// write path (INSERT-time write + load-time rebuild) with the per-storage
/// load lifecycle (orphan sweep + rebuild-on-load). One instance per
/// `MergeTreeData`; the write methods are static and hold no per-storage state.
class UniqueKeyDenseIndexOps
{
public:
    explicit UniqueKeyDenseIndexOps(MergeTreeData & data_) : data(data_) {}

    /// ===== Stateless write path (static) =====

    /// INSERT-path entry point. Times the write with the
    /// `UniqueKeySSTWriteMicroseconds` ProfileEvent. The caller
    /// (`MergeTreeDataWriter`) must check `hasUniqueKey()` before calling.
    static void writeDenseIndexOnInsert(
        IDataPartStorage & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const Block & block,
        const IColumn::Permutation * permutation,
        UInt64 max_encoded_size,
        ContextPtr context);

    /// ===== Per-storage load lifecycle (instance) =====

    /// Sweeps stray `unique_key_index.sst` (on non-UK tables) and
    /// `unique_key_index.sst.tmp` half-writes over every Active part.
    /// Caller holds the parts lock.
    void sweepOrphans(const DataPartsLock & part_lock);

    /// Materializes `unique_key_index.sst` when it is missing OR present but
    /// invalid (corrupt/truncated — the SST carries no checksums.txt entry, so
    /// presence alone is not trusted; the existing file is checksum-verified and
    /// removed+rebuilt if it fails). Defensive path for parts arriving without a
    /// usable sidecar (ATTACH / restore / fetch); fast-paths when a valid SST is
    /// already on disk. Fails closed: throws (CORRUPTED_DATA / SUPPORT_IS_DISABLED)
    /// when a non-empty UK part cannot get a dense index (missing UK column, empty
    /// read, rebuild error, or no RocksDB). The caller detaches the part as broken.
    ///
    /// With `storage_is_writable = false` (readonly startup) validation still runs
    /// — it is read-only I/O — but a missing/corrupt SST cannot be removed or
    /// rebuilt, so it throws UNIQUE_KEY_DENSE_INDEX_UNREADABLE (file left in
    /// place) and the caller fails the load instead of detaching.
    void ensureValidDenseIndex(MutableDataPartPtr & part, bool storage_is_writable) const;

    /// Per-part ATTACH hook: `.sst.tmp` cleanup + `ensureValidDenseIndex`.
    void onPartAttach(MutableDataPartPtr & part) const;

private:
    /// Gated on USE_ROCKSDB: its only caller branch (the rebuild body in
    /// `ensureValidDenseIndex`) is gated too, so without RocksDB this would be an
    /// unused private member function.
#if USE_ROCKSDB
    Block readUniqueKeyColumns(
        const MutableDataPartPtr & part,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & uk_names) const;
#endif

    MergeTreeData & data;
};

}
