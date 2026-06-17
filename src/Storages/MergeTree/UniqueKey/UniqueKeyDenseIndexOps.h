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

    /// Writes the dense index (`unique_key_index.sst`) from `block`. Dispatches
    /// to the sorted writer when `uk_names` is a non-Nullable prefix of
    /// `sort_names`, the unsorted writer otherwise. Returns 0 (no-op) when
    /// `uk_names` is empty.
    static UInt64 writeDenseIndex(
        IDataPartStorage & storage,
        const Block & block,
        const Names & uk_names,
        const Names & sort_names,
        const std::vector<bool> & sort_reverse_flags,
        const IColumn::Permutation * permutation,
        UInt64 max_encoded_size,
        ContextPtr context);

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

    /// Materializes `unique_key_index.sst` if missing. Defensive path
    /// for parts arriving without a sidecar (ATTACH / restore / fetch);
    /// fast-paths when the SST is already on disk. Fails closed: throws
    /// (CORRUPTED_DATA / SUPPORT_IS_DISABLED) when a non-empty UK part cannot
    /// get a dense index (missing UK column, empty read, rebuild error, or no
    /// RocksDB). The caller is responsible for detaching the part as broken.
    void rebuildIfMissing(MutableDataPartPtr & part) const;

    /// Per-part ATTACH hook: `.sst.tmp` cleanup + `rebuildIfMissing`.
    void onPartAttach(MutableDataPartPtr & part) const;

private:
    /// Gated on USE_ROCKSDB: its only caller branch (the rebuild body in
    /// `rebuildIfMissing`) is gated too, so without RocksDB this would be an
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
