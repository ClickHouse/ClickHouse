#pragma once

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


/// Dense-index operations for one storage: the stateless write path (INSERT-time write plus
/// load-time rebuild) and the per-storage load lifecycle (orphan sweep, rebuild-on-load). One
/// instance per `MergeTreeData`; the write methods are static and hold no per-storage state.
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

    /// Read `uk_names` for every physical row of `part` in part-offset order
    /// (`apply_deleted_mask=false`, sequential source), so block row `i` == part
    /// offset `i`. Shared by the load-time rebuild and the merge late-kill path.
    static Block readUniqueKeyColumns(
        const MergeTreeData & data,
        const std::shared_ptr<const IMergeTreeDataPart> & part,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & uk_names);

    /// ===== Per-storage (instance): the load lifecycle, and the merge write =====

    /// Sweeps stray `unique_key_index.sst` (on non-UK tables) and
    /// `unique_key_index.sst.tmp` half-writes over every Active part.
    /// Caller holds the parts lock.
    void sweepOrphans(const DataPartsLock & part_lock);

    /// Materialize `unique_key_index.sst` when it is missing or fails its checksum. The SST has
    /// no `checksums.txt` entry, so presence alone is not trusted. Fails closed: a non-empty part
    /// that cannot get a dense index throws and the caller detaches it as broken.
    ///
    /// Under `storage_is_writable = false` validation still runs, but nothing can be rebuilt, so
    /// it throws `UNIQUE_KEY_DENSE_INDEX_UNREADABLE` and the caller fails the load.
    void ensureValidDenseIndex(MutableDataPartPtr & part, bool storage_is_writable) const;

    /// Per-part ATTACH hook: `.sst.tmp` cleanup + `ensureValidDenseIndex`.
    void onPartAttach(MutableDataPartPtr & part) const;

    /// MERGE-path entry point: writes the finalized merged part's `unique_key_index.sst`
    /// from `unique_key_columns`, the UK columns the merge retained from its own output
    /// (row i == part offset i). No-op on an empty part, fails closed otherwise.
    void writeDenseIndexOnMerge(
        MutableDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot, const Block & unique_key_columns) const;

private:
    MergeTreeData & data;
};

}
