#pragma once

#include "config.h"

#include <base/types.h>
#include <Core/Block.h>
#include <Core/Names.h>

#include <memory>
#include <vector>


namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;
struct StorageInMemoryMetadata;

using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;


/// UNIQUE KEY dense-index operations for one storage: the per-storage load
/// lifecycle (rebuild-on-load). One instance per `MergeTreeData`.
/// The stateless write path lives in `SSTIndexWriter`.
class UniqueKeyDenseIndexOps
{
public:
    explicit UniqueKeyDenseIndexOps(MergeTreeData & data_) : data(data_) {}

    /// ===== Per-storage load lifecycle (instance) =====

    /// Materializes `unique_key_index.sst` when it is missing OR present but
    /// invalid. Presence alone is not trusted: the existing file is validated
    /// against the RocksDB block checksums and the part's row count, and
    /// removed+rebuilt if it fails.
    ///
    /// The SST is recorded in `checksums.txt`, so a missing or wrongly-sized file
    /// is rejected earlier by `checkConsistencyBase` and the part is detached as
    /// broken. What reaches this function is (a) size-preserving damage, and
    /// (b) parts that legitimately carry no SST entry (UNIQUE KEY added by
    /// ALTER, or a restored backup without the sidecar).
    ///
    /// Fails closed: throws (CORRUPTED_DATA / SUPPORT_IS_DISABLED) when a
    /// non-empty UK part cannot get a dense index (missing UK column, empty read,
    /// rebuild error, or no RocksDB). The caller detaches the part as broken.
    ///
    /// With `storage_is_writable = false` (readonly startup) validation still runs
    /// - it is read-only I/O - but a corrupt or absent SST cannot be removed or
    /// rebuilt, so it throws UNIQUE_KEY_DENSE_INDEX_UNREADABLE (file left in
    /// place) and the caller fails the load instead of detaching.
    void ensureValidDenseIndex(MutableDataPartPtr & part, bool storage_is_writable) const;

    /// Per-part ATTACH hook: `ensureValidDenseIndex`.
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
