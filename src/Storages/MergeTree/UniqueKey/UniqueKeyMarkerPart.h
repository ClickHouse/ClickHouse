#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <base/scope_guard.h>

#include <memory>
#include <utility>

namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;
struct MergeTreePartition;
using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

}

namespace DB::UniqueKeyTxn
{

/// Build a UNIQUE KEY DELETE marker part — a 0-row part named
/// `all_<block_number>_<block_number>_0/` that carries a `unique_key.txt`
/// manifest with `is_marker: true` and the supplied `bitmaps_created`
/// `(target, csn)` entries.
///
/// Reuses `MergeTreeData::createEmptyPart` (the same primitive behind stock
/// 0-row covering parts) verbatim and only adds the manifest write.
///
/// Returned handle:
///   - `data_part` is a tmp-state MutableDataPart still under
///     `tmp_empty_<name>/`. The caller (commit driver) does the publishing rename.
///   - `tmp_dir_holder` is the scope_guard that cleans up the tmp directory
///     if the caller drops the handle without publishing.
///
/// CSN placement: callers staging a marker for the publish lock pass
/// `creation_csn = INVALID_CSN`; the commit driver rewrites the manifest
/// (creation_csn + `bitmaps_created` csns) with the real csn before the rename.
struct MarkerPartHandle
{
    MutableDataPartPtr data_part;
    scope_guard        tmp_dir_holder;
};

/// `partition_id` is captured implicitly via `partition`; the marker's
/// `MergeTreePartInfo` is `(partition_id, block_number, block_number, level=0)`.
/// `bitmaps_created` is the manifest payload (passed through `UniqueKeyManifest`).
MarkerPartHandle createMarkerPart(
    MergeTreeData & data,
    const String & partition_id,
    Int64 block_number,
    const MergeTreePartition & partition,
    const UniqueKeyManifest & meta);

}
