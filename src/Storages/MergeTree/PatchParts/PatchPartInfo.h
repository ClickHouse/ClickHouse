#pragma once
#include <Core/Types.h>
#include <Core/Names.h>
#include <memory>
#include <vector>
#include <Core/MergeTreeSerializationEnums.h>

namespace DB
{

struct KeyDescription;

/** This directory contains classes and functions with implementation of patch parts.
  * Patch parts are created on lightweight updates (UPDATE queries, ALTER UPDATE
  * queries with `alter_update_mode` set to lightweight mode) and represent a patch
  * to the original part.
  *
  * There are two on-disk formats:
  *
  * Legacy format (v1, used when `patch_parts_version = 'v1'`):
  * patch parts contain only updated columns and several system columns:
  *  - _part - the name of the original part.
  *  - _part_offset - the row in the original part.
  *  - _block_number - the block number of row in the original part.
  *  - _block_offset - the block offset of row in the original part.
  *  - _data_version - the data version of the updated data (block number allocated for UPDATE query).
  * Sorted by `(_part, _part_offset)`. Applied via `PatchMode::Merge` or `PatchMode::Join`.
  *
  * New format (v2, used when `patch_parts_version = 'v2'`):
  * patch parts carry the main table's sort-key columns instead of `_part, _part_offset`:
  *  - <sorting_key_column_1>, ..., <sorting_key_column_n> - sort key columns of the target table.
  *  - _block_number - the block number of row in the original part.
  *  - _block_offset - the block offset of row in the original part.
  *  - _data_version - the data version of the updated data (block number allocated for UPDATE query).
  * Sorted by `(sorting_key_columns..., _block_number, _block_offset)`. Applied via `PatchMode::MergeOnKey`,
  * which streaming-merges the patch against the main part on the sort-key prefix and uses
  * `(_block_number, _block_offset)` to disambiguate rows within each equal-sort-key run.
  *
  * System columns help to find rows in original part which should be updated.
  * System columns are related to the virtual columns in the original part,
  * which are added for reading if patch parts should be applied.
  *
  * Patch parts have special index which helps to understand whether it is needed to apply patch part to the original part (see SourcePartsSetForPatch.h).
  *
  * Patch parts belong to the different partitions than the original part.
  * The partition id of the patch part is 'patch-<hash of column names in patch part>-<original_partition_id>'.
  * Therefore patch parts with different columns are stored in different partitions. For the v2 format the
  * hash additionally covers the sort-key column names and a v2 marker so v1 and v2 patches never collide
  * in the same partition (and therefore never enter the same patch-on-patch merge).
  * For example three updates "SET x = 1 WHERE <cond>" and "SET y = 1 WHERE <cond>" and "SET x = 1, y = 1 WHERE <cond>"
  * will create three patch parts in three different partition.
  *
  * Patch parts can be applied to the original parts to get data with changes stored in the patch parts on SELECTs and merges.
  * Patch parts are applied on merges (if table setting `apply_patches_on_merge` is enabled). After patch is applied on merge,
  * updated data is materialized and patch won't be applied on-fly anymore. Patch parts that are materialized in all active
  * parts are detected asynchronously and removed from the table.
  *
  * Patch parts can be merged among themselves. Merge of patch parts uses Replacing merge algorithm with _data_version as a version column.
  * Therefore patch part always stores the latest version for each updated row in part.
  *
  * Lightweight updates don't wait for currently running merges and mutations to finish (unlike heavy mutations) and always
  * use current snapshot of data parts to execute an update and produce a patch part. Because of that there can be two cases
  * of applying patch parts.
  *
  * For example if we read part A, we need to apply patch part X:
  *  1. if X contains part A itself. It happens if part A was not participating in merge when UPDATE was executed.
  *  2. if X contains part B and C, which are covered by part A. It happens if there was a merge (B, C) -> A running when UPDATE was executed.
  *
  * Legacy format handles these two cases with two separate modes (`Merge` by `_part_offset` for case 1, `Join`
  * by `(_block_number, _block_offset)` for case 2). `Join` mode is slow and memory-heavy because it requires
  * a hash table over the whole patch. The new format handles both cases uniformly with `MergeOnKey`: since
  * sort-key values and `(_block_number, _block_offset)` are both preserved across merges, the streaming
  * merge-by-sort-key + per-run disambiguation works regardless of whether the source and main part share
  * lineage.
  *
  * All modes use `_data_version` to leave rows with the latest version.
  */

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

class IMergeTreeDataPartInfoForReader;
using DataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

enum class PatchMode
{
    /// Legacy v1 format. Apply patch via merging by sorted columns (_part, _part_offset).
    Merge,
    /// Legacy v1 format. Apply patch via joining by key columns (_block_number, _block_offset).
    Join,
    /// New v2 format. Apply patch via streaming merge on the main table's sort-key columns.
    /// Within each equal-sort-key run rows are disambiguated with (_block_number, _block_offset).
    MergeOnKey,
};

template <typename TDataPartPtr>
struct PatchPartInfoBase
{
    PatchMode mode;
    TDataPartPtr part;
    Names source_parts;
    /// Data version of source part to which this patch is applied.
    Int64 source_data_version = 0;
    /// If true convert columns from patch to current data types in table metadata.
    bool perform_alter_conversions = true;

    /// Populated for `MergeOnKey` (v2) patches only, at `PatchPartInfo` construction time. Shares
    /// a single `KeyDescription` instance — built once per `SourcePartsSetForPatch` by slicing
    /// the target table's sort-key AST to the persisted prefix length (see
    /// `SourcePartsSetForPatch::getSortingKeyPrefixDescription`). Nullptr for `Merge`/`Join`
    /// (v1 patches). The shared_ptr is copied into each `PatchPartInfo` that needs it so
    /// downstream consumers (`applyPatchMergeOnKey`, `MergeTreeReadersChain`,
    /// `getVirtualsRequiredForPatch`, `MergeTreePatchReaderMergeOnKey`) read sort-key metadata
    /// directly off the `KeyDescription` without rebuilding anything per application.
    ///
    /// Shape of the stored `KeyDescription`: the semantic sort-key prefix *only* — it does NOT
    /// include the trailing `_block_number` / `_block_offset` identity columns that appear in
    /// the patch part's own metadata. Those are handled separately by the apply path.
    std::shared_ptr<const KeyDescription> sorting_key;

    String describe() const;
};

using PatchPartInfo = PatchPartInfoBase<DataPartPtr>;
using PatchPartInfoForReader = PatchPartInfoBase<DataPartInfoForReaderPtr>;

using PatchParts = std::vector<PatchPartInfo>;
using PatchPartsForReader = std::vector<PatchPartInfoForReader>;


struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct PatchPartMetadata
{
    MergeTreePatchPartsVersion version;
    StorageMetadataPtr metadata;
    std::optional<UInt64> sorting_key_prefix_size;
};

}
