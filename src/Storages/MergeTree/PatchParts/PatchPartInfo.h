#pragma once
#include <Core/Types.h>
#include <Core/Names.h>

namespace DB
{

/** This directory contains classes functions with implementation of patch parts.
  * Patch parts are created on lightweight updates (UPDATE queies, ALTER UPDATE
  * queries with `alter_update_mode` set to lightweight mode) and represent a patch
  * to the original part.
  *
  * Patch parts are the same as the regular parts, but contain only updated columns and several system columns:
  *  - _part - the name of the original part.
  *  - _part_offset - the row in the original part.
  *  - _block_number - the block number of row in the original part.
  *  - _block_offset - the block offset of row in the original part.
  *  - _data_version - the data version of the updated data (block number allocated for UPDATE query).
  *
  * System columns help to find rows in original part which should be updated.
  * System columns are related to the virtual columns in the original part,
  * which are added for reading if patch parts should be applied.
  *
  * Patch parts are sorted by _part and _part_offset.
  *
  * Patch parts have special index which helps to understand whether it is needed to apply patch part to the original part (see SourcePartsSetForPatch.h).
  *
  * Patch parts belong to the different partitions than the original part.
  * The partition id of the patch part is 'patch-<hash of column names in patch part>-<original_partition_id>'.
  * Therefore patch parts with different columns are stored in different partitions.
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
  * For these two cases there are two ways to apply patch parts respectively:
  *  1. using merge by sorted columns (_part, _part_offset).
  *  2. using join by (_block_number, _block_offset) columns.
  *
  * In the second case we cannot use Merge mode because rows are rearranged by merge of data parts.
  * But we can can use Join mode because _block_number and _block_offset are persisted in table.
  * A pair of (_block_number, _block_offset) creates an unique ordered identifier of row in table.
  * The Join mode is slower and requires more memory than Merge mode, but the second case should happen rarely.
  * Both modes of applying patches use _data_version to leave rows with the latest version.
  */

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

class IMergeTreeDataPartInfoForReader;
using DataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

enum class PatchMode
{
    /// Apply patch via merging by sorted columns (_part, _part_offset).
    Merge,
    /// Apply patch via joining by key columns (_block_number, _block_offset).
    Join,
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

    String describe() const;
};

using PatchPartInfo = PatchPartInfoBase<DataPartPtr>;
using PatchPartInfoForReader = PatchPartInfoBase<DataPartInfoForReaderPtr>;

using PatchParts = std::vector<PatchPartInfo>;
using PatchPartsForReader = std::vector<PatchPartInfoForReader>;

}
