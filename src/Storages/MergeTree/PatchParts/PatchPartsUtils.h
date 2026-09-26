#pragma once

#include <Core/Block.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>

namespace DB
{

struct MergeTreePartition;
class ColumnLowCardinality;

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
using PartitionIdToMaxBlockPtr = std::shared_ptr<const PartitionIdToMaxBlock>;

/// Returns patches from patch_part required to be applied to source_part.
/// Returns at most one patch of type Merge and at most one patch of type Join.
PatchParts getPatchesForPart(const MergeTreePartInfo & source_part, const DataPartPtr & patch_part);

/// Returns metadata snapshot of a legacy (v1) patch part.
/// Sort key is `(_part, _part_offset)`.
StorageMetadataPtr getPatchPartMetadataV1(Block sample_block, ContextPtr local_context);
StorageMetadataPtr getPatchPartMetadataV1(ColumnsDescription patch_part_desc, ContextPtr local_context);

/// Returns metadata snapshot of a v2 patch part.
/// Sort key is `(<sorting_key>..., _block_number, _block_offset)`.
StorageMetadataPtr getPatchPartMetadataV2(Block sample_block, const KeyDescription & sorting_key, ContextPtr local_context);
StorageMetadataPtr getPatchPartMetadataV2(ColumnsDescription patch_part_desc, const KeyDescription & sorting_key, ContextPtr local_context);
StorageMetadataPtr getPatchPartMetadataV2(ColumnsDescription patch_part_desc, const String & sorting_key_str, ContextPtr local_context);

/// The effective sorting key for applying a v2 patch part is the longest common prefix
/// of the patch part's sorting key and the table's current sorting key, so it is fully
/// identified by its size. The first function returns that size, the second builds the key.
size_t getEffectivePatchSortingKeySize(const KeyDescription & patch_sorting_key, const StorageMetadataPtr & storage_metadata);
std::shared_ptr<const KeyDescription> getEffectivePatchSortingKey(size_t effective_key_size, const StorageMetadataPtr & storage_metadata);

const NamesAndTypesList & getPatchPartSystemColumnsV1();
const NamesAndTypesList & getPatchPartSystemColumnsV2();
const NamesAndTypesList & getAllPatchPartSystemColumns();
bool isPatchPartSystemColumn(const String & column_name);

/// Returns range of rows in part_name_column that equal part_name.
std::pair<UInt64, UInt64> getPartNameRange(const ColumnLowCardinality & part_name_column, const String & part_name);

std::pair<UInt64, UInt64> getPartNameOffsetRange(
    const ColumnLowCardinality & part_name_column,
    const PaddedPODArray<UInt64> & part_offset_data,
    const String & part_name,
    UInt64 part_offset_begin, UInt64 part_offset_end);

/// Returns virtual and sorting key columns that should be read from the regular part to apply the patch.
Names getKeyColumnsRequiredForPatch(const PatchPartInfoForReader & patch);

/// Returns the sorting key columns physically stored in the patch part (only v2 patches store them).
NameSet getSortingKeyColumnsInPatch(const StorageMetadataPtr & patch_metadata);

/// Partition id of patch part is 'patch-<hash of column names in patch part>-<original_partition_id>.
/// Functions below help to check and extract original_partition_id from partition id of patch part.
bool isPatchPartitionId(const String & partition_id);
bool isPatchForPartition(const MergeTreePartInfo & info, const String & partition_id);
String getOriginalPartitionIdOfPatch(const String & partition_id);
String getPartitionIdForPatch(const MergeTreePartition & partition);

/// Returns the hash of the patch structure from the partition id of a patch part.
String getStructureHashOfPatch(const String & partition_id);

/// Returns the hash of column names and types of a patch part.
String getColumnsHashWithTypes(const ColumnsDescription & columns_desc);

/// Returns true if patch max data version of the patch if higher than max_data_version.
/// Asserts that the patch's min and max data versions don't intersect max_data_version.
bool patchHasHigherDataVersion(const IMergeTreeDataPart & patch, Int64 max_data_version);
bool patchHasHigherDataVersion(const IMergeTreeDataPartInfoForReader & patch, Int64 max_data_version);

/// Returns maximal version among patches which version are in [current_data_version, next_mutation_version)
/// If there no such patches returns current_data_version.
PartsRange getPatchesToApplyOnMerge(const std::vector<MergeTreePartInfo> & patch_parts, const PartsRange & range, Int64 next_mutation_version);

/// Returns minimal block number with Update operation.
std::optional<Int64> getMinUpdateBlockNumber(const CommittingBlocksSet & committing_blocks);

using CommittingBlocks = std::unordered_map<String, CommittingBlocksSet>;
using PatchesByPartition = std::unordered_map<String, DataPartsVector>;
using PatchInfosByPartition = std::unordered_map<String, std::vector<MergeTreePartInfo>>;

/// Returns patches collected by original partition_id.
/// Functions with the second argument skip patches with lower data versions than provided in the second argument.
PatchesByPartition getPatchPartsByPartition(const DataPartsVector & patch_parts);
PatchesByPartition getPatchPartsByPartition(const DataPartsVector & patch_parts, const PartitionIdToMaxBlockPtr & partitions);

PatchInfosByPartition getPatchPartsByPartition(const std::vector<MergeTreePartInfo> & patch_parts, Int64 max_data_version);
PatchInfosByPartition getPatchPartsByPartition(const std::vector<MergeTreePartInfo> & patch_parts, const CommittingBlocks & committing_blocks);

/// Data versions of the regular parts collected by partition id. Versions are sorted and deduplicated.
using DataVersionsByPartition = std::unordered_map<String, std::vector<Int64>>;

/// Returns data versions of the regular (not patch) parts collected by partition id.
DataVersionsByPartition getDataVersionsByPartition(const DataPartsVector & parts);
DataVersionsByPartition getDataVersionsByPartition(const std::vector<MergeTreePartInfo> & parts);

/// Returns the data version of some regular part of the partition that lies between from and to, if there is one.
/// The bounds are unordered because merge order is independent of data-version order.
/// A merge of patch parts unions their ranges of data versions, and a patch part is applied to a part either
/// wholly or not at all. Therefore a merge whose result spans the data version of an existing part produces
/// a patch that can be neither applied nor skipped for that part. See 'patchHasHigherDataVersion'.
std::optional<Int64> findDataVersionInRange(const DataVersionsByPartition & data_versions, const String & partition_id, Int64 from, Int64 to);

}
