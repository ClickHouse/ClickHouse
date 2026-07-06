#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Core/Block.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct KeyDescription;

/** A helper index of source parts for which updated data is stored in the patch part.
  * It is used to get patches for the regular parts.
  */
class SourcePartsSetForPatch
{
public:
    /// On-disk format version of the patch part.
    ///  0 = legacy v1 (sorted by `_part, _part_offset`, applied with `Merge` or `Join` mode).
    ///  1 = v2 (sorted by `sorting_key..., _block_number, _block_offset`, applied with `MergeOnKey`).
    static constexpr UInt8 V1_FORMAT_VERSION = 0;
    static constexpr UInt8 V2_FORMAT_VERSION = 1;
    static constexpr UInt8 MAX_SUPPORTED_FORMAT_VERSION = V2_FORMAT_VERSION;

    static constexpr auto FILENAME = "source_parts.dat";

    SourcePartsSetForPatch() = default;
    SourcePartsSetForPatch(UInt8 format_version_, Names sorting_key_columns_);

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    UInt8 getFormatVersion() const { return format_version; }

    /// Columns of the table's sorting key the v2 patch was written with, as one-line
    /// formatted texts with DESC modifiers (see `getSortingKeyColumnsForPatch`), excluding
    /// the trailing `_block_number`, `_block_offset` identity columns. Empty for v1 patches.
    const Names & getSortingKeyColumns() const { return sorting_key_columns; }

    /// Returns a set with the same format version and sort-key columns but without source
    /// parts. Used for empty covering parts in patch partitions, which patch nothing but
    /// must keep the structure of the partition.
    SourcePartsSetForPatch cloneEmpty() const { return SourcePartsSetForPatch(format_version, sorting_key_columns); }

    void addSourcePart(const String & name, UInt64 data_version);

    /// `sorting_key` is the effective sort-key prefix for `MergeOnKey` patches
    /// (see `buildPatchSortingKeyDescription`), unused (nullptr) for v1 patches.
    PatchParts getPatchParts(
        const MergeTreePartInfo & original_part,
        const DataPartPtr & patch_part,
        std::shared_ptr<const KeyDescription> sorting_key) const;

    static SourcePartsSetForPatch build(
        const Block & block,
        UInt64 data_version,
        UInt8 format_version,
        Names sorting_key_columns);

    /// Merge patch-on-patch sets. The input parts share the same partition, so their
    /// `format_version` and `sorting_key_columns` are guaranteed equal (both are covered
    /// by the partition-id hash); we just copy them from the first part.
    static SourcePartsSetForPatch merge(const DataPartsVector & source_parts);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    void buildSourcePartsSet();

    /// Max data version -> part set that contains all parts from min_max_versions_by_part with this max data version.
    /// Can be reconstructed from source_parts_by_version.
    std::map<UInt64, ActiveDataPartSet> source_parts_by_version;

    /// Part name -> min and max version of updated data stored in patch part for the source part.
    /// Serialized to the file on disk.
    std::map<String, std::pair<UInt64, UInt64>> min_max_versions_by_part;

    UInt64 min_data_version = 0;
    UInt64 max_data_version = 0;

    /// Format version of the patch part on disk (see `V1_FORMAT_VERSION` / `V2_FORMAT_VERSION`).
    UInt8 format_version = V1_FORMAT_VERSION;

    /// Sort-key columns the v2 patch was written with, persisted in `source_parts.dat` right
    /// after `format_version`. Empty and unused for v1 patches.
    Names sorting_key_columns;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
SourcePartsSetForPatch buildSourceSetForPatch(
    Block & block,
    UInt64 data_version,
    const PatchPartMetadata & patch_metadata);

}
