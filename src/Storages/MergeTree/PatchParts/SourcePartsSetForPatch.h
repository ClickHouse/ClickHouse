#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Core/Block.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

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

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    UInt8 getFormatVersion() const { return format_version; }
    void setFormatVersion(UInt8 new_format_version) { format_version = new_format_version; }

    /// Number of semantic sort-key columns in the v2 patch's sort key — i.e. the length of the
    /// sort-key prefix, excluding the two trailing identity columns `_block_number` and
    /// `_block_offset`. Captured from the target table's sort key at write time and persisted
    /// alongside the format-version byte; zero for v1 patches.
    UInt64 getSortKeyPrefixSize() const { return sorting_key_prefix_size; }
    void setSortKeyPrefixSize(UInt64 size) { sorting_key_prefix_size = size; }

    void addSourcePart(const String & name, UInt64 data_version);
    PatchParts getPatchParts(const MergeTreePartInfo & original_part, const DataPartPtr & patch_part) const;

    static SourcePartsSetForPatch build(const Block & block, UInt64 data_version);
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

    /// Format version of the patch part on disk. Populated on read from the version byte;
    /// set explicitly by the sink before write. See `V1_FORMAT_VERSION` / `V2_FORMAT_VERSION`.
    /// Only the prefix *length* is persisted (`sorting_key_prefix_size` below), not the sort-key
    /// AST itself; v2 readers rebuild the AST from the target table's current
    /// `StorageMetadataPtr` and slice it to that length (see `MergeTreeData::getPatchPartMetadata`
    /// and `MergeTreeData::getAlterConversionsForPart`). The partition-id hash — computed over
    /// the sort-key AST-prefix text at write time and embedded in the patch's partition name —
    /// keeps v1 and v2 patches, and pre/post-`ALTER MODIFY ORDER BY` patches, isolated from each
    /// other's merges.
    UInt8 format_version = V1_FORMAT_VERSION;

    /// Length of the semantic sort-key prefix persisted on the v2 patch. Written to
    /// `source_parts.dat` right after `format_version`; zero and unused for v1 patches. Stored
    /// so that readers can directly slice the target table's sort key to the shape the patch was
    /// written with, instead of deriving `n_semantic = n_full - 2` by subtracting the two
    /// identity columns after a `getPatchPartMetadataV2` rebuild.
    UInt64 sorting_key_prefix_size = 0;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
SourcePartsSetForPatch buildSourceSetForPatch(Block & block, UInt64 data_version);

}
