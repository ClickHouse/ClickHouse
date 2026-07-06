#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Core/Block.h>

#include <optional>

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

    /// For v2 patches eagerly builds `sorting_key_prefix_description` from `metadata_snapshot`
    /// by slicing the table's ORDER BY expression list to `sorting_key_prefix_size_` children.
    /// For v1 patches (`sorting_key_prefix_size_ = std::nullopt`) the description stays null.
    SourcePartsSetForPatch(
        const StorageMetadataPtr & metadata_snapshot,
        UInt8 format_version_,
        std::optional<UInt64> sorting_key_prefix_size_);

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    UInt8 getFormatVersion() const { return format_version; }

    /// Length of the semantic sort-key prefix of the v2 patch, excluding the trailing
    /// `_block_number`, `_block_offset` identity columns. Zero for v1 patches.
    UInt64 getSortKeyPrefixSize() const { return sorting_key_prefix_size; }

    /// Shared semantic sort-key prefix `KeyDescription`. Built eagerly in the explicit
    /// constructor or in `readBinary` (both take `StorageMetadataPtr`). Nullptr for v1 patches
    /// and for default-constructed sets.
    const std::shared_ptr<const KeyDescription> & getSortingKeyPrefixDescription() const { return sorting_key_prefix_description; }

    void addSourcePart(const String & name, UInt64 data_version);
    PatchParts getPatchParts(const MergeTreePartInfo & original_part, const DataPartPtr & patch_part) const;

    static SourcePartsSetForPatch build(
        const Block & block,
        UInt64 data_version,
        const StorageMetadataPtr & metadata_snapshot,
        std::optional<UInt64> sorting_key_prefix_size_);

    /// Merge patch-on-patch sets. The input parts share the same partition, so their
    /// `format_version`, `sorting_key_prefix_size`, and `sorting_key_prefix_description` are
    /// guaranteed equal; we just copy them from the first part (no metadata rebuild).
    static SourcePartsSetForPatch merge(const DataPartsVector & source_parts);

    void writeBinary(WriteBuffer & out) const;

    /// `metadata_snapshot` is needed to build `sorting_key_prefix_description` for v2 patches;
    /// for v1 patches it is unused. Passing it avoids a separate two-step init.
    void readBinary(ReadBuffer & in, const StorageMetadataPtr & metadata_snapshot);

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
    /// Only the prefix *length* is persisted, not the sort-key AST: v2 readers rebuild the AST
    /// from the table's current metadata and slice it to `sorting_key_prefix_size`.
    UInt8 format_version = V1_FORMAT_VERSION;

    /// Length of the semantic sort-key prefix persisted on the v2 patch, written to
    /// `source_parts.dat` right after `format_version`. Zero and unused for v1 patches.
    UInt64 sorting_key_prefix_size = 0;

    /// Semantic sort-key prefix as a `KeyDescription`, built eagerly in the explicit constructor
    /// or in `readBinary` and shared across every `PatchPartInfo` produced from this set. Nullptr for v1.
    std::shared_ptr<const KeyDescription> sorting_key_prefix_description;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
SourcePartsSetForPatch buildSourceSetForPatch(
    Block & block,
    UInt64 data_version,
    const StorageMetadataPtr & metadata_snapshot,
    std::optional<UInt64> sorting_key_prefix_size);

}
