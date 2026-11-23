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
  * It is used to get patches required for applying to the regular parts.
  */
class PatchPartIndex
{
public:
    static constexpr UInt8 MAX_SUPPORTED_FORMAT_VERSION = static_cast<UInt8>(MergeTreePatchPartsVersion::V2);
    /// The serialized index starts with the format version of the patch part. If some flag is set, it starts with
    /// `SERIALIZATION_VERSION_WITH_FLAGS` instead, followed by the format version and the flags, so that older
    /// versions, which don't know the flags, refuse to read it.
    static constexpr UInt8 SERIALIZATION_VERSION_WITH_FLAGS = 2;
    static_assert(MAX_SUPPORTED_FORMAT_VERSION < SERIALIZATION_VERSION_WITH_FLAGS);
    static constexpr auto FILENAME = "source_parts.dat";

    PatchPartIndex(MergeTreePatchPartsVersion format_version_, String sorting_key_desc_);

    [[nodiscard]] bool empty() const { return min_max_versions_by_part.empty(); }
    [[nodiscard]] UInt64 getMinDataVersion() const { return min_data_version; }
    [[nodiscard]] UInt64 getMaxDataVersion() const { return max_data_version; }

    [[nodiscard]] UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    [[nodiscard]] UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    [[nodiscard]] MergeTreePatchPartsVersion getFormatVersion() const { return format_version; }

    /// The table's sorting key the v2 patch was written with, as a one-line formatted text.
    [[nodiscard]] const String & getSortingKeyDesc() const { return sorting_key_desc; }
    /// Whether the patch stores the row position as `_block_offset` of rows of parts that store `_block_offset`
    /// without `_block_number` (see `IMergeTreeDataPart::storesBlockOffsetWithoutBlockNumber`). True for patches
    /// written with `use_block_offset_only_with_block_number` while the table has such parts. Patches written by
    /// older versions store the stored `_block_offset`. Patches with different values are put to different
    /// partitions, so they are never merged.
    [[nodiscard]] bool blockOffsetsAreRowPositions() const { return block_offsets_are_row_positions; }

    /// Returns an index with the same format version, sorting key and `blockOffsetsAreRowPositions` but without source parts.
    [[nodiscard]] PatchPartIndex cloneEmpty() const;

    void addSourcePart(const String & name, UInt64 data_version);

    [[nodiscard]] PatchParts getPatchParts(
        const MergeTreePartInfo & original_part,
        const DataPartPtr & patch_part,
        std::shared_ptr<const KeyDescription> effective_sorting_key,
        NameSet stored_sorting_key_columns) const;

    static PatchPartIndex build(
        const Block & block,
        UInt64 data_version,
        MergeTreePatchPartsVersion format_version,
        String sorting_key_desc,
        bool block_offsets_are_row_positions);

    static PatchPartIndex merge(const DataPartsVector & source_parts);
    void writeBinary(WriteBuffer & out) const;
    static PatchPartIndex readBinary(ReadBuffer & in);

private:
    void buildSourcePartsByVersion();

    /// Max data version -> part set that contains all parts from min_max_versions_by_part with this max data version.
    /// Can be reconstructed from min_max_versions_by_part.
    std::map<UInt64, ActiveDataPartSet> source_parts_by_version;

    /// Part name -> min and max version of updated data stored in patch part for the source part.
    /// Serialized to the file on disk.
    std::map<String, std::pair<UInt64, UInt64>> min_max_versions_by_part;

    UInt64 min_data_version = 0;
    UInt64 max_data_version = 0;
    MergeTreePatchPartsVersion format_version = MergeTreePatchPartsVersion::V1;
    /// One-line text of the sorting key the v2 patch was written with.
    String sorting_key_desc;
    /// Serialized as a flag, see `SERIALIZATION_VERSION_WITH_FLAGS`.
    bool block_offsets_are_row_positions = false;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version column in block with const value (data_version).
PatchPartIndex buildPatchPartIndex(
    Block & block,
    UInt64 data_version,
    const PatchPartMetadata & patch_metadata);

}
