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
    /// The serialized index starts with the format version of the patch part. If `PatchBlockOffsets` of some source part
    /// is not `Stored`, it starts with `SERIALIZATION_VERSION_WITH_BLOCK_OFFSETS` instead, followed by the format
    /// version, and every source part is followed by its `PatchBlockOffsets`. Older versions refuse to read such index.
    static constexpr UInt8 SERIALIZATION_VERSION_WITH_BLOCK_OFFSETS = 2;
    static_assert(MAX_SUPPORTED_FORMAT_VERSION < SERIALIZATION_VERSION_WITH_BLOCK_OFFSETS);
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
    /// Returns an index with the same format version and sorting key but without source parts.
    [[nodiscard]] PatchPartIndex cloneEmpty() const { return PatchPartIndex(format_version, sorting_key_desc); }

    [[nodiscard]] PatchBlockOffsets getBlockOffsets(const String & part_name) const;

    void addSourcePart(const String & name, UInt64 data_version, PatchBlockOffsets block_offsets);

    /// Throws if the patch is matched with the rows of `original_part` by `(_block_number, _block_offset)`,
    /// but stores another `_block_offset` for them than the part reads now (`block_offsets`).
    /// It is called only for parts that store `_block_offset` without `_block_number`.
    void checkBlockOffsetsOfSourceParts(const MergeTreePartInfo & original_part, PatchBlockOffsets block_offsets, const String & patch_part_name) const;

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
        PatchBlockOffsets block_offsets);

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

    /// Source parts with `PatchBlockOffsets` other than `Stored`. Serialized to the file on disk.
    std::map<String, PatchBlockOffsets> block_offsets_by_part;

    UInt64 min_data_version = 0;
    UInt64 max_data_version = 0;
    MergeTreePatchPartsVersion format_version = MergeTreePatchPartsVersion::V1;
    /// One-line text of the sorting key the v2 patch was written with.
    String sorting_key_desc;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version column in block with const value (data_version).
PatchPartIndex buildPatchPartIndex(
    Block & block,
    UInt64 data_version,
    const PatchPartMetadata & patch_metadata);

}
