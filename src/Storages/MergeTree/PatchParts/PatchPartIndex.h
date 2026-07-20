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
class PatchPartIndex
{
public:
    /// On-disk format version of the patch part.
    ///  0 = legacy v1 (sorted by `_part, _part_offset`, applied with `Merge` or `Join` mode).
    ///  1 = v2 (sorted by `sorting_key..., _block_number, _block_offset`, applied with `MergeOnKey`).
    static constexpr UInt8 V1_FORMAT_VERSION = 0;
    static constexpr UInt8 V2_FORMAT_VERSION = 1;
    static constexpr UInt8 MAX_SUPPORTED_FORMAT_VERSION = V2_FORMAT_VERSION;

    static constexpr auto FILENAME = "source_parts.dat";

    PatchPartIndex() = default;
    PatchPartIndex(UInt8 format_version_, String sorting_key_desc_);

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    UInt8 getFormatVersion() const { return format_version; }

    /// The table's sorting key the v2 patch was written with, as a one-line formatted text.
    const String & getSortingKeyDesc() const { return sorting_key_desc; }

    /// Returns an index with the same format version and sorting key but without source parts.
    PatchPartIndex cloneEmpty() const { return PatchPartIndex(format_version, sorting_key_desc); }

    void addSourcePart(const String & name, UInt64 data_version);

    /// `effective_sorting_key` is the effective sort-key prefix for `MergeOnKey` patches
    PatchParts getPatchParts(
        const MergeTreePartInfo & original_part,
        const DataPartPtr & patch_part,
        std::shared_ptr<const KeyDescription> effective_sorting_key) const;

    static PatchPartIndex build(
        const Block & block,
        UInt64 data_version,
        UInt8 format_version,
        String sorting_key_desc);

    static PatchPartIndex merge(const DataPartsVector & source_parts);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    void buildSourcePartsByVersion();

    /// Max data version -> part set that contains all parts from min_max_versions_by_part with this max data version.
    /// Can be reconstructed from source_parts_by_version.
    std::map<UInt64, ActiveDataPartSet> source_parts_by_version;

    /// Part name -> min and max version of updated data stored in patch part for the source part.
    /// Serialized to the file on disk.
    std::map<String, std::pair<UInt64, UInt64>> min_max_versions_by_part;

    UInt64 min_data_version = 0;
    UInt64 max_data_version = 0;
    UInt8 format_version = V1_FORMAT_VERSION;
    /// One-line text of the sorting key the v2 patch was written with.
    String sorting_key_desc;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
PatchPartIndex buildPatchPartIndex(
    Block & block,
    UInt64 data_version,
    const PatchPartMetadata & patch_metadata);

}
