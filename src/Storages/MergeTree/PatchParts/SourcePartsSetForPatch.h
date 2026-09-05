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
    static constexpr UInt8 VERSION = 0;
    static constexpr auto FILENAME = "source_parts.dat";

    SourcePartsSetForPatch() = default;

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

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
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
SourcePartsSetForPatch buildSourceSetForPatch(Block & block, UInt64 data_version);

}
