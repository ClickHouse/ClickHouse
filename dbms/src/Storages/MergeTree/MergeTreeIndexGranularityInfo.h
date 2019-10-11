#pragma once

#include <optional>
#include <Core/Types.h>
// #include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;

/// Meta information about index granularity
struct MergeTreeIndexGranularityInfo
{
public:
    using MergeTreeDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

    /// Marks file extension '.mrk' or '.mrk2'
    String marks_file_extension;

    /// Size of one mark in file two or three size_t numbers
    UInt8 mark_size_in_bytes;

    /// Is stride in rows between marks non fixed?
    bool is_adaptive;

    /// Fixed size in rows of one granule if index_granularity_bytes is zero
    size_t fixed_index_granularity;

    /// Approximate bytes size of one granule
    size_t index_granularity_bytes;

    MergeTreeIndexGranularityInfo(const MergeTreeDataPartPtr & part);

    void changeGranularityIfRequired(const MergeTreeDataPartPtr & part);

    String getMarksFilePath(const String & path_prefix) const
    {
        return path_prefix + marks_file_extension;
    }
private:

    void setAdaptive(size_t index_granularity_bytes_);
    void setNonAdaptive();
    std::optional<std::string> getMrkExtensionFromFS(const std::string & path_to_table) const;
};

constexpr inline auto getNonAdaptiveMrkExtension() { return ".mrk"; }
constexpr inline auto getAdaptiveMrkExtension() { return ".mrk2"; }
constexpr inline auto getNonAdaptiveMrkSize() { return sizeof(UInt64) * 2; }
constexpr inline auto getAdaptiveMrkSize() { return sizeof(UInt64) * 3; }

}
