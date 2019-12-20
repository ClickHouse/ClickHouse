#pragma once

#include <optional>
#include <Core/Types.h>

namespace DB
{

class MergeTreeData;
/// Meta information about index granularity
struct MergeTreeIndexGranularityInfo
{
public:
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

    MergeTreeIndexGranularityInfo(
        const MergeTreeData & storage);

    void changeGranularityIfRequired(const std::string & path_to_part);

    String getMarksFilePath(const String & column_path) const
    {
        return column_path + marks_file_extension;
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
