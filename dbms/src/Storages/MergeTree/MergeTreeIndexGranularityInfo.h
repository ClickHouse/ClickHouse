#pragma once

#include <optional>
#include <Core/Types.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <DataStreams/MarkInCompressedFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PART_TYPE;
}

class MergeTreeData;

/// Meta information about index granularity
struct MergeTreeIndexGranularityInfo
{
public:
    /// Marks file extension '.mrk' or '.mrk2'
    String marks_file_extension;

    /// Size of one mark in file two or three size_t numbers
    UInt32 mark_size_in_bytes = 0;

    UInt8 skip_index_mark_size_in_bytes = 0;

    /// Is stride in rows between marks non fixed?
    bool is_adaptive = false;

    /// Fixed size in rows of one granule if index_granularity_bytes is zero
    size_t fixed_index_granularity = 0;

    /// Approximate bytes size of one granule
    size_t index_granularity_bytes = 0;

    bool initialized = false;

    MergeTreeIndexGranularityInfo() {}

    MergeTreeIndexGranularityInfo(
        const MergeTreeData & storage, MergeTreeDataPartType part_type, size_t columns_num);

    void initialize(const MergeTreeData & storage, MergeTreeDataPartType part_type, size_t columns_num);

    void changeGranularityIfRequired(const std::string & path_to_part);

    String getMarksFilePath(const String & path_prefix) const
    {
        return path_prefix + marks_file_extension;
    }

    static std::optional<std::string> getMrkExtensionFromFS(const std::string & path_to_table);

private:
    void setAdaptive(size_t index_granularity_bytes_, MergeTreeDataPartType part_type, size_t columns_num);
    void setNonAdaptive();
    void setCompactAdaptive(size_t index_granularity_bytes_, size_t columns_num);
};

constexpr inline auto getNonAdaptiveMrkExtension() { return ".mrk"; }
constexpr inline auto getNonAdaptiveMrkSize() { return sizeof(MarkInCompressedFile) * 2; }

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type);
size_t getAdaptiveMrkSize(MergeTreeDataPartType part_type, size_t columns_num);

}
