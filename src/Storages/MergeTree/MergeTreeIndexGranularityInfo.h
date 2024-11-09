#pragma once

#include <optional>
#include <base/types.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IDataPartStorage.h>

namespace DB
{

class MergeTreeData;


/** Various types of mark files are stored in files with various extensions:
  * .mrk, .mrk2, .mrk3, .cmrk, .cmrk2, .cmrk3.
  * This helper allows to obtain mark type from file extension and vice versa.
  */
struct MarkType
{
    explicit MarkType(std::string_view extension);
    MarkType(bool adaptive_, bool compressed_, MergeTreeDataPartType::Value part_type_);

    static bool isMarkFileExtension(std::string_view extension);
    std::string getFileExtension() const;

    std::string describe() const;

    bool adaptive = false;
    bool compressed = false;
    MergeTreeDataPartType::Value part_type = MergeTreeDataPartType::Unknown;
};


/// Meta information about index granularity
struct MergeTreeIndexGranularityInfo
{
public:
    MarkType mark_type;

    /// Fixed size in rows of one granule if index_granularity_bytes is zero
    size_t fixed_index_granularity = 0;

    /// Approximate bytes size of one granule
    size_t index_granularity_bytes = 0;

    MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MergeTreeDataPartType type_);

    MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MarkType mark_type_);

    MergeTreeIndexGranularityInfo(MergeTreeDataPartType type_, bool is_adaptive_, size_t index_granularity_, size_t index_granularity_bytes_);
    MergeTreeIndexGranularityInfo(MarkType mark_type_, size_t index_granularity_, size_t index_granularity_bytes_);

    void changeGranularityIfRequired(const IDataPartStorage & data_part_storage);

    String getMarksFilePath(const String & path_prefix) const
    {
        return path_prefix + mark_type.getFileExtension();
    }

    size_t getMarkSizeInBytes(size_t columns_num = 1) const;

    static std::optional<MarkType> getMarksTypeFromFilesystem(const IDataPartStorage & data_part_storage);

    std::string describe() const;
};

constexpr auto getNonAdaptiveMrkSizeWide() { return sizeof(UInt64) * 2; }
constexpr auto getAdaptiveMrkSizeWide() { return sizeof(UInt64) * 3; }
inline size_t getAdaptiveMrkSizeCompact(size_t columns_num);

}
