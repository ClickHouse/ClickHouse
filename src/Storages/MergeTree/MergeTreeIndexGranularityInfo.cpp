#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_PART_TYPE;
}

std::optional<std::string> MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(const DataPartStoragePtr & data_part_storage)
{
    if (data_part_storage->exists())
    {
        for (auto it = data_part_storage->iterate(); it->isValid(); it->next())
        {
            const auto & ext = fs::path(it->name()).extension();
            if (ext == getNonAdaptiveMrkExtension()
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Wide)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Compact))
                return ext;
        }
    }
    return {};
}

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MergeTreeDataPartType type_)
    : type(type_)
{
    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;

    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
    {
        if (type != MergeTreeDataPartType::Wide)
            throw Exception("Only Wide parts can be used with non-adaptive granularity.", ErrorCodes::NOT_IMPLEMENTED);
        setNonAdaptive();
    }
    else
        setAdaptive(storage_settings->index_granularity_bytes);
}

void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const DataPartStoragePtr & data_part_storage)
{
    auto mrk_ext = getMarksExtensionFromFilesystem(data_part_storage);
    if (mrk_ext && *mrk_ext == getNonAdaptiveMrkExtension())
        setNonAdaptive();
}

void MergeTreeIndexGranularityInfo::setAdaptive(size_t index_granularity_bytes_)
{
    is_adaptive = true;
    marks_file_extension = getAdaptiveMrkExtension(type);
    index_granularity_bytes = index_granularity_bytes_;
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    marks_file_extension = getNonAdaptiveMrkExtension();
    index_granularity_bytes = 0;
}

size_t MergeTreeIndexGranularityInfo::getMarkSizeInBytes(size_t columns_num) const
{
    if (type == MergeTreeDataPartType::Wide)
        return is_adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else if (type == MergeTreeDataPartType::Compact)
        return getAdaptiveMrkSizeCompact(columns_num);
    else if (type == MergeTreeDataPartType::InMemory)
        return 0;
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

size_t getAdaptiveMrkSizeCompact(size_t columns_num)
{
    /// Each mark contains number of rows in granule and two offsets for every column.
    return sizeof(UInt64) * (columns_num * 2 + 1);
}

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type)
{
    if (part_type == MergeTreeDataPartType::Wide)
        return ".mrk2";
    else if (part_type == MergeTreeDataPartType::Compact)
        return ".mrk3";
    else if (part_type == MergeTreeDataPartType::InMemory)
        return "";
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

}
