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
            if (ext == getNonAdaptiveMrkExtension(false)
                || ext == getNonAdaptiveMrkExtension(true)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Wide, false)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Wide, true)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Compact, false)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::Compact, true))
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
    is_compress_marks = storage_settings->compress_marks;

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
    if (mrk_ext && *mrk_ext == getNonAdaptiveMrkExtension(is_compress_marks))
        setNonAdaptive();
}

void MergeTreeIndexGranularityInfo::setAdaptive(size_t index_granularity_bytes_)
{
    is_adaptive = true;
    marks_file_extension = getAdaptiveMrkExtension(type, is_compress_marks);
    index_granularity_bytes = index_granularity_bytes_;
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    marks_file_extension = getNonAdaptiveMrkExtension(is_compress_marks);
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

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type, bool is_compress_marks)
{
    if (part_type == MergeTreeDataPartType::Wide)
        return is_compress_marks ? ".cmrk2" : ".mrk2";
    else if (part_type == MergeTreeDataPartType::Compact)
        return is_compress_marks ? ".cmrk3" : ".mrk3";
    else if (part_type == MergeTreeDataPartType::InMemory)
        return "";
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

}
