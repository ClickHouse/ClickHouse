#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/Path.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_PART_TYPE;
}

std::optional<std::string> MergeTreeIndexGranularityInfo::getMrkExtensionFromFS(const std::string & path_to_part)
{
    if (Poco::File(path_to_part).exists())
    {
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator part_it(path_to_part); part_it != end; ++part_it)
        {
            const auto & ext = "." + part_it.path().getExtension();
            if (ext == getNonAdaptiveMrkExtension()
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::COMPACT))
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
        if (type != MergeTreeDataPartType::WIDE)
            throw Exception("Only Wide parts can be used with non-adaptive granularity.", ErrorCodes::NOT_IMPLEMENTED);
        setNonAdaptive();
    }
    else
        setAdaptive(storage_settings->index_granularity_bytes);
}

void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const std::string & path_to_part)
{
    auto mrk_ext = getMrkExtensionFromFS(path_to_part);
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
    if (type == MergeTreeDataPartType::WIDE)
        return is_adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else if (type == MergeTreeDataPartType::COMPACT)
        return sizeof(UInt64) * (columns_num * 2 + 1);
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type)
{
    if (part_type == MergeTreeDataPartType::WIDE)
        return ".mrk2";
    else if (part_type == MergeTreeDataPartType::COMPACT)
        return ".mrk3";
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

}
