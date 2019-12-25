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

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(
    const MergeTreeData & storage, MergeTreeDataPartType part_type, size_t columns_num)
{
    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;

    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
    {
        if (part_type != MergeTreeDataPartType::WIDE)
            throw Exception("Only Wide parts can be used with non-adaptive granularity.", ErrorCodes::NOT_IMPLEMENTED);
        setNonAdaptive();
    }
    else
        setAdaptive(storage_settings->index_granularity_bytes, part_type, columns_num);

    initialized = true;
}

void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const std::string & path_to_part)
{
    auto mrk_ext = getMrkExtensionFromFS(path_to_part);
    if (mrk_ext && *mrk_ext == getNonAdaptiveMrkExtension())
        setNonAdaptive();
}

void MergeTreeIndexGranularityInfo::setAdaptive(size_t index_granularity_bytes_, MergeTreeDataPartType part_type, size_t columns_num)
{
    is_adaptive = true;
    mark_size_in_bytes = getAdaptiveMrkSize(part_type, columns_num);
    skip_index_mark_size_in_bytes = sizeof(MarkInCompressedFile) + sizeof(UInt64);
    marks_file_extension = getAdaptiveMrkExtension(part_type);
    index_granularity_bytes = index_granularity_bytes_;
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    mark_size_in_bytes = skip_index_mark_size_in_bytes = getNonAdaptiveMrkSize();
    marks_file_extension = getNonAdaptiveMrkExtension();
    index_granularity_bytes = 0;
}

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type)
{
    switch (part_type)
    {
        case MergeTreeDataPartType::WIDE:
            return ".mrk2";
        case MergeTreeDataPartType::COMPACT:
            return ".mrk3";
        default:
            throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
    }
}

size_t getAdaptiveMrkSize(MergeTreeDataPartType part_type, size_t columns_num)
{
    switch (part_type)
    {
        case MergeTreeDataPartType::WIDE:
            return sizeof(UInt64) * 3;
        case MergeTreeDataPartType::COMPACT:
            return sizeof(UInt64) * (columns_num * 2 + 1);
        default:
            throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
    }
}

}
