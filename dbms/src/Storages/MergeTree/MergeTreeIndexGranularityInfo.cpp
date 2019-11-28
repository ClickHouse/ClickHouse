#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/Path.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <iostream>

namespace DB
{
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
    initialize(storage, part_type, columns_num);
}

void MergeTreeIndexGranularityInfo::initialize(const MergeTreeData & storage, MergeTreeDataPartType part_type, size_t columns_num)
{
    if (initialized)
        return;

    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;

    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
    {
        if (part_type != MergeTreeDataPartType::WIDE)
            throw ""; /// FIXME normal exception
        setNonAdaptive();
    }
    else
        setAdaptive(storage_settings->index_granularity_bytes, part_type, columns_num);
}


void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const std::string & path_to_part)
{
    /// FIXME check when we cant create compact part
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

}
