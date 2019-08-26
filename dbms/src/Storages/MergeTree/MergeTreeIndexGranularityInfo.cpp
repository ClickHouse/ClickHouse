#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/Path.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <iostream>

namespace DB
{
std::optional<std::string> MergeTreeIndexGranularityInfo::getMrkExtensionFromFS(const std::string & path_to_part) const
{
    if (Poco::File(path_to_part).exists())
    {
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator part_it(path_to_part); part_it != end; ++part_it)
        {
            const auto & ext = "." + part_it.path().getExtension();
            if (ext == getNonAdaptiveMrkExtension() || ext == getAdaptiveMrkExtension())
                return ext;
        }
    }
    return {};
}

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(
    const MergeTreeData & storage)
{
    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;
    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
        setNonAdaptive();
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
    mark_size_in_bytes = getAdaptiveMrkSize();
    marks_file_extension = getAdaptiveMrkExtension();
    index_granularity_bytes = index_granularity_bytes_;
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    mark_size_in_bytes = getNonAdaptiveMrkSize();
    marks_file_extension = getNonAdaptiveMrkExtension();
    index_granularity_bytes = 0;
}

}
