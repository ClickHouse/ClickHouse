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
    extern const int LOGICAL_ERROR;
}


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

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(const MergeTreeData & storage,
    const String & marks_file_extension_, UInt8 mark_size_in_bytes_)
    : marks_file_extension(marks_file_extension_), mark_size_in_bytes(mark_size_in_bytes_)
{
    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;
    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
        setNonAdaptive();
    else
        setAdaptive(storage_settings->index_granularity_bytes);
}


void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const String & path)
{
    auto mrk_ext = getMrkExtensionFromFS(path);
    if (mrk_ext && *mrk_ext == ".mrk") /// TODO
        setNonAdaptive();
}

void MergeTreeIndexGranularityInfo::setAdaptive(size_t index_granularity_bytes_)
{
    is_adaptive = true;
    index_granularity_bytes = index_granularity_bytes_;
    
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    index_granularity_bytes = 0;
}

}
