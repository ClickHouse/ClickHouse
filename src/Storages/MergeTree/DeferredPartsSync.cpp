#include <Storages/MergeTree/DeferredPartsSync.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Disks/StoragePolicy.h>
#include <Common/logger_useful.h>

#include <filesystem>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool fsync_after_insert;
    extern const MergeTreeSettingsBool defer_fsync_after_insert;
}

bool DeferredPartsSync::isEnabled(const MergeTreeData & storage)
{
    const auto settings = storage.getSettings();
    if (!(*settings)[MergeTreeSetting::fsync_after_insert] || !(*settings)[MergeTreeSetting::defer_fsync_after_insert])
        return false;

    const auto disks = storage.getStoragePolicy()->getDisks();
    return std::ranges::all_of(disks, [](const auto & disk) { return disk->getDataSourceDescription().type == DataSourceType::Local; });
}

void DeferredPartsSync::add(MergeTreeDataPartPtr part)
{
    parts.push_back({std::move(part), std::chrono::steady_clock::now()});
}

void DeferredPartsSync::syncOld()
{
    sync(/*only_old=*/ true);
}

void DeferredPartsSync::syncAll()
{
    sync(/*only_old=*/ false);
}

void DeferredPartsSync::sync(bool only_old)
{
    /// A part committed this long ago was most likely already written back by the OS,
    /// so syncing it is cheap. It is also much less than `old_parts_lifetime`, so holding the
    /// parts for this time does not delay the removal of parts that were merged in the meantime.
    static constexpr auto min_age_to_sync = std::chrono::seconds(30);
    const auto now = std::chrono::steady_clock::now();

    std::vector<std::string> paths;
    size_t num_parts = 0;
    while (!parts.empty() && (!only_old || now - parts.front().commit_time >= min_age_to_sync))
    {
        for (const auto & entry : std::filesystem::recursive_directory_iterator(parts.front().part->getDataPartStorage().getFullPath()))
            if (entry.is_regular_file())
                paths.push_back(entry.path());

        parts.pop_front();
        ++num_parts;
    }

    if (num_parts == 0)
        return;

    LOG_TRACE(getLogger("DeferredPartsSync"), "Syncing {} files of {} inserted parts", paths.size(), num_parts);
    parallelSyncPaths(paths);
}

}
