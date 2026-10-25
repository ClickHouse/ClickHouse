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
    extern const MergeTreeSettingsBool group_fsync_before_insert_finish;
}

bool DeferredPartsSync::isEnabled(const MergeTreeData & storage, const StorageMetadataPtr & metadata_snapshot)
{
    const auto settings = storage.getSettings();
    if (!(*settings)[MergeTreeSetting::fsync_after_insert] || !(*settings)[MergeTreeSetting::group_fsync_before_insert_finish])
        return false;

    /// `MergeTreeSink::commitUniqueKeyPart` commits the parts by its own path, so they keep the inline fsync.
    if (metadata_snapshot->hasUniqueKey())
        return false;

    const auto disks = storage.getStoragePolicy()->getDisks();
    return std::ranges::all_of(disks, [](const auto & disk) { return disk->getDataSourceDescription().type == DataSourceType::Local; });
}

void DeferredPartsSync::add(MergeTreeDataPartPtr part)
{
    /// The same part may be added many times, for example, for every deduplicated block of a coalesced async insert.
    if (!queued_parts.insert(part.get()).second)
        return;

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

    /// The parts are kept alive until their files are synced, so that they are not removed in the meantime.
    std::vector<MergeTreeDataPartPtr> parts_to_sync;
    while (!parts.empty() && (!only_old || now - parts.front().commit_time >= min_age_to_sync))
    {
        queued_parts.erase(parts.front().part.get());
        parts_to_sync.push_back(std::move(parts.front().part));
        parts.pop_front();
    }

    if (parts_to_sync.empty())
        return;

    std::vector<std::string> paths;
    for (const auto & part : parts_to_sync)
    {
        for (const auto & entry : std::filesystem::recursive_directory_iterator(part->getDataPartStorage().getFullPath()))
        {
            /// Temporary files, such as `txn_version.txt.tmp`, are renamed over the files of a committed part
            /// when its metadata is updated. The writer syncs them itself, and they may disappear at any moment.
            if (entry.is_regular_file() && !entry.path().filename().string().ends_with(".tmp"))
                paths.push_back(entry.path());
        }
    }

    LOG_TRACE(getLogger("DeferredPartsSync"), "Syncing {} files of {} inserted parts", paths.size(), parts_to_sync.size());
    parallelSyncPaths(paths);
}

}
