#include <Storages/MergeTree/DeferredPartsSync.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Disks/StoragePolicy.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <filesystem>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool fsync_after_insert;
    extern const MergeTreeSettingsBoolAuto group_fsync_before_insert_finish;
}

bool DeferredPartsSync::shouldSyncExistingParts(const MergeTreeData & storage)
{
    if (!(*storage.getSettings())[MergeTreeSetting::fsync_after_insert])
        return false;

    const auto disks = storage.getStoragePolicy()->getDisks();
    return std::ranges::all_of(disks, [](const auto & disk) { return disk->getDataSourceDescription().type == DataSourceType::Local; });
}

bool DeferredPartsSync::isEnabled(const MergeTreeData & storage, const StorageMetadataPtr & metadata_snapshot)
{
    if (!shouldSyncExistingParts(storage))
        return false;

    const auto settings = storage.getSettings();

    /// With `auto`, only for non-replicated tables: another replica can deduplicate an insert against
    /// a part that is committed but not yet synced here, and it has no local files to sync.
    if (!(*settings)[MergeTreeSetting::group_fsync_before_insert_finish].valueOr(!storage.supportsReplication()))
        return false;

    /// `MergeTreeSink::commitUniqueKeyPart` commits the parts by its own path, so they keep the inline fsync.
    return !metadata_snapshot->hasUniqueKey();
}

DeferredPartsSync::~DeferredPartsSync()
{
    if (parts.empty())
        return;

    try
    {
        syncAll();
    }
    catch (...)
    {
        /// The insert is not acknowledged anyway, and the parts that failed to sync are passed to `on_failed_sync`,
        /// so a retry of the insert is not deduplicated against them.
        tryLogCurrentException(getLogger("DeferredPartsSync"), "Cannot sync the parts committed by a failed insert");
    }
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
    /// The index of the part in `parts_to_sync` for every path.
    std::vector<size_t> path_part_indices;
    for (size_t part_index = 0; part_index < parts_to_sync.size(); ++part_index)
    {
        for (const auto & entry : std::filesystem::recursive_directory_iterator(parts_to_sync[part_index]->getDataPartStorage().getFullPath()))
        {
            /// Temporary files, such as `txn_version.txt.tmp`, are renamed over the files of a committed part
            /// when its metadata is updated. The writer syncs them itself, and they may disappear at any moment.
            if (entry.is_regular_file() && !entry.path().filename().string().ends_with(".tmp"))
            {
                paths.push_back(entry.path());
                path_part_indices.push_back(part_index);
            }
        }
    }

    LOG_TRACE(getLogger("DeferredPartsSync"), "Syncing {} files of {} inserted parts", paths.size(), parts_to_sync.size());

    std::vector<UInt8> failed;
    try
    {
        parallelSyncPaths(paths, failed);
    }
    catch (...)
    {
        if (on_failed_sync)
        {
            std::vector<UInt8> part_failed(parts_to_sync.size(), 0);
            for (size_t i = 0; i < failed.size(); ++i)
                if (failed[i])
                    part_failed[path_part_indices[i]] = 1;

            for (size_t part_index = 0; part_index < parts_to_sync.size(); ++part_index)
            {
                if (!part_failed[part_index])
                    continue;

                /// The error of the fsync is more important, so it is rethrown below.
                try
                {
                    on_failed_sync(parts_to_sync[part_index]);
                }
                catch (...)
                {
                    tryLogCurrentException(getLogger("DeferredPartsSync"),
                        fmt::format("Cannot handle the part {} that failed to sync", parts_to_sync[part_index]->name));
                }
            }
        }
        throw;
    }
}

}
