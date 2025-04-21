#include <Storages/MergeTree/MergeTreeCleanupThread.h>

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_parts_interval_seconds;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_temporary_directories_interval_seconds;
    extern const MergeTreeSettingsSeconds temporary_directories_lifetime;
}

MergeTreeCleanupThread::MergeTreeCleanupThread(StorageMergeTree & storage_)
    : IMergeTreeCleanupThread(storage_)
    , storage(storage_)
{
}

Float32 MergeTreeCleanupThread::iterate()
{
    size_t cleaned_other = 0;
    size_t cleaned_part_like = 0;
    size_t cleaned_parts = 0;

    auto storage_settings = storage.getSettings();

    if (auto lock = storage.time_after_previous_cleanup_temporary_directories.compareAndRestartDeferred(
            (*storage_settings)[MergeTreeSetting::merge_tree_clear_old_temporary_directories_interval_seconds]))
    {
        auto shared_lock = storage.lockForShare(
            RWLockImpl::NO_QUERY, (*storage.getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
        /// Both use relative_data_path which changes during rename, so we do it under share lock
        cleaned_part_like += storage.clearOldTemporaryDirectories(
            (*storage.getSettings())[MergeTreeSetting::temporary_directories_lifetime].totalSeconds());
    }

    if (auto lock = storage.time_after_previous_cleanup_parts.compareAndRestartDeferred(
            (*storage_settings)[MergeTreeSetting::merge_tree_clear_old_parts_interval_seconds]))
    {
        cleaned_parts += storage.clearOldPartsFromFilesystem(/* force */ false, /* with_pause_point */ true);
        cleaned_other += storage.clearOldMutations();
        cleaned_part_like += storage.clearEmptyParts();
        cleaned_part_like += storage.clearUnusedPatchParts();
        cleaned_part_like += storage.unloadPrimaryKeysAndClearCachesOfOutdatedParts();
    }

    constexpr Float32 parts_number_amplification = 1.3f; /// Assuming we merge 4-5 parts each time
    Float32 cleaned_inserted_parts = cleaned_parts / parts_number_amplification;
    return cleaned_inserted_parts + cleaned_part_like + cleaned_other;
}

}
