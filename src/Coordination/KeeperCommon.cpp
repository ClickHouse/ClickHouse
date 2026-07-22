#include <Coordination/KeeperCommon.h>

#include <string>
#include <filesystem>
#include <thread>

#include <Common/logger_useful.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/CoordinationSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <base/find_symbols.h>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

bool isLocalDisk(const IDisk & disk)
{
    return dynamic_cast<const DiskLocal *>(&disk) != nullptr;
}

uint64_t getLogIdxFromSnapshotPath(const std::string & snapshot_path)
{
    std::filesystem::path path(snapshot_path);
    std::string filename = path.stem();
    std::vector<std::string_view> name_parts;
    splitInto<'_', '.'>(name_parts, filename);
    return parse<uint64_t>(name_parts[1]);
}

std::string getCanonicalSnapshotS3Name(const std::string & snapshot_path)
{
    const uint64_t up_to_log_idx = getLogIdxFromSnapshotPath(snapshot_path);
    return fmt::format("snapshot_{}.bin{}", up_to_log_idx, snapshot_path.ends_with(".zstd") ? ".zstd" : "");
}

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<bool()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context)
{
    LOG_TRACE(logger, "Moving {} to {} from disk {} to disk {}", path_from, path_to, disk_from->getName(), disk_to->getName());
    /// we use empty file with prefix tmp_ to detect incomplete copies
    /// if a copy is complete we don't care from which disk we use the same file
    /// so it's okay if a failure happens after removing of tmp file but before we remove
    /// the file from the source disk
    /// The marker is named after the target: it lives next to the copied file on disk_to and
    /// startup recovery pairs a tmp_ marker with the file of the same name to drop an incomplete
    /// copy. A move may rename the file (changelog rotation shrinks the index range), so keying
    /// the marker off path_from would leave a partial renamed target unmarked.
    auto to_path = fs::path(path_to);
    auto tmp_file_name = to_path.parent_path() / (std::string{tmp_keeper_file_prefix} + to_path.filename().string());

    /// Directories whose entries change during the move. Made durable so a power loss cannot
    /// leave a target that looks complete (marker absent, content present) without it actually
    /// being on stable storage. getDirectorySyncGuard is a no-op (nullptr) on non-local disks.
    /// The marker and the copied file share a directory on disk_to; the source is on disk_from.
    const auto target_dir = to_path.parent_path();
    const auto source_dir = fs::path(path_from).parent_path();
    auto fsync_dir = [](const DiskPtr & disk, const fs::path & dir)
    {
        /// RAII: opens the directory fd now, fdatasyncs it on scope exit.
        SyncGuardPtr dir_sync_guard = disk->getDirectorySyncGuard(dir.generic_string());
    };

    const auto & coordination_settings = keeper_context->getFixedCoordinationSettings();
    auto max_retries_on_init = coordination_settings[CoordinationSetting::disk_move_retries_during_init].value;
    auto retries_sleep = std::chrono::milliseconds(coordination_settings[CoordinationSetting::disk_move_retries_wait_ms]);
    auto run_with_retries = [&](const auto & op, std::string_view operation_description)
    {
        size_t retry_num = 0;
        do
        {
            try
            {
                op();
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(
                    logger,
                    fmt::format(
                        "While moving file {} to disk {} and running '{}'", path_from, disk_to->getName(), operation_description));
                std::this_thread::sleep_for(retries_sleep);
            }

            ++retry_num;
            if (keeper_context->getServerState() == KeeperContext::Phase::INIT && retry_num == max_retries_on_init)
            {
                LOG_ERROR(logger, "Operation '{}' failed too many times", operation_description);
                break;
            }
        } while (!keeper_context->isShutdownCalled());

        LOG_ERROR(
            logger,
            "Failed to run '{}' while moving file {} to disk {}",
            operation_description,
            path_from,
            disk_to->getName());
        return false;
    };

    if (!run_with_retries(
            [&]
            {
                auto buf = disk_to->writeFile(tmp_file_name);
                buf->finalize();
                /// Persist the marker's directory entry before the copy starts, so its presence
                /// (== "copy in progress") is durable. Otherwise a crash mid-copy could leave a
                /// partial target while the marker's creation is lost, making the partial target
                /// look complete on restart.
                fsync_dir(disk_to, target_dir);
            },
            "creating temporary file"))
        return;

    /// sync = true: fsync the copied content so it is on stable storage before we drop the marker.
    if (!run_with_retries(
            [&] { disk_from->copyFile(path_from, *disk_to, path_to, /*read_settings=*/{}, /*write_settings=*/{}, /*cancellation_hook=*/{}, /*sync=*/true); },
            "copying file"))
        return;

    if (!run_with_retries(
            [&]
            {
                disk_to->removeFileIfExists(tmp_file_name);
                /// Persist the marker removal (and, in the same directory, the copied file's entry)
                /// before publishing: after this returns, "marker absent + target present" is
                /// durable, so a restart keeps the target instead of deleting it as an incomplete copy.
                fsync_dir(disk_to, target_dir);
            },
            "removing temporary file"))
        return;

    if (before_file_remove_op && !before_file_remove_op())
    {
        LOG_DEBUG(logger, "Move of {} to disk {} was rejected by the caller, keeping the source file", path_from, disk_to->getName());
        return;
    }

    if (!run_with_retries(
            [&]
            {
                disk_from->removeFileIfExists(path_from);
                /// Persist the source removal so the file does not reappear after a power loss.
                fsync_dir(disk_from, source_dir);
            },
            "removing file from source disk"))
        return;
}
}
