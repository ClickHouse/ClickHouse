#include <Coordination/KeeperCommon.h>

#include <string>
#include <filesystem>

#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/CoordinationSettings.h>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

static size_t findLastSlash(StringRef path)
{
    if (path.size == 0)
        return std::string::npos;

    for (size_t i = path.size - 1; i > 0; --i)
    {
        if (path.data[i] == '/')
            return i;
    }

    if (path.data[0] == '/')
        return 0;

    return std::string::npos;
}

StringRef parentNodePath(StringRef path)
{
    auto rslash_pos = findLastSlash(path);
    if (rslash_pos > 0)
        return StringRef{path.data, rslash_pos};
    return "/";
}

StringRef getBaseNodeName(StringRef path)
{
    size_t basename_start = findLastSlash(path);
    return StringRef{path.data + basename_start + 1, path.size - basename_start - 1};
}

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<void()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context)
{
    LOG_TRACE(logger, "Moving {} to {} from disk {} to disk {}", path_from, path_to, disk_from->getName(), disk_to->getName());
    /// we use empty file with prefix tmp_ to detect incomplete copies
    /// if a copy is complete we don't care from which disk we use the same file
    /// so it's okay if a failure happens after removing of tmp file but before we remove
    /// the file from the source disk
    auto from_path = fs::path(path_from);
    auto tmp_file_name = from_path.parent_path() / (std::string{tmp_keeper_file_prefix} + from_path.filename().string());

    const auto & coordination_settings = keeper_context->getCoordinationSettings();
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
            },
            "creating temporary file"))
        return;

    if (!run_with_retries([&] { disk_from->copyFile(from_path, *disk_to, path_to, {}); }, "copying file"))
        return;

    if (!run_with_retries([&] { disk_to->removeFileIfExists(tmp_file_name); }, "removing temporary file"))
        return;

    if (before_file_remove_op)
        before_file_remove_op();

    if (!run_with_retries([&] { disk_from->removeFileIfExists(path_from); }, "removing file from source disk"))
        return;
}
}
