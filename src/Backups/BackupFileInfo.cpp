#include <Backups/BackupFileInfo.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Common/CurrentThread.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Interpreters/ProcessList.h>

#include <atomic>


namespace ProfileEvents
{
    extern const Event BackupPreparingFileInfosMicroseconds;
}


namespace DB
{

namespace
{
    using SizeAndChecksum = std::pair<UInt64, UInt128>;

    std::optional<SizeAndChecksum> getInfoAboutFileFromBaseBackupIfExists(const BackupPtr & base_backup, const std::string & file_path)
    {
        if (base_backup && base_backup->fileExists(file_path))
            return base_backup->getFileSizeAndChecksum(file_path);

        return std::nullopt;
    }

    enum class CheckBackupResult : uint8_t
    {
        HasPrefix,
        HasFull,
        HasNothing,
    };

    CheckBackupResult checkBaseBackupForFile(const SizeAndChecksum & base_backup_info, const BackupFileInfo & new_entry_info)
    {
        /// We cannot reuse base backup because our file is smaller
        /// than file stored in previous backup
        if ((new_entry_info.size < base_backup_info.first) || !base_backup_info.first)
            return CheckBackupResult::HasNothing;

        if (base_backup_info.first == new_entry_info.size)
            return CheckBackupResult::HasFull;

        return CheckBackupResult::HasPrefix;

    }

    struct ChecksumsForNewEntry
    {
        /// 0 is the valid checksum of empty data.
        UInt128 full_checksum = 0;

        /// std::nullopt here means that it's too difficult to calculate a partial checksum so it shouldn't be used.
        std::optional<UInt128> prefix_checksum;
    };

    /// Calculate checksum for backup entry if it's empty.
    /// Also able to calculate additional checksum of some prefix.
    ChecksumsForNewEntry calculateNewEntryChecksumsIfNeeded(const BackupEntryPtr & entry, UInt64 prefix_size, const ReadSettings & read_settings)
    {
        ChecksumsForNewEntry res;
        /// The partial checksum should be calculated before the full checksum to enable optimization in BackupEntryWithChecksumCalculation.
        res.prefix_checksum = entry->getPartialChecksum(prefix_size, read_settings);
        res.full_checksum = entry->getChecksum(read_settings);
        return res;
    }

    /// We store entries' file names in the backup without leading slashes.
    void removeLeadingSlash(String & path)
    {
        if (path.starts_with('/'))
            path.erase(0, 1);
    }
}


/// Note: this format doesn't allow to parse data back
/// It is useful only for debugging purposes
String BackupFileInfo::describe() const
{
    String result;
    result += fmt::format("file_name: {};\n", file_name);
    result += fmt::format("size: {};\n", size);
    result += fmt::format("object_key: {};\n", object_key);
    result += fmt::format("checksum: {};\n", getHexUIntLowercase(checksum));
    result += fmt::format("base_size: {};\n", base_size);
    result += fmt::format("base_checksum: {};\n", getHexUIntLowercase(checksum));
    result += fmt::format("data_file_name: {};\n", data_file_name);
    result += fmt::format("data_file_index: {};\n", data_file_index);
    result += fmt::format("encrypted_by_disk: {};\n", encrypted_by_disk);
    if (!reference_target.empty())
        result += fmt::format("reference_target: {};\n", reference_target);
    return result;
}


BackupFileInfo buildFileInfoForBackupEntry(
    String file_name,
    const BackupEntryPtr & backup_entry,
    const BackupPtr & base_backup,
    const ReadSettings & read_settings,
    LoggerPtr log)
{
    /// The name is taken by value so that the caller can hand over its own copy: a backup of millions of
    /// files holds gigabytes of path characters, and one copy of them is enough.
    BackupFileInfo info;
    info.file_name = std::move(file_name);
    removeLeadingSlash(info.file_name);

    /// The adjusted path is needed throughout this function - reference the copy stored in `info` instead
    /// of keeping a second one alive for the whole call.
    const String & adjusted_path = info.file_name;

    /// If it's a "reference" just set the target to a concrete file
    if (backup_entry->isReference())
    {
        info.reference_target = backup_entry->getReferenceTarget();
        removeLeadingSlash(info.reference_target);
        return info;
    }

    info.size = backup_entry->getSize();
    info.encrypted_by_disk = backup_entry->isEncryptedByDisk();

    if (backup_entry->isFromRemoteFile())
    {
        info.object_key = backup_entry->getRemotePath();
        return info;
    }

    /// We don't set `info.data_file_name` and `info.data_file_index` in this function because they're set during backup coordination
    /// (see the class BackupCoordinationFileInfos).

    if (!info.size)
    {
        /// Empty file.
        return info;
    }

    if (!log)
        log = getLogger("FileInfoFromBackupEntry");

    std::optional<SizeAndChecksum> base_backup_file_info = getInfoAboutFileFromBaseBackupIfExists(base_backup, adjusted_path);

    /// We have info about this file in base backup
    /// If file has no checksum -- calculate and fill it.
    if (base_backup_file_info)
    {
        LOG_TRACE(log, "File {} found in base backup, checking for equality", adjusted_path);
        CheckBackupResult check_base = checkBaseBackupForFile(*base_backup_file_info, info);

        /// File with the same name but smaller size exist in previous backup
        if (check_base == CheckBackupResult::HasPrefix)
        {
            auto checksums = calculateNewEntryChecksumsIfNeeded(backup_entry, base_backup_file_info->first, read_settings);
            info.checksum = checksums.full_checksum;

            /// We have prefix of this file in backup with the same checksum.
            /// In ClickHouse this can happen for StorageLog for example,
            /// or for .sql files that store DDL if only last part of the query was changed.
            if (checksums.prefix_checksum == base_backup_file_info->second)
            {
                LOG_TRACE(log, "Found prefix of file {} in the base backup, will write rest of the file to current backup", adjusted_path);
                info.base_size = base_backup_file_info->first;
                info.base_checksum = base_backup_file_info->second;
            }
            else
            {
                LOG_TRACE(log, "Prefix of file {} doesn't match the file in the base backup", adjusted_path);
            }
        }
        else
        {
            /// We have full file or have nothing, first of all let's get checksum
            /// of current file
            auto checksums = calculateNewEntryChecksumsIfNeeded(backup_entry, 0, read_settings);
            info.checksum = checksums.full_checksum;

            if (info.checksum == base_backup_file_info->second)
            {
                LOG_TRACE(log, "Found whole file {} in base backup", adjusted_path);
                chassert(check_base == CheckBackupResult::HasFull);
                chassert(info.size == base_backup_file_info->first);

                info.base_size = base_backup_file_info->first;
                info.base_checksum = base_backup_file_info->second;
                /// Actually we can add this info to coordination and exist,
                /// but we intentionally don't do it, otherwise control flow
                /// of this function will be very complex.
            }
            else
            {
                LOG_TRACE(log, "Whole file {} in base backup doesn't match by checksum", adjusted_path);
            }
        }
    }
    else
    {
        auto checksums = calculateNewEntryChecksumsIfNeeded(backup_entry, 0, read_settings);
        info.checksum = checksums.full_checksum;
    }

    /// We don't have info about this file_name (sic!) in base backup,
    /// however file could be renamed, so we will check one more time using size and checksum
    if (base_backup && base_backup->fileExists(std::pair{info.size, info.checksum}))
    {
        LOG_TRACE(log, "Found a file in the base backup with the same size and checksum as {}", adjusted_path);
        info.base_size = info.size;
        info.base_checksum = info.checksum;
    }

    if (base_backup && !info.base_size)
        LOG_TRACE(log, "Nothing found for file {} in base backup", adjusted_path);

    return info;
}

BackupFileInfosAndEntries buildFileInfosForBackupEntries(BackupEntries && backup_entries, const BackupPtr & base_backup, const ReadSettings & read_settings, ThreadPool & thread_pool, QueryStatusPtr process_list_element)
{
    LoggerPtr log = getLogger("FileInfosFromBackupEntries");
    LOG_TRACE(log, "Building file infos for backup entries");
    auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::BackupPreparingFileInfosMicroseconds);

    const size_t num_entries = backup_entries.size();

    /// Take the names and the entries apart and release the pairs. The names are consumed here, one
    /// into each file info, so they must not outlive this function; the entries go on to the writing
    /// phase, which holds a pointer per entry instead of a 40-byte pair slot it can no longer read.
    Strings names;
    names.reserve(num_entries);
    BackupFileInfosAndEntries res;
    res.entries.reserve(num_entries);
    for (auto & [name, entry] : backup_entries)
    {
        names.push_back(std::move(name));
        res.entries.push_back(std::move(entry));
    }
    BackupEntries{}.swap(backup_entries);

    res.file_infos.resize(num_entries);

    /// A task per entry, so that a slow one holds up nothing, bounded so that millions of them do not keep a task
    /// each: `TaskTracker::add` admits one while fewer than this are in flight, and rethrows the first failure.
    const size_t max_tasks_inflight = 2 * std::max<size_t>(1, thread_pool.getMaxThreads());

    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);

    /// The tracker rethrows the first failure, which stops the loop below from adding more tasks, but the tasks
    /// already given to the pool still run. Calculating a checksum reads a whole file, so without this they go on
    /// reading - up to `max_tasks_inflight` of them - after the backup has failed or been cancelled.
    std::atomic_bool failed = false;

    /// Capturing by reference is fine: everything captured outlives the tracker, being created before it.
    TaskTracker task_tracker(threadPoolCallbackRunnerUnsafe<void>(thread_pool, ThreadName::BACKUP_WORKER), max_tasks_inflight, limited_log);

    try
    {
        for (size_t i = 0; i != num_entries; ++i)
        {
            task_tracker.add([&names, &res, &read_settings, &base_backup, &process_list_element, &failed, i, log,
                              current_component = Coordination::getCurrentComponent()]
            {
                if (failed)
                    return;

                auto component_guard = Coordination::setCurrentComponent(current_component);

                const auto & entry = res.entries[i];

                try
                {
                    if (process_list_element)
                        process_list_element->checkTimeLimit();

                    /// The name is moved out: from here on it lives in `res.file_infos[i]`, indexed the same way.
                    res.file_infos[i] = buildFileInfoForBackupEntry(std::move(names[i]), entry, base_backup, read_settings, log);
                }
                catch (...)
                {
                    /// Stop the other tasks, as `copyS3File` does.
                    failed = true;
                    throw;
                }
            });
        }

        task_tracker.waitAll();
    }
    catch (...)
    {
        /// `~TaskTracker` asserts that no task is left before it waits, so a failed or cancelled backup has to
        /// drain it here, as `copyS3File` does, or the assertion aborts a debug build while it unwinds.
        task_tracker.safeWaitAll();
        throw;
    }

    return res;
}

}
