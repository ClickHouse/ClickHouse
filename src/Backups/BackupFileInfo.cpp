#include <Backups/BackupFileInfo.h>

#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Interpreters/ProcessList.h>

#include <base/hex.h>


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
    ChecksumsForNewEntry calculateNewEntryChecksumsIfNeeded(const BackupEntryPtr & entry, size_t prefix_size, const ReadSettings & read_settings)
    {
        ChecksumsForNewEntry res;
        /// The partial checksum should be calculated before the full checksum to enable optimization in BackupEntryWithChecksumCalculation.
        res.prefix_checksum = entry->getPartialChecksum(prefix_size, read_settings);
        res.full_checksum = entry->getChecksum(read_settings);
        return res;
    }

    /// We store entries' file names in the backup without leading slashes.
    String removeLeadingSlash(const String & path)
    {
        if (path.starts_with('/'))
            return path.substr(1);
        return path;
    }
}


/// Note: this format doesn't allow to parse data back
/// It is useful only for debugging purposes
String BackupFileInfo::describe() const
{
    String result;
    result += fmt::format("file_name: {};\n", file_name);
    result += fmt::format("size: {};\n", size);
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
    const String & file_name,
    const BackupEntryPtr & backup_entry,
    const BackupPtr & base_backup,
    const ReadSettings & read_settings,
    LoggerPtr log)
{
    auto adjusted_path = removeLeadingSlash(file_name);

    BackupFileInfo info;
    info.file_name = adjusted_path;

    /// If it's a "reference" just set the target to a concrete file
    if (backup_entry->isReference())
    {
        info.reference_target = removeLeadingSlash(backup_entry->getReferenceTarget());
        return info;
    }

    info.size = backup_entry->getSize();
    info.encrypted_by_disk = backup_entry->isEncryptedByDisk();

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
            /// In ClickHouse this can happen for StorageLog for example.
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
                assert(check_base == CheckBackupResult::HasFull);
                assert(info.size == base_backup_file_info->first);

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

BackupFileInfos buildFileInfosForBackupEntries(const BackupEntries & backup_entries, const BackupPtr & base_backup, const ReadSettings & read_settings, ThreadPool & thread_pool, QueryStatusPtr process_list_element)
{
    BackupFileInfos infos;
    infos.resize(backup_entries.size());

    std::atomic_bool failed = false;

    LoggerPtr log = getLogger("FileInfosFromBackupEntries");

    ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, "BackupWorker");
    for (size_t i = 0; i != backup_entries.size(); ++i)
    {
        if (failed)
            break;

        runner([&infos, &backup_entries, &read_settings, &base_backup, &process_list_element, i, log, &failed]()
        {
            if (failed)
                return;
            try
            {
                const auto & name = backup_entries[i].first;
                const auto & entry = backup_entries[i].second;

                if (process_list_element)
                    process_list_element->checkTimeLimit();

                infos[i] = buildFileInfoForBackupEntry(name, entry, base_backup, read_settings, log);
            }
            catch (...)
            {
                failed = true;
                throw;
            }
        });
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    return infos;
}

}
