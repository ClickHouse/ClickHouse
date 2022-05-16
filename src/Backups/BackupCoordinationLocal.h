#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationHelpers.h>
#include <map>
#include <mutex>


namespace Poco { class Logger; }

namespace DB
{

/// Stores backup contents information in memory.
class BackupCoordinationLocal : public IBackupCoordination
{
public:
    BackupCoordinationLocal();
    ~BackupCoordinationLocal() override;

    void addReplicatedTableDataPath(const String & table_zk_path, const String & table_data_path) override;
    void addReplicatedTablePartNames(
        const String & host_id,
        const DatabaseAndTableName & table_name,
        const String & table_zk_path,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    void finishPreparing(const String & host_id, const String & error_message) override;
    void waitForAllHostsPrepared(const Strings & host_ids, std::chrono::seconds timeout) const override;

    Strings getReplicatedTableDataPaths(const String & table_zk_path) const override;
    Strings getReplicatedTablePartNames(const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path) const override;

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() const override;
    Strings listFiles(const String & prefix, const String & terminator) const override;

    std::optional<FileInfo> getFileInfo(const String & file_name) const override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() const override;

private:
    mutable std::mutex mutex;
    BackupCoordinationReplicatedTablesInfo replicated_tables;
    std::map<String /* file_name */, SizeAndChecksum> file_names; /// Should be ordered alphabetically, see listFiles(). For empty files we assume checksum = 0.
    std::map<SizeAndChecksum, FileInfo> file_infos; /// Information about files. Without empty files.
    Strings archive_suffixes;
    size_t current_archive_suffix = 0;

    const Poco::Logger * log;
};


}
