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

    void syncStage(const String & current_host, int stage, const Strings & wait_hosts, std::chrono::seconds timeout) override;
    void syncStageError(const String & current_host, const String & error_message) override;
       
    void addReplicatedPartNames(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;
    Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_zk_path, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_zk_path) const override;

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
    BackupCoordinationReplicatedPartNames replicated_part_names;
    std::unordered_map<String, Strings> replicated_data_paths;
    std::map<String /* file_name */, SizeAndChecksum> file_names; /// Should be ordered alphabetically, see listFiles(). For empty files we assume checksum = 0.
    std::map<SizeAndChecksum, FileInfo> file_infos; /// Information about files. Without empty files.
    Strings archive_suffixes;
    size_t current_archive_suffix = 0;
};


}
