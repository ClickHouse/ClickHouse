#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <base/defines.h>
#include <map>
#include <mutex>


namespace Poco { class Logger; }

namespace DB
{

/// Implementation of the IBackupCoordination interface performing coordination in memory.
class BackupCoordinationLocal : public IBackupCoordination
{
public:
    BackupCoordinationLocal();
    ~BackupCoordinationLocal() override;

    void setStatus(const String & current_host, const String & new_status, const String & message) override;
    Strings setStatusAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts) override;
    Strings setStatusAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms) override;

    void addReplicatedPartNames(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;
    Strings getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedMutations(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name,
                                const std::vector<MutationInfo> & mutations) override;
    std::vector<MutationInfo> getReplicatedMutations(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_shared_id, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_shared_id) const override;

    void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path) override;
    Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const override;

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() const override;
    Strings listFiles(const String & directory, bool recursive) const override;
    bool hasFiles(const String & directory) const override;

    std::optional<FileInfo> getFileInfo(const String & file_name) const override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() const override;

private:
    mutable std::mutex mutex;
    BackupCoordinationReplicatedTables replicated_tables TSA_GUARDED_BY(mutex);
    BackupCoordinationReplicatedAccess replicated_access TSA_GUARDED_BY(mutex);
    std::map<String /* file_name */, SizeAndChecksum> file_names TSA_GUARDED_BY(mutex); /// Should be ordered alphabetically, see listFiles(). For empty files we assume checksum = 0.
    std::map<SizeAndChecksum, FileInfo> file_infos TSA_GUARDED_BY(mutex); /// Information about files. Without empty files.
    Strings archive_suffixes TSA_GUARDED_BY(mutex);
    size_t current_archive_suffix TSA_GUARDED_BY(mutex) = 0;
};

}
