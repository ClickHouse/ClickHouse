#pragma once
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/IDisk.h>
#include <memory>
#include <string>

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;


class DataPartStorageOnDisk final : public IDataPartStorage
{
public:
    DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    std::string getFullPath() const override;
    std::string getRelativePath() const override;
    std::string getPartDirectory() const override { return part_dir; }
    std::string getFullRootPath() const override;

    MutableDataPartStoragePtr getProjection(const std::string & name) override;
    DataPartStoragePtr getProjection(const std::string & name) const override;

    bool exists() const override;
    bool exists(const std::string & name) const override;
    bool isDirectory(const std::string & name) const override;

    Poco::Timestamp getLastModified() const override;
    DataPartStorageIteratorPtr iterate() const override;

    size_t getFileSize(const std::string & file_name) const override;
    UInt32 getRefCount(const std::string & file_name) const override;

    UInt64 calculateTotalSizeOnDisk() const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    void checkConsistency(const MergeTreeDataPartChecksums & checksums) const override;

    void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        MergeTreeDataPartState state,
        Poco::Logger * log) override;

    /// Returns path to place detached part in or nullopt if we don't need to detach part (if it already exists and has the same content)
    std::optional<String> getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached, bool broken) const override;

    /// Returns true if detached part already exists and has the same content (compares checksums.txt and the list of files)
    bool looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path, std::optional<String> & original_checksums_content,
                                                      std::optional<Strings> & original_files_list) const;

    void setRelativePath(const std::string & path) override;

    std::string getDiskName() const override;
    std::string getDiskType() const override;
    bool isStoredOnRemoteDisk() const override;
    bool supportZeroCopyReplication() const override;
    bool supportParallelWrite() const override;
    bool isBroken() const override;
    void syncRevision(UInt64 revision) const override;
    UInt64 getRevision() const override;
    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & paths) const override;
    std::string getDiskPath() const override;
    ReservationPtr reserve(UInt64 bytes) const override;
    ReservationPtr tryReserve(UInt64 bytes) const override;
    String getUniqueId() const override;

    void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        BackupEntries & backup_entries,
        bool make_temporary_hard_links,
        TemporaryFilesOnDisks * temp_dirs) const override;

    MutableDataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        bool make_source_readonly,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        bool copy_instead_of_hardlink,
        const NameSet & files_to_copy_instead_of_hardlinks) const override;

    MutableDataPartStoragePtr clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & disk,
        Poco::Logger * log) const override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;

    void createDirectories() override;
    void createProjection(const std::string & name) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        const WriteSettings & settings) override;

    std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(WriteMode mode) const override;

    void createFile(const String & name) override;
    void moveFile(const String & from_name, const String & to_name) override;
    void replaceFile(const String & from_name, const String & to_name) override;

    void removeFile(const String & name) override;
    void removeFileIfExists(const String & name) override;
    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    SyncGuardPtr getDirectorySyncGuard() const override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;

    void rename(
        const std::string & new_root_path,
        const std::string & new_part_dir,
        Poco::Logger * log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void beginTransaction() override;
    void commitTransaction() override;
    bool hasActiveTransaction() const override { return transaction != nullptr; }

private:
    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
    bool has_shared_transaction = false;

    DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);

    template <typename Op>
    void executeOperation(Op && op);

    void clearDirectory(
        const std::string & dir,
        bool can_remove_shared_data,
        const NameSet & names_not_to_remove,
        const MergeTreeDataPartChecksums & checksums,
        const std::unordered_set<String> & skip_directories,
        bool is_temp,
        MergeTreeDataPartState state,
        Poco::Logger * log,
        bool is_projection) const;
};

}
