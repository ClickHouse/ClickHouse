#pragma once
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <memory>
#include <string>

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class DataPartStorageOnDiskBase : public IDataPartStorage
{
public:
    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    std::string getFullPath() const override;
    std::string getRelativePath() const override;
    std::string getPartDirectory() const override;
    std::string getFullRootPath() const override;
    std::string getParentDirectory() const override;

    Poco::Timestamp getLastModified() const override;
    UInt64 calculateTotalSizeOnDisk() const override;

    /// Returns path to place detached part in or nullopt if we don't need to detach part (if it already exists and has the same content)
    std::optional<String> getRelativePathForPrefix(LoggerPtr log, const String & prefix, bool detached, bool broken) const override;

    /// Returns true if detached part already exists and has the same content (compares checksums.txt and the list of files)
    bool looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path, std::optional<String> & original_checksums_content,
                                                      std::optional<Strings> & original_files_list) const;

    void setRelativePath(const std::string & path) override;

    std::string getDiskName() const override;
    std::string getDiskType() const override;
    bool isStoredOnRemoteDisk() const override;
    std::optional<String> getCacheName() const override;
    bool supportZeroCopyReplication() const override;
    bool supportParallelWrite() const override;
    bool isBroken() const override;
    bool isReadonly() const override;
    std::string getDiskPath() const override;
    ReservationPtr reserve(UInt64 bytes) const override;
    ReservationPtr tryReserve(UInt64 bytes) const override;

    ReplicatedFilesDescription getReplicatedFilesDescription(const NameSet & file_names) const override;
    ReplicatedFilesDescription getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const override;

    void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        const BackupSettings & backup_settings,
        bool make_temporary_hard_links,
        BackupEntries & backup_entries,
        TemporaryFilesOnDisks * temp_dirs,
        bool is_projection_part,
        bool allow_backup_broken_projection) const override;

    MutableDataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const override;

    MutableDataPartStoragePtr freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const override;

    MutableDataPartStoragePtr clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & dst_disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        LoggerPtr log,
        const std::function<void()> & cancellation_hook
        ) const override;

    void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        LoggerPtr log) override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;
    void createDirectories() override;

    std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(WriteMode mode) const override;

    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    SyncGuardPtr getDirectorySyncGuard() const override;
    bool hasActiveTransaction() const override;

protected:
    DiskPtr getDisk() const;

    DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);
    virtual MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const = 0;

    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
    bool has_shared_transaction = false;

    template <typename Op>
    void executeWriteOperation(Op && op)
    {
        if (transaction)
            op(*transaction);
        else
            op(*volume->getDisk());
    }

private:
    void clearDirectory(
        const std::string & dir,
        const CanRemoveDescription & can_remove_description,
        const MergeTreeDataPartChecksums & checksums,
        bool is_temp,
        LoggerPtr log);

    /// For names of expected data part files returns the actual names
    /// of files in filesystem to which data of these files is written.
    /// Actual file name may be the same as expected
    /// or be the name of the file with packed data.
    virtual NameSet getActualFileNamesOnDisk(const NameSet & file_names) const = 0;

    /// Returns the destination path for the part directory while copying a detached part.
    String getPartDirForPrefix(const String & prefix, bool detached, int try_no) const;
};

}
