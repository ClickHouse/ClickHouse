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

    void loadVersionMetadata(VersionMetadata & version, Poco::Logger * log) const override;
    void checkConsistency(const MergeTreeDataPartChecksums & checksums) const override;

    void checkAndFixMetadataConsistency() const override;

    void remove(
        bool can_remove_shared_data,
        const NameSet & names_not_to_remove,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        Poco::Logger * log) const override;

    std::string getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached) const override;

    void setRelativePath(const std::string & path) override;
    void onRename(const std::string & new_root_path, const std::string & new_part_dir) override;

    std::string getDiskName() const override;
    std::string getDiskType() const override;
    bool isStoredOnRemoteDisk() const override;
    bool supportZeroCopyReplication() const override;
    bool supportParallelWrite() const override;
    bool isBroken() const override;
    void syncRevision(UInt64 revision) override;
    UInt64 getRevision() const override;
    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & paths) const override;
    std::string getDiskPath() const override;

    DisksSet::const_iterator isStoredOnDisk(const DisksSet & disks) const override;

    ReservationPtr reserve(UInt64 bytes) const override;
    ReservationPtr tryReserve(UInt64 bytes) const override;
    size_t getVolumeIndex(const IStoragePolicy &) const override;

    void writeChecksums(const MergeTreeDataPartChecksums & checksums, const WriteSettings & settings) const override;
    void writeColumns(const NamesAndTypesList & columns, const WriteSettings & settings) const override;
    void writeVersionMetadata(const VersionMetadata & version, bool fsync_part_dir) const override;
    void appendCSNToVersionMetadata(const VersionMetadata & version, VersionMetadata::WhichCSN which_csn) const override;
    void appendRemovalTIDToVersionMetadata(const VersionMetadata & version, bool clear) const override;
    void writeDeleteOnDestroyMarker(Poco::Logger * log) const override;
    void removeDeleteOnDestroyMarker() const override;
    void removeVersionMetadata() const override;

    String getUniqueId() const override;

    bool shallParticipateInMerges(const IStoragePolicy &) const override;

    void backup(
        TemporaryFilesOnDisks & temp_dirs,
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        BackupEntries & backup_entries) const override;

    DataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        bool make_source_readonly,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        bool copy_instead_of_hardlink) const override;

    DataPartStoragePtr clone(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & disk,
        Poco::Logger * log) const override;

    void changeRootPath(const std::string & from_root, const std::string & to_root) override;

    DataPartStorageBuilderPtr getBuilder() const override;
private:
    VolumePtr volume;
    std::string root_path;
    std::string part_dir;

    void clearDirectory(
        const std::string & dir,
        bool can_remove_shared_data,
        const NameSet & names_not_to_remove,
        const MergeTreeDataPartChecksums & checksums,
        const std::unordered_set<String> & skip_directories,
        Poco::Logger * log,
        bool is_projection) const;
};

class DataPartStorageBuilderOnDisk final : public IDataPartStorageBuilder
{
public:
    DataPartStorageBuilderOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    void setRelativePath(const std::string & path) override;

    bool exists() const override;

    void createDirectories() override;
    void createProjection(const std::string & name) override;

    std::string getPartDirectory() const override { return part_dir; }
    std::string getFullPath() const override;
    std::string getRelativePath() const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        const WriteSettings & settings) override;

    void removeFile(const String & name) override;
    void removeFileIfExists(const String & name) override;
    void removeRecursive() override;
    void removeSharedRecursive(bool keep_in_remote_fs) override;

    SyncGuardPtr getDirectorySyncGuard() const override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) const override;

    ReservationPtr reserve(UInt64 bytes) override;

    DataPartStorageBuilderPtr getProjection(const std::string & name) const override;

    DataPartStoragePtr getStorage() const override;

    void rename(
        const std::string & new_root_path,
        const std::string & new_part_dir,
        Poco::Logger * log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) override;

    void commit() override;

private:
    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
    DiskTransactionPtr transaction;
};

}
