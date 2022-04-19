#pragma once
#include <Storages/MergeTree/IDataPartStorage.h>
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

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    bool exists() const override;
    bool exists(const std::string & path) const override;

    Poco::Timestamp getLastModified() const override;

    size_t getFileSize(const std::string & path) const override;

    DiskDirectoryIteratorPtr iterate() const override;
    DiskDirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    void remove(
        bool keep_shared_data,
        const MergeTreeDataPartChecksums & checksums, 
        std::list<ProjectionChecksums> projections,
        Poco::Logger * log) const override;

    void setRelativePath(const std::string & path) override;

    std::string getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached) const override;

    std::string getRelativePath() const override { return part_dir; }
    std::string getFullPath() const override;
    std::string getFullRootPath() const override;
    std::string getFullRelativePath() const override;

    UInt64 calculateTotalSizeOnDisk() const override;

    bool isStoredOnRemoteDisk() const override;
    bool supportZeroCopyReplication() const override;
    bool isBroken() const override;
    std::string getDiskPathForLogs() const override;

    void writeChecksums(MergeTreeDataPartChecksums & checksums) const override;
    void writeColumns(NamesAndTypesList & columns) const override;
    void writeDeleteOnDestroyMarker(Poco::Logger * log) const override;

    void checkConsistency(const MergeTreeDataPartChecksums & checksums) const override;

    ReservationPtr reserve(UInt64 bytes) override;

    String getUniqueId() const override;

    bool shallParticipateInMerges(const IStoragePolicy &) const override;

    void backup(
        TemporaryFilesOnDisks & temp_dirs,
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        BackupEntries & backup_entries) const override;

    DataPartStoragePtr freeze(
        const std::string & to,
        const std::string & dir_path,
        std::function<void(const DiskPtr &)> save_metadata_callback) const override;

    DataPartStoragePtr clone(
        const std::string & to,
        const std::string & dir_path,
        Poco::Logger * log) const override;

    void rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync) override;

    std::string getName() const override;

    DataPartStoragePtr getProjection(const std::string & name) const override;

private:
    VolumePtr volume;
    std::string root_path;
    std::string part_dir;

    void clearDirectory(
        const std::string & dir,
        bool keep_shared_data, 
        const MergeTreeDataPartChecksums & checksums, 
        const std::unordered_set<String> & skip_directories, 
        Poco::Logger * log,
        bool is_projection) const;
};

class DataPartStorageBuilderOnDisk final : public IDataPartStorageBuilder
{
    DataPartStorageBuilderOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_);

    void setRelativePath(const std::string & path) override;

    bool exists() const override;
    bool exists(const std::string & path) const override;

    void createDirectories() override;

    std::string getRelativePath() const override { return part_dir; }
    std::string getFullPath() const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size) override;

    void removeFile(const String & path) override;
    void removeRecursive() override;

    ReservationPtr reserve(UInt64 bytes) override;

    DataPartStorageBuilderPtr getProjection(const std::string & name) const override;

    DataPartStoragePtr getStorage() const override;

private:
    VolumePtr volume;
    std::string root_path;
    std::string part_dir;
};

}
