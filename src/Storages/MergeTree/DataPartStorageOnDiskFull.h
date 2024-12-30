#pragma once
#include "Storages/MergeTree/IDataPartStorage.h"
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

namespace DB
{

/// A storage for data part that stores files on filesystem as is.
class DataPartStorageOnDiskFull final : public DataPartStorageOnDiskBase
{
public:
    DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_);
    MergeTreeDataPartStorageType getType() const override { return MergeTreeDataPartStorageType::Full; }

    MutableDataPartStoragePtr getProjection(const std::string & name, bool use_parent_transaction = true) override; // NOLINT
    DataPartStoragePtr getProjection(const std::string & name) const override;

    bool exists() const override;
    bool existsFile(const std::string & name) const override;
    bool existsDirectory(const std::string & name) const override;

    DataPartStorageIteratorPtr iterate() const override;
    Poco::Timestamp getFileLastModified(const String & file_name) const override;
    size_t getFileSize(const std::string & file_name) const override;
    UInt32 getRefCount(const std::string & file_name) const override;
    std::vector<std::string> getRemotePaths(const std::string & file_name) const override;
    String getUniqueId() const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    void createProjection(const std::string & name) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void createFile(const String & name) override;
    void moveFile(const String & from_name, const String & to_name) override;
    void replaceFile(const String & from_name, const String & to_name) override;

    void removeFile(const String & name) override;
    void removeFileIfExists(const String & name) override;

    void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;
    void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) override;

    void beginTransaction() override;
    void commitTransaction() override;
    void precommitTransaction() override {}
    bool hasActiveTransaction() const override { return transaction != nullptr; }

private:
    DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_);
    MutableDataPartStoragePtr create(VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const override;

    NameSet getActualFileNamesOnDisk(const NameSet & file_names) const override { return file_names; }
};

}
