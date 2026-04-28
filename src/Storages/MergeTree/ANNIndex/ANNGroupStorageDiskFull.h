#pragma once

#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Disks/IVolume.h>

namespace DB
{

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

/// The single terminal implementation of `IANNGroupStorage`.
///
/// This is the **only** file in the ANN module that is allowed to call `IDisk::*` or
/// `IDiskTransaction::*` directly. When `shared_transaction` is non-null, all write
/// operations are routed through that transaction so that they can be committed or
/// rolled back atomically alongside the table-level manifest update.
class ANNGroupStorageDiskFull final : public IANNGroupStorage
{
public:
    ANNGroupStorageDiskFull(
        VolumePtr volume_,
        std::string relative_group_path_,
        DiskTransactionPtr shared_transaction_ = nullptr);

    std::string getFullPath() const override;
    std::string getRelativePath() const override { return relative_group_path; }
    std::string getGroupDir() const override;

    bool exists() const override;
    bool existsFile(const std::string & name) const override;

    size_t getFileSize(const std::string & name) const override;
    Poco::Timestamp getFileLastModified(const std::string & name) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void removeFileIfExists(const std::string & name) override;
    void renameFile(const std::string & from, const std::string & to) override;
    void renameDirectoryTo(const std::string & new_relative_path) override;

private:
    VolumePtr volume;
    std::string relative_group_path;
    DiskTransactionPtr shared_transaction;

    DiskPtr getDisk() const;
};

}
