#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>

#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>

#include <Common/Exception.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

ANNGroupStorageDiskFull::ANNGroupStorageDiskFull(
    VolumePtr volume_,
    std::string relative_group_path_,
    DiskTransactionPtr shared_transaction_)
    : volume(std::move(volume_))
    , relative_group_path(std::move(relative_group_path_))
    , shared_transaction(std::move(shared_transaction_))
{
    if (!volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNGroupStorageDiskFull requires a non-null volume");
}

DiskPtr ANNGroupStorageDiskFull::getDisk() const
{
    return volume->getDisk(0);
}

std::string ANNGroupStorageDiskFull::getFullPath() const
{
    return fs::path(getDisk()->getPath()) / relative_group_path;
}

std::string ANNGroupStorageDiskFull::getGroupDir() const
{
    return fs::path(relative_group_path).filename().string();
}

bool ANNGroupStorageDiskFull::exists() const
{
    return getDisk()->existsDirectory(relative_group_path);
}

bool ANNGroupStorageDiskFull::existsFile(const std::string & name) const
{
    return getDisk()->existsFile(fs::path(relative_group_path) / name);
}

size_t ANNGroupStorageDiskFull::getFileSize(const std::string & name) const
{
    return getDisk()->getFileSize(fs::path(relative_group_path) / name);
}

Poco::Timestamp ANNGroupStorageDiskFull::getFileLastModified(const std::string & name) const
{
    return getDisk()->getLastModified(fs::path(relative_group_path) / name);
}

std::unique_ptr<ReadBufferFromFileBase> ANNGroupStorageDiskFull::readFile(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    return getDisk()->readFile(fs::path(relative_group_path) / name, settings, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> ANNGroupStorageDiskFull::writeFile(
    const std::string & name,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    const auto full_path = fs::path(relative_group_path) / name;
    if (shared_transaction)
        return shared_transaction->writeFile(full_path, buf_size, mode, settings);
    return getDisk()->writeFile(full_path, buf_size, mode, settings);
}

void ANNGroupStorageDiskFull::removeFileIfExists(const std::string & name)
{
    const auto full_path = fs::path(relative_group_path) / name;
    if (shared_transaction)
        shared_transaction->removeFileIfExists(full_path);
    else
        getDisk()->removeFileIfExists(full_path);
}

void ANNGroupStorageDiskFull::renameFile(const std::string & from, const std::string & to)
{
    const auto full_from = fs::path(relative_group_path) / from;
    const auto full_to = fs::path(relative_group_path) / to;
    if (shared_transaction)
        shared_transaction->moveFile(full_from, full_to);
    else
        getDisk()->moveFile(full_from, full_to);
}

void ANNGroupStorageDiskFull::renameDirectoryTo(const std::string & new_relative_path)
{
    if (shared_transaction)
        shared_transaction->moveDirectory(relative_group_path, new_relative_path);
    else
        getDisk()->moveDirectory(relative_group_path, new_relative_path);

    relative_group_path = new_relative_path;
}

}
