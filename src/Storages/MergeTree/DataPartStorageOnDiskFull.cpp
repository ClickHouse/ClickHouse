#include <base/pathToString.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Disks/IDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_))
{
}

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_), std::move(transaction_))
{
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::create(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, bool /*initialize_*/) const
{
    return std::make_shared<DataPartStorageOnDiskFull>(std::move(volume_), std::move(root_path_), std::move(part_dir_));
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name, bool use_parent_transaction) // NOLINT
{
    /// Not arena-scoped: most callers use this only as a short-lived filesystem handle (CHECK TABLE,
    /// mutation hardlink/copy, existence probes). The part-lifetime projection storage is created via
    /// `getProjectionPartBuilder`, which scopes the arena itself.
    return std::shared_ptr<DataPartStorageOnDiskFull>(new DataPartStorageOnDiskFull(volume, pathToGenericString(fs::path(root_path) / part_dir), name, use_parent_transaction ? transaction : nullptr));
}

DataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDiskFull>(volume, pathToGenericString(fs::path(root_path) / part_dir), name);
}

bool DataPartStorageOnDiskFull::exists() const
{
    return volume->getDisk()->existsDirectory(pathToGenericString(fs::path(root_path) / part_dir));
}

bool DataPartStorageOnDiskFull::existsFileImpl(const std::string & name) const
{
    return volume->getDisk()->existsFile(pathToGenericString(fs::path(root_path) / part_dir / name));
}

bool DataPartStorageOnDiskFull::existsDirectory(const std::string & name) const
{
    return volume->getDisk()->existsDirectory(pathToGenericString(fs::path(root_path) / part_dir / name));
}

class DataPartStorageIteratorOnDisk final : public IDataPartStorageIterator
{
public:
    DataPartStorageIteratorOnDisk(DiskPtr disk_, DirectoryIteratorPtr it_)
        : disk(std::move(disk_)), it(std::move(it_))
    {
    }

    void next() override { it->next(); }
    bool isValid() const override { return it->isValid(); }
    bool isFile() const override { return isValid() && disk->existsFile(it->path()); }
    std::string name() const override { return it->name(); }
    std::string path() const override { return it->path(); }

private:
    DiskPtr disk;
    DirectoryIteratorPtr it;
};

DataPartStorageIteratorPtr DataPartStorageOnDiskFull::iterate() const
{
    return std::make_unique<DataPartStorageIteratorOnDisk>(
        volume->getDisk(),
        volume->getDisk()->iterateDirectory(pathToGenericString(fs::path(root_path) / part_dir)));
}

Poco::Timestamp DataPartStorageOnDiskFull::getFileLastModified(const String & file_name) const
{
    return volume->getDisk()->getLastModified(pathToGenericString(fs::path(root_path) / part_dir / file_name));
}

size_t DataPartStorageOnDiskFull::getFileSizeImpl(const String & file_name) const
{
    return volume->getDisk()->getFileSize(pathToGenericString(fs::path(root_path) / part_dir / file_name));
}

std::optional<UInt64> DataPartStorageOnDiskFull::getPackedFileUncompressedSize(const std::string & file_name) const
{
    if (looksLikePackedSkipIndexFile(file_name))
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(file_name))
            return reader->getFileUncompressedSize(file_name);
    return {};
}

UInt32 DataPartStorageOnDiskFull::getRefCount(const String & file_name) const
{
    return volume->getDisk()->getRefCount(pathToGenericString(fs::path(root_path) / part_dir / file_name));
}

std::vector<std::string> DataPartStorageOnDiskFull::getRemotePaths(const std::string & file_name) const
{
    const std::string path = pathToGenericString(fs::path(root_path) / part_dir / file_name);
    auto objects = volume->getDisk()->getStorageObjects(path);

    std::vector<std::string> remote_paths;
    remote_paths.reserve(objects.size());

    for (const auto & object : objects)
        remote_paths.push_back(object.remote_path);

    return remote_paths;
}

String DataPartStorageOnDiskFull::getUniqueId() const
{
    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk {} doesn't support zero-copy replication", disk->getName());

    return disk->getUniqueId(pathToGenericString(fs::path(getRelativePath()) / "checksums.txt"));
}

void DataPartStorageOnDiskFull::prepareReadImpl(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    volume->getDisk()->prepareRead(pathToGenericString(fs::path(root_path) / part_dir / name), settings, read_hint, pipeline);
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDiskFull::readFileIfExistsImpl(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    return volume->getDisk()->readFileIfExists(pathToGenericString(fs::path(root_path) / part_dir / name), settings, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskFull::writeFile(
    const String & name,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    if (transaction)
        return transaction->writeFile(pathToGenericString(fs::path(root_path) / part_dir / name), buf_size, mode, settings);
    return volume->getDisk()->writeFile(pathToGenericString(fs::path(root_path) / part_dir / name), buf_size, mode, settings);
}

void DataPartStorageOnDiskFull::createFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.createFile(pathToGenericString(fs::path(root_path) / part_dir / name)); });
}

void DataPartStorageOnDiskFull::moveFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.moveFile(pathToGenericString(relative_path / from_name), pathToGenericString(relative_path / to_name));
    });
}

void DataPartStorageOnDiskFull::replaceFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.replaceFile(pathToGenericString(relative_path / from_name), pathToGenericString(relative_path / to_name));
    });
}

void DataPartStorageOnDiskFull::removeFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFile(pathToGenericString(fs::path(root_path) / part_dir / name)); });
}

void DataPartStorageOnDiskFull::removeFileIfExists(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFileIfExists(pathToGenericString(fs::path(root_path) / part_dir / name)); });
}

void DataPartStorageOnDiskFull::createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to)
{
    const auto * source_on_disk = typeid_cast<const DataPartStorageOnDiskFull *>(&source);
    if (!source_on_disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create hardlink from different storage. Expected DataPartStorageOnDiskFull, got {}",
            typeid(source).name());

    executeWriteOperation([&](auto & disk)
    {
        disk.createHardLink(
            pathToGenericString(fs::path(source_on_disk->getRelativePath()) / from),
            pathToGenericString(fs::path(root_path) / part_dir / to));
    });
}

void DataPartStorageOnDiskFull::copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to)
{
    const auto * source_on_disk = typeid_cast<const DataPartStorageOnDiskFull *>(&source);
    if (!source_on_disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create copy file from different storage. Expected DataPartStorageOnDiskFull, got {}",
            typeid(source).name());

    /// Copying files between different disks is
    /// not supported in disk transactions.
    source_on_disk->getDisk()->copyFile(
        pathToGenericString(fs::path(source_on_disk->getRelativePath()) / from),
        *volume->getDisk(),
        pathToGenericString(fs::path(root_path) / part_dir / to),
        getReadSettings());
}

void DataPartStorageOnDiskFull::createProjection(const std::string & name)
{
    executeWriteOperation([&](auto & disk) { disk.createDirectory(pathToGenericString(fs::path(root_path) / part_dir / name)); });
}

void DataPartStorageOnDiskFull::beginTransaction()
{
    if (transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Uncommitted{}transaction already exists", has_shared_transaction ? " shared " : " ");

    transaction = volume->getDisk()->createTransaction();
}

void DataPartStorageOnDiskFull::commitTransaction()
{
    if (!transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    if (has_shared_transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot commit shared transaction");

    transaction->commit();
    transaction.reset();
}

}
