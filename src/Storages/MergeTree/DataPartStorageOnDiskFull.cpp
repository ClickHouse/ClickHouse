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

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(
    VolumePtr volume_,
    std::string root_path_,
    std::string part_dir_,
    ProjectionStorageFormat projection_storage_format_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_), projection_storage_format_)
{
}

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(
    VolumePtr volume_,
    std::string root_path_,
    std::string part_dir_,
    DiskTransactionPtr transaction_,
    ProjectionStorageFormat projection_storage_format_)
    : DataPartStorageOnDiskBase(
        std::move(volume_), std::move(root_path_), std::move(part_dir_), std::move(transaction_), projection_storage_format_)
{
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::create(
    VolumePtr volume_,
    std::string root_path_,
    std::string part_dir_,
    bool /*initialize_*/,
    ProjectionStorageFormat projection_storage_format_) const
{
    return std::make_shared<DataPartStorageOnDiskFull>(
        std::move(volume_), std::move(root_path_), std::move(part_dir_), projection_storage_format_);
}

namespace
{
    std::pair<std::string, std::string> getProjectionStorageRootAndDir(
        const std::string & root_path,
        const std::string & part_dir,
        const std::string & name,
        IDataPartStorage::ProjectionStorageFormat format)
    {
        switch (format)
        {
            case IDataPartStorage::ProjectionStorageFormat::LEGACY_NESTED:
                return {fs::path(root_path) / part_dir, name};
            case IDataPartStorage::ProjectionStorageFormat::FLAT:
                return {root_path, part_dir + "." + name};
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected projection storage format: {}", static_cast<int>(format));
        }
    }
}

IDataPartStorage::ProjectionStorageFormat DataPartStorageOnDiskFull::detectProjectionAndItsFormat(const std::string & name) const
{
    ProjectionStorageFormat detected = ProjectionStorageFormat::NONE;

    const auto disk = volume->getDisk();

    const auto [proj_root_first, proj_dir_first] = getProjectionStorageRootAndDir(root_path, part_dir, name, projection_storage_format);
    if (disk->existsDirectory(fs::path(proj_root_first) / proj_dir_first))
    {
        /// Probing the configured format first
        detected = projection_storage_format;
    }
    else
    {
        /// Now probing the other remaining format if needed
        ProjectionStorageFormat other = (projection_storage_format == ProjectionStorageFormat::LEGACY_NESTED)
            ? ProjectionStorageFormat::FLAT
            : ProjectionStorageFormat::LEGACY_NESTED;
        const auto [proj_root_other, proj_dir_other] = getProjectionStorageRootAndDir(root_path, part_dir, name, other);
        if (disk->existsDirectory(fs::path(proj_root_other) / proj_dir_other))
            detected = other;
    }

    return detected;
}

bool DataPartStorageOnDiskFull::hasProjection(const std::string & name)
{
    return detectProjectionAndItsFormat(name) != ProjectionStorageFormat::NONE;
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name, bool use_parent_transaction) // NOLINT
{
    ProjectionStorageFormat detected = detectProjectionAndItsFormat(name);

    if (detected == ProjectionStorageFormat::NONE)
        detected = projection_storage_format;

    /// If projection is found - build a handle pointint to it, otherwise - fall through
    auto [proj_root, proj_dir] = getProjectionStorageRootAndDir(root_path, part_dir, name, detected);
    return std::shared_ptr<DataPartStorageOnDiskFull>(new DataPartStorageOnDiskFull(
        volume,
        std::move(proj_root),
        std::move(proj_dir),
        use_parent_transaction ? transaction : nullptr,
        detected));
}

DataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name) const
{
    ProjectionStorageFormat detected = detectProjectionAndItsFormat(name);

    if (detected == ProjectionStorageFormat::NONE)
        detected = projection_storage_format;

    auto [proj_root, proj_dir] = getProjectionStorageRootAndDir(root_path, part_dir, name, detected);
    return std::make_shared<DataPartStorageOnDiskFull>(
            volume,
            std::move(proj_root),
            std::move(proj_dir),
            detected);
}

bool DataPartStorageOnDiskFull::exists() const
{
    return volume->getDisk()->existsDirectory(fs::path(root_path) / part_dir);
}

bool DataPartStorageOnDiskFull::existsFile(const std::string & name) const
{
    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
            return true;
    }
    return volume->getDisk()->existsFile(fs::path(root_path) / part_dir / name);
}

bool DataPartStorageOnDiskFull::existsDirectory(const std::string & name) const
{
    return volume->getDisk()->existsDirectory(fs::path(root_path) / part_dir / name);
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
        volume->getDisk()->iterateDirectory(fs::path(root_path) / part_dir));
}

Poco::Timestamp DataPartStorageOnDiskFull::getFileLastModified(const String & file_name) const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir / file_name);
}

size_t DataPartStorageOnDiskFull::getFileSize(const String & file_name) const
{
    if (looksLikePackedSkipIndexFile(file_name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(file_name))
            return reader->getFileSize(file_name);
    }
    return volume->getDisk()->getFileSize(fs::path(root_path) / part_dir / file_name);
}

UInt32 DataPartStorageOnDiskFull::getRefCount(const String & file_name) const
{
    return volume->getDisk()->getRefCount(fs::path(root_path) / part_dir / file_name);
}

std::vector<std::string> DataPartStorageOnDiskFull::getRemotePaths(const std::string & file_name) const
{
    const std::string path = fs::path(root_path) / part_dir / file_name;
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

    return disk->getUniqueId(fs::path(getRelativePath()) / "checksums.txt");
}

void DataPartStorageOnDiskFull::prepareRead(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
        {
            /// Packed substreams skip the disk's normal pipeline (filesystem cache,
            /// async prefetch, etc.) and read through PackedFilesReader::readFile, which
            /// opens the archive via the underlying disk and wraps the result with
            /// ReadBufferFromFileView at the right offset. The archive's current location is
            /// captured here and passed in, so the reader holds no path of its own.
            auto disk = volume->getDisk();
            String archive_path = fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME);
            ReadPipeline::BufferCreator creator =
                [reader, disk, archive_path, name, read_hint](const StoredObject &, const ReadSettings & s, bool, bool)
                {
                    return reader->readFile(disk, archive_path, name, s, read_hint);
                };
            pipeline.setSource(std::move(creator), StoredObjects{StoredObject{}}, settings);
            return;
        }
    }
    volume->getDisk()->prepareRead(fs::path(root_path) / part_dir / name, settings, read_hint, pipeline);
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDiskFull::readFileIfExists(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
            return reader->readFile(
                volume->getDisk(),
                fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME),
                name, settings, read_hint);
    }
    return volume->getDisk()->readFileIfExists(fs::path(root_path) / part_dir / name, settings, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskFull::writeFile(
    const String & name,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    if (transaction)
        return transaction->writeFile(fs::path(root_path) / part_dir / name, buf_size, mode, settings);
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / name, buf_size, mode, settings);
}

void DataPartStorageOnDiskFull::createFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.createFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::moveFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.moveFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDiskFull::replaceFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.replaceFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDiskFull::removeFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::removeFileIfExists(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFileIfExists(fs::path(root_path) / part_dir / name); });
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
            fs::path(source_on_disk->getRelativePath()) / from,
            fs::path(root_path) / part_dir / to);
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
        fs::path(source_on_disk->getRelativePath()) / from,
        *volume->getDisk(),
        fs::path(root_path) / part_dir / to,
        getReadSettings());
}

void DataPartStorageOnDiskFull::createProjection(const std::string & name)
{
    /// Always write at the storage's configured layout. No probing.
    const auto [proj_root, proj_dir] = getProjectionStorageRootAndDir(root_path, part_dir, name, projection_storage_format);
    executeWriteOperation([&](auto & disk) { disk.createDirectory(fs::path(proj_root) / proj_dir); });
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
