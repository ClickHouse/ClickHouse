#include <Storages/MergeTree/DataPartStorageOnDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Disks/IVolume.h>
#include <IO/WriteBufferFromFileBase.h>
#include <base/logger_useful.h>
#include <Disks/IStoragePolicy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NOT_ENOUGH_SPACE;
}

DataPartStorageOnDisk::DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), part_dir(std::move(part_dir_))
{   
}

std::string DataPartStorageOnDisk::getFullRelativePath() const
{
    return fs::path(root_path) / part_dir;
}

std::string DataPartStorageOnDisk::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / part_dir;
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDisk::readFile(
    const std::string & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return volume->getDisk()->readFile(fs::path(root_path) / part_dir / path, settings, read_hint, file_size);
}

bool DataPartStorageOnDisk::exists(const std::string & path) const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir / path);
}

bool DataPartStorageOnDisk::exists() const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir);
}

Poco::Timestamp DataPartStorageOnDisk::getLastModified() const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir);
}

size_t DataPartStorageOnDisk::getFileSize(const String & path) const
{
    return volume->getDisk()->getFileSize(fs::path(root_path) / part_dir / path);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterate() const
{
    return volume->getDisk()->iterateDirectory(fs::path(root_path) / part_dir);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterateDirectory(const String & path) const
{
    return volume->getDisk()->iterateDirectory(fs::path(root_path) / part_dir / path);
}

DataPartStoragePtr DataPartStorageOnDisk::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDisk>(volume, std::string(fs::path(root_path) / part_dir), name);
}

static UInt64 calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from)
{
    if (disk->isFile(from))
        return disk->getFileSize(from);
    std::vector<std::string> files;
    disk->listFiles(from, files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDiskImpl(disk, fs::path(from) / file);
    return res;
}

UInt64 DataPartStorageOnDisk::calculateTotalSizeOnDisk() const
{
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), fs::path(root_path) / part_dir);
}

bool DataPartStorageOnDisk::isStoredOnRemoteDisk() const
{
    return volume->getDisk()->isRemote();
}

bool DataPartStorageOnDisk::supportZeroCopyReplication() const
{
    return volume->getDisk()->supportZeroCopyReplication();
}

void DataPartStorageOnDisk::writeChecksums(MergeTreeDataPartChecksums & checksums) const
{
    std::string path = fs::path(root_path) / part_dir / "checksums.txt";

    {
        auto out = volume->getDisk()->writeFile(path + ".tmp", 4096);
        checksums.write(*out);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

void DataPartStorageOnDisk::writeColumns(NamesAndTypesList & columns) const
{
    std::string path = fs::path(root_path) / part_dir / "columns.txt";

    {
        auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
        columns.writeText(*buf);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

void DataPartStorageOnDisk::writeDeleteOnDestroyMarker(Poco::Logger * log) const
{
    String marker_path = fs::path(root_path) / part_dir / "delete-on-destroy.txt";
    auto disk = volume->getDisk();
    try
    {
        volume->getDisk()->createFile(marker_path);
    }
    catch (Poco::Exception & e)
    {
        LOG_ERROR(log, "{} (while creating DeleteOnDestroy marker: {})", e.what(), backQuote(fullPath(disk, marker_path)));
    }
}

void DataPartStorageOnDisk::rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync)
{
    if (!exists())
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "Part directory {} doesn't exist. Most likely it is a logical error.",
            std::string(fs::path(volume->getDisk()->getPath()) / root_path / part_dir));

    /// Why "" ?
    String to = fs::path(root_path) / new_relative_path / "";

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            LOG_WARNING(log, 
                "Part directory {} already exists and contains {} files. Removing it.", 
                fullPath(volume->getDisk(), to), files.size());

            volume->getDisk()->removeRecursive(to);
        }
        else
        {
            throw Exception(
                ErrorCodes::DIRECTORY_ALREADY_EXISTS, 
                "Part directory {} already exists", 
                fullPath(volume->getDisk(), to));
        }
    }

    // metadata_manager->deleteAll(true);
    // metadata_manager->assertAllDeleted(true);

    String from = getFullRelativePath();

    /// Why?
    volume->getDisk()->setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
    volume->getDisk()->moveDirectory(from, to);
    part_dir = new_relative_path;
    // metadata_manager->updateAll(true);

    SyncGuardPtr sync_guard;
    if (fsync)
        sync_guard = volume->getDisk()->getDirectorySyncGuard(getFullRelativePath());
}

bool DataPartStorageOnDisk::shallParticipateInMerges(const IStoragePolicy & storage_policy) const
{
    /// `IMergeTreeDataPart::volume` describes space where current part belongs, and holds
    /// `SingleDiskVolume` object which does not contain up-to-date settings of corresponding volume.
    /// Therefore we shall obtain volume from storage policy.
    auto volume_ptr = storage_policy.getVolume(storage_policy.getVolumeIndexByDisk(volume->getDisk()));

    return !volume_ptr->areMergesAvoided();
}

std::string DataPartStorageOnDisk::getName() const
{
    return volume->getDisk()->getName();
}


DataPartStorageBuilderOnDisk::DataPartStorageBuilderOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), part_dir(std::move(part_dir_))
{   
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageBuilderOnDisk::readFile(
    const std::string & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return volume->getDisk()->readFile(fs::path(root_path) / part_dir / path, settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageBuilderOnDisk::writeFile(
    const String & path,
    size_t buf_size)
{
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / path, buf_size);
}

void DataPartStorageBuilderOnDisk::removeFile(const String & path)
{
    return volume->getDisk()->removeFile(fs::path(root_path) / part_dir / path);
}

void DataPartStorageBuilderOnDisk::removeRecursive()
{
    volume->getDisk()->removeRecursive(fs::path(root_path) / part_dir);
}

bool DataPartStorageBuilderOnDisk::exists() const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir);
}


bool DataPartStorageBuilderOnDisk::exists(const std::string & path) const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir / path);
}

std::string DataPartStorageBuilderOnDisk::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / part_dir;
}

void DataPartStorageBuilderOnDisk::createDirectories()
{
    return volume->getDisk()->createDirectories(fs::path(root_path) / part_dir);
}

ReservationPtr DataPartStorageBuilderOnDisk::reserve(UInt64 bytes)
{
    auto res = volume->reserve(bytes);
    if (!res)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(bytes));

    return res;
}

DataPartStorageBuilderPtr DataPartStorageBuilderOnDisk::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageBuilderOnDisk>(volume, std::string(fs::path(root_path) / part_dir), name);
}

}
