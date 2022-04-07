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
}

DataPartStorageOnDisk::DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_)
    : volume(std::move(volume_)), root_path(std::move(root_path_))
{   
}

std::string DataPartStorageOnDisk::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path;
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDisk::readFile(
    const std::string & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return volume->getDisk()->readFile(fs::path(root_path) / path, settings, read_hint, file_size);
}

bool DataPartStorageOnDisk::exists(const std::string & path) const
{
    return volume->getDisk()->exists(fs::path(root_path) / path);
}

bool DataPartStorageOnDisk::exists() const
{
    return volume->getDisk()->exists(root_path);
}

size_t DataPartStorageOnDisk::getFileSize(const String & path) const
{
    return volume->getDisk()->getFileSize(fs::path(root_path) / path);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterate() const
{
    return volume->getDisk()->iterateDirectory(root_path);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterateDirectory(const String & path) const
{
    return volume->getDisk()->iterateDirectory(fs::path(root_path) / path);
}

DataPartStoragePtr DataPartStorageOnDisk::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDisk>(volume, fs::path(root_path) / name);
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
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), root_path);
}

void DataPartStorageOnDisk::writeChecksums(MergeTreeDataPartChecksums & checksums) const
{
    std::string path = fs::path(root_path) / "checksums.txt";

    {
        auto out = volume->getDisk()->writeFile(path + ".tmp", 4096);
        checksums.write(*out);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

void DataPartStorageOnDisk::writeColumns(NamesAndTypesList & columns) const
{
    std::string path = fs::path(root_path) / "columns.txt";

    {
        auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
        columns.writeText(*buf);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

void DataPartStorageOnDisk::rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync)
{
    if (!volume->getDisk()->exists(root_path))
        throw Exception("Part directory " + fullPath(volume->getDisk(), root_path) + " doesn't exist. Most likely it is a logical error.", ErrorCodes::FILE_DOESNT_EXIST);

    /// Why?
    String to = fs::path(new_relative_path) / "";

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            LOG_WARNING(log, "Part directory {} already exists and contains {} files. Removing it.", fullPath(volume->getDisk(), to), files.size());

            volume->getDisk()->removeRecursive(to);
        }
        else
        {
            throw Exception("Part directory " + fullPath(volume->getDisk(), to) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    // metadata_manager->deleteAll(true);
    // metadata_manager->assertAllDeleted(true);

    /// Why?
    volume->getDisk()->setLastModified(root_path, Poco::Timestamp::fromEpochTime(time(nullptr)));
    volume->getDisk()->moveDirectory(root_path, to);
    root_path = new_relative_path;
    // metadata_manager->updateAll(true);

    SyncGuardPtr sync_guard;
    if (fsync)
        sync_guard = volume->getDisk()->getDirectorySyncGuard(root_path);
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

}
