#include <Storages/MergeTree/DataPartStorageOnDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Disks/IVolume.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

DataPartStorageOnDisk::DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_, std::string relative_root_path_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), relative_root_path(std::move(relative_root_path_))
{   
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDisk::readFile(
    const std::string & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return volume->getDisk()->readFile(fs::path(relative_root_path) / path, settings, read_hint, file_size);
}

bool DataPartStorageOnDisk::exists(const std::string & path) const
{
    return volume->getDisk()->exists(fs::path(relative_root_path) / path);
}

bool DataPartStorageOnDisk::exists() const
{
    return volume->getDisk()->exists(relative_root_path);
}

size_t DataPartStorageOnDisk::getFileSize(const String & path) const
{
    return volume->getDisk()->getFileSize(fs::path(relative_root_path) / path);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterate() const
{
    return volume->getDisk()->iterateDirectory(relative_root_path);
}

DiskDirectoryIteratorPtr DataPartStorageOnDisk::iterateDirectory(const String & path) const
{
    return volume->getDisk()->iterateDirectory(fs::path(relative_root_path) / path);
}

DataPartStoragePtr DataPartStorageOnDisk::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDisk>(volume, fs::path(relative_root_path) / name);
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
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), relative_root_path);
}

void DataPartStorageOnDisk::writeChecksums(MergeTreeDataPartChecksums & checksums) const
{
    std::string path = fs::path(relative_root_path) / "checksums.txt";

    {
        auto out = volume->getDisk()->writeFile(path + ".tmp", 4096);
        checksums.write(*out);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

void DataPartStorageOnDisk::writeColumns(NamesAndTypesList & columns) const
{
    std::string path = fs::path(relative_root_path) / "columns.txt";

    {
        auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
        columns.writeText(*buf);
    }

    volume->getDisk()->moveFile(path + ".tmp", path);
}

std::string DataPartStorageOnDisk::getName() const
{
    return volume->getDisk()->getName();
}

}
