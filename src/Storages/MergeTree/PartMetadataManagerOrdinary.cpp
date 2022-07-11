#include "PartMetadataManagerOrdinary.h"

#include <IO/ReadBufferFromFileBase.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path)
{
    size_t file_size = disk->getFileSize(path);
    return disk->readFile(path, ReadSettings().adjustBufferSize(file_size), file_size);
}

PartMetadataManagerOrdinary::PartMetadataManagerOrdinary(const IMergeTreeDataPart * part_) : IPartMetadataManager(part_)
{
}


std::unique_ptr<SeekableReadBuffer> PartMetadataManagerOrdinary::read(const String & file_name) const
{
    String file_path = fs::path(part->getFullRelativePath()) / file_name;
    return openForReading(disk, file_path);
}

bool PartMetadataManagerOrdinary::exists(const String & file_name) const
{
    return disk->exists(fs::path(part->getFullRelativePath()) / file_name);
}


}
