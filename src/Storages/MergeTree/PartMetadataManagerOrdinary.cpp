#include "PartMetadataManagerOrdinary.h"

#include <IO/ReadBufferFromFileBase.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DataPartStoragePtr & data_part_storage, const String & path)
{
    size_t file_size = data_part_storage->getFileSize(path);
    return data_part_storage->readFile(path, ReadSettings().adjustBufferSize(file_size), file_size, std::nullopt);
}

PartMetadataManagerOrdinary::PartMetadataManagerOrdinary(const IMergeTreeDataPart * part_) : IPartMetadataManager(part_)
{
}


std::unique_ptr<SeekableReadBuffer> PartMetadataManagerOrdinary::read(const String & file_name) const
{
    return openForReading(part->data_part_storage, file_name);
}

bool PartMetadataManagerOrdinary::exists(const String & file_name) const
{
    return part->data_part_storage->exists(file_name);
}


}
