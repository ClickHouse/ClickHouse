#include "IPartMetadataManager.h"

#include <Disks/IVolume.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

IPartMetadataManager::IPartMetadataManager(const IMergeTreeDataPart * part_) : part(part_)
{
}

bool IPartMetadataManager::isCompressedFromFileName(const String & file_name)
{
    std::string extension = fs::path(file_name).extension();
    return (MarkType::isMarkFileExtension(extension) && MarkType(extension).compressed)
        || isCompressedFromIndexExtension(extension);
}

std::unique_ptr<ReadBuffer> IPartMetadataManager::readIfExists(const String & file_name) const
{
    if (exists(file_name))
        return read(file_name);
    else
        return {};
}

}
