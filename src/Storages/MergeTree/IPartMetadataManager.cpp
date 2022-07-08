#include "IPartMetadataManager.h"

#include <Disks/IVolume.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{
IPartMetadataManager::IPartMetadataManager(const IMergeTreeDataPart * part_) : part(part_)
{
}
}
