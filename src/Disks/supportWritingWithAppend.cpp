#include <Disks/supportWritingWithAppend.h>

#include <Disks/ObjectStorages/DiskObjectStorage.h>

namespace DB
{
bool supportWritingWithAppend(const DiskPtr & disk)
{
    auto target_disk = disk;
    if (auto delegate_disk = target_disk->getDelegateDiskIfExists())
        target_disk = delegate_disk;

    if (auto * object_storage = dynamic_cast<DiskObjectStorage *>(target_disk.get()))
    {
        if (!object_storage->getMetadataStorage()->supportWritingWithAppend())
            return false;
    }
    return true;
}
}

