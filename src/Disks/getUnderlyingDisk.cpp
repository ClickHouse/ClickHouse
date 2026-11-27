#include <Disks/getUnderlyingDisk.h>

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>


namespace DB
{

std::shared_ptr<const IDisk> getUnderlyingDisk(const DiskPtr & disk)
{
    std::shared_ptr<const IDisk> underlying = disk;

    while (auto delegate = underlying->getDelegateDiskIfExists())
        underlying = delegate;

    for (auto layer = std::dynamic_pointer_cast<const DiskObjectStorage>(underlying); layer; layer = layer->getWrappedDisk())
        underlying = layer;

    return underlying;
}

}
