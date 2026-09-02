#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>

namespace DB
{

ObjectStorageRouter::ObjectStorageRouter(std::unordered_map<Location, ObjectStoragePtr> object_storages_)
    : object_storages(std::move(object_storages_))
{
}

ObjectStoragePtr ObjectStorageRouter::takePointingTo(const Location & location) const
{
    return object_storages.at(location);
}

std::unordered_map<Location, ObjectStoragePtr> ObjectStorageRouter::getRegistry() const
{
    return object_storages;
}

}
