#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/Replication/Location.h>

namespace DB
{

class ObjectStorageRouter
{
public:
    explicit ObjectStorageRouter(std::unordered_map<Location, ObjectStoragePtr> object_storages_);

    ObjectStoragePtr takePointingTo(const Location & location) const;
    std::unordered_map<Location, ObjectStoragePtr> getRegistry() const;

private:
    std::unordered_map<Location, ObjectStoragePtr> object_storages;
};

using ObjectStorageRouterPtr = std::shared_ptr<ObjectStorageRouter>;

}
