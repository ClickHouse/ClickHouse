#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/ObjectStorages/Cached/FileCachesHolder.h>

#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Common/assert_cast.h>

namespace DB
{

void DiskObjectStorage::wrapWithCache(FileCachesHolder && holder, const String & layer_name)
{
    object_storage = std::make_shared<CachedObjectStorage>(object_storage, std::move(holder), layer_name);
}

NameSet DiskObjectStorage::getCacheLayersNames() const
{
    NameSet cache_layers;
    auto current_object_storage = object_storage;
    while (current_object_storage->supportsCache())
    {
        auto * cached_object_storage = assert_cast<CachedObjectStorage *>(current_object_storage.get());
        cache_layers.insert(cached_object_storage->getCacheConfigName());
        current_object_storage = cached_object_storage->getWrappedObjectStorage();
    }
    return cache_layers;
}

}
