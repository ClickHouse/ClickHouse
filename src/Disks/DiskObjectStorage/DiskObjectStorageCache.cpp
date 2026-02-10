#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>

#include <Interpreters/Context.h>

#include <Common/assert_cast.h>

namespace DB
{

/// TODO: This is crap, we need to reimplement cache disk, it is too bad :(
DiskObjectStoragePtr DiskObjectStorage::wrapWithCache(FileCachePtr cache, const FileCacheSettings & cache_settings, const String & layer_name)
{
    auto registry = object_storages->getRegistry();
    auto local_location = cluster->getLocalLocation();
    registry[local_location] = std::make_shared<CachedObjectStorage>(registry[local_location], cache, cache_settings, layer_name);

    return std::make_shared<DiskObjectStorage>(
        getName(),
        std::make_shared<ClusterConfiguration>(cluster->getConfiguration()),
        std::make_shared<MetadataStorageFromCacheObjectStorage>(metadata_storage),
        std::make_shared<ObjectStorageRouter>(std::move(registry)),
        Context::getGlobalContextInstance()->getConfigRef(),
        "storage_configuration.disks." + name,
        use_fake_transaction);
}

NameSet DiskObjectStorage::getCacheLayersNames() const
{
    NameSet cache_layers;
    auto current_object_storage = object_storages->takePointingTo(cluster->getLocalLocation());
    while (current_object_storage->supportsCache())
    {
        auto * cached_object_storage = assert_cast<CachedObjectStorage *>(current_object_storage.get());
        cache_layers.insert(cached_object_storage->getCacheConfigName());
        current_object_storage = cached_object_storage->getWrappedObjectStorage();
    }
    return cache_layers;
}

}
