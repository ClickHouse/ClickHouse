#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>

#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/Encrypted/EncryptedObjectStorage.h>

#include <Common/assert_cast.h>

namespace DB
{

void DiskObjectStorage::wrapWithCache(FileCachePtr cache, const FileCacheSettings & cache_settings, const String & layer_name)
{
    object_storage = std::make_shared<CachedObjectStorage>(object_storage, cache, cache_settings, layer_name);
}

void DiskObjectStorage::wrapWithEncryption(EncryptedObjectStorageSettingsPtr enc_settings, const String & layer_name)
{
    object_storage = std::make_shared<EncryptedObjectStorage>(object_storage, enc_settings, layer_name);
}

NameSet DiskObjectStorage::getOverlaysNames() const
{
    NameSet layers;
    auto current_object_storage = object_storage;
    while (current_object_storage->supportsOverlays())
    {
        layers.insert(current_object_storage->getLayerName());
        current_object_storage = current_object_storage->getWrappedObjectStorage();
    }
    return layers;
}

}
