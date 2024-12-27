#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>

#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Common/assert_cast.h>

namespace DB
{

void DiskObjectStorage::wrapWithCache(FileCachePtr cache, const FileCacheSettings & cache_settings, const String & layer_name)
{
    object_storage = std::make_shared<CachedObjectStorage>(object_storage, cache, cache_settings, layer_name);
}

NameSet DiskObjectStorage::getLayersNames() const
{
    NameSet cache_layers;
    auto current_object_storage = object_storage;
    while (current_object_storage != nullptr)
    {
        std::optional<std::string> layer_name = current_object_storage->getLayerName();
        if (layer_name.has_value())
        {
            cache_layers.insert(layer_name.value());
        }
        current_object_storage = current_object_storage->getWrappedObjectStorage();
    }
    return cache_layers;
}

}
