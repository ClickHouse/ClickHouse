#include <Disks/ObjectStorages/StoredObject.h>

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>


namespace DB
{

StoredObject::StoredObject(
    const std::string & absolute_path_,
    uint64_t bytes_size_,
    const std::string & mapped_path_,
    const std::string & cache_path_)
    : absolute_path(absolute_path_)
    , mapped_path(mapped_path_)
    , cache_path(cache_path_)
    , bytes_size(bytes_size_)
{
}

std::string StoredObject::getPathKeyForCache() const
{
    return cache_path;
}

const std::string & StoredObject::getMappedPath() const
{
    return mapped_path;
}

StoredObject StoredObject::create(
    const IObjectStorage & object_storage,
    const std::string & object_path,
    size_t object_size,
    const std::string & mapped_path)
{
    const auto cache_path = object_storage.getUniqueId(object_path);
    return StoredObject(object_path, object_size, mapped_path, cache_path);
}

}
