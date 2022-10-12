#include <Disks/ObjectStorages/StoredObject.h>

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>


namespace DB
{

StoredObject::StoredObject(
    const std::string & absolute_path_,
    uint64_t bytes_size_,
    PathKeyForCacheCreator && path_key_for_cache_creator_)
    : absolute_path(absolute_path_)
    , bytes_size(bytes_size_)
    , path_key_for_cache_creator(std::move(path_key_for_cache_creator_))
{
}

std::string StoredObject::getPathKeyForCache() const
{
    if (!path_key_for_cache_creator)
        return ""; /// This empty result need to be used with care.

    return path_key_for_cache_creator(absolute_path);
}

StoredObject StoredObject::create(
    const IObjectStorage & object_storage, const std::string & object_path, size_t object_size, bool object_bypasses_cache)
{
    if (object_bypasses_cache)
        return StoredObject(object_path, object_size, {});

    auto path_key_for_cache_creator = [&object_storage](const std::string & path) -> String
    {
        try
        {
            return object_storage.getUniqueId(path);
        }
        catch (...)
        {
            LOG_DEBUG(
               &Poco::Logger::get("StoredObject"),
                "Object does not exist while getting cache path hint (object path: {})",
                path);

            return "";
        }
    };

    return StoredObject(object_path, object_size, std::move(path_key_for_cache_creator));
}

}
