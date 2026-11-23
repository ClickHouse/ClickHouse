#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

namespace DB
{

size_t getTotalSize(const StoredObjects & objects)
{
    size_t size = 0;
    for (const auto & object : objects)
        size += object.bytes_size;
    return size;
}

Strings collectRemotePaths(const StoredObjects & objects)
{
    Strings remote_paths;
    remote_paths.reserve(objects.size());
    for (const auto & object : objects)
        remote_paths.push_back(object.remote_path);
    return remote_paths;
}

}
