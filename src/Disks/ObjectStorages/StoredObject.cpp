#include <Disks/ObjectStorages/StoredObject.h>

namespace DB
{

size_t getTotalSize(const StoredObjects & objects)
{
    size_t size = 0;
    for (const auto & object : objects)
        size += object.bytes_size;
    return size;
}

Strings getRemotePaths(const StoredObjects & objects)
{
    Strings paths;
    paths.reserve(objects.size());
    for (const auto & object : objects)
        paths.push_back(object.remote_path);
    return paths;
}

}
