#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

namespace DB
{

size_t getTotalSize(const StoredObjects & objects)
{
    /// If any object is unknown-size, the total is unknown — propagate the
    /// sentinel rather than overflowing to ~uint64_t::max via a literal add.
    size_t size = 0;
    for (const auto & object : objects)
    {
        if (object.bytes_size == StoredObject::UnknownSize)
            return StoredObject::UnknownSize;
        size += object.bytes_size;
    }
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
