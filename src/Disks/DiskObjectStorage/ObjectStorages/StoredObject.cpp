#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Common/SipHash.h>

namespace DB
{

UInt64 getETagHash(const String & etag)
{
    return etag.empty() ? 0 : sipHash64(etag);
}

UInt64 getImmutableContentIdHash(const String & immutable_content_id)
{
    return immutable_content_id.empty() ? 0 : sipHash64(immutable_content_id);
}

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
