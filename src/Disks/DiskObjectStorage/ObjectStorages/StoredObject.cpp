#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

UInt64 getETagHash(const String & etag)
{
    return etag.empty() ? 0 : sipHash64(etag);
}

size_t getTotalSize(const StoredObjects & objects)
{
    size_t size = 0;
    for (const auto & object : objects)
    {
        /// The total of objects one of which has an unknown size is unknown too, as in `OffsetMap`.
        if (object.bytes_size == StoredObject::UnknownSize)
            return StoredObject::UnknownSize;
        if (common::addOverflow(size, static_cast<size_t>(object.bytes_size), size))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "The total size of stored objects overflows `size_t`");
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
