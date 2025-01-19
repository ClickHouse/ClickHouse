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

String remotePathsAndBytesSizesToString(const StoredObjects & objects)
{
    String str;
    for (const auto & object : objects)
    {
        if (!str.empty())
            str += ", ";
        str += object.remote_path;
        str += ":";
        str += std::to_string(object.bytes_size);
    }
    return str;
}

}
