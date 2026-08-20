#pragma once

#include <IO/ChainedBuffers.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Maps FILE offsets (the executor's physical, header-inclusive space) to (object,
/// offset-within-object). A "file" is the concatenation of its stored objects. Encryption is
/// invisible here. The payload/header split (logical vs physical) is the executor's business, above
/// this map.
class OffsetMap
{
public:
    struct ObjectRange
    {
        StoredObject object;
        size_t object_offset = 0;
        size_t size = 0;
    };

    /// Objects are concatenated in their input order to form the file.
    void build(const StoredObjects & objects);

    /// A single file range may span multiple objects.
    VectorWithMemoryTracking<ObjectRange> map(ByteRange file_range) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    struct Segment
    {
        StoredObject object;
        size_t object_offset = 0;
        size_t file_offset = 0;
        size_t size = 0;
    };

    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
