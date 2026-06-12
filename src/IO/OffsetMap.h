#pragma once

#include <IO/Rope.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Maps logical file offsets to (object, offset-within-object).
class OffsetMap
{
public:
    struct PhysicalRange
    {
        StoredObject object;
        size_t object_offset = 0;
        size_t size = 0;
    };

    /// Objects are concatenated in their input order to form the logical file.
    void build(const StoredObjects & objects);

    /// A single logical range may span multiple objects.
    VectorWithMemoryTracking<PhysicalRange> map(ByteRange logical_range) const;

    /// Find the object whose range contains `logical_offset` (nullptr at/past
    /// `totalSize`). The optional output is the object's file-level offset.
    const StoredObject * findObjectAt(size_t logical_offset, size_t * object_file_offset = nullptr) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    struct Segment
    {
        StoredObject object;
        size_t object_offset = 0;
        size_t logical_offset = 0;
        size_t size = 0;
    };

    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
