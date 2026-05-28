#pragma once

#include <IO/Rope.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <vector>
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
        size_t object_offset;
        size_t size;
    };

    /// Objects are concatenated in their input order to form the logical file.
    void build(const StoredObjects & objects);

    /// A single logical range may span multiple objects.
    VectorWithMemoryTracking<PhysicalRange> map(ByteRange logical_range) const;

    /// Find the object whose range contains `logical_offset`. Returns nullptr
    /// if `logical_offset` is at or past `totalSize()`. The optional output
    /// `object_file_offset` (the segment's `logical_offset`) lets callers
    /// translate between object-local and file-level coordinates without
    /// re-walking the segment list.
    const StoredObject * findObjectAt(size_t logical_offset, size_t * object_file_offset = nullptr) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    struct Segment
    {
        StoredObject object;
        size_t object_offset;
        size_t logical_offset;
        size_t size;
    };

    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
