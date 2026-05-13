#pragma once

#include <IO/Rope.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <vector>

namespace DB
{

/// Maps logical file offsets to (object, offset-within-object).
/// Replaces ReadBufferFromRemoteFSGather as the offset translator.
class OffsetMap
{
public:
    struct PhysicalRange
    {
        StoredObject object;
        size_t object_offset;
        size_t size;
    };

    /// Build from a list of StoredObjects. Objects are concatenated
    /// in order to form the logical file.
    void build(const StoredObjects & objects);

    /// Map a logical range to physical ranges.
    /// A single logical range may span multiple objects.
    std::vector<PhysicalRange> map(ByteRange logical_range) const;

    size_t totalSize() const { return total_size; }

private:
    struct Segment
    {
        StoredObject object;
        size_t object_offset;
        size_t logical_offset;
        size_t size;
    };

    std::vector<Segment> segments;
    size_t total_size = 0;
};

}
