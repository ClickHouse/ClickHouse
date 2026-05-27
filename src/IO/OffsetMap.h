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
    std::vector<PhysicalRange> map(ByteRange logical_range) const;  // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Find the object whose range contains `logical_offset`. Returns nullptr
    /// if `logical_offset` is at or past `totalSize()`. The optional output
    /// `object_file_offset` (the segment's `logical_offset`) lets callers
    /// translate between object-local and file-level coordinates without
    /// re-walking the segment list.
    const StoredObject * findObjectAt(size_t logical_offset, size_t * object_file_offset = nullptr) const;

    size_t totalSize() const { return total_size; }

    /// True iff the single stored object had `bytes_size == UnknownSize`
    /// at `build` time. Consumers (`ReaderExecutor::readNextWindow`) use
    /// this to switch to streaming-until-EOF behaviour instead of
    /// trusting `totalSize` as the EOF marker.
    bool hasUnknownSize() const { return has_unknown_size; }

private:
    struct Segment
    {
        StoredObject object;
        size_t object_offset;
        size_t logical_offset;
        size_t size;
    };

    std::vector<Segment> segments;  // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_size = 0;
    bool has_unknown_size = false;
};

}
