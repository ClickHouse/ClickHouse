#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Maps file offsets to (object, offset-within-object), abstracting many storage objects behind
/// a single file. Offsets are in the concatenated-file space (payload/header split is the caller's).
class OffsetMap
{
public:
    /// One object's placement in the file: `file_offset` is its start, `size` its length.
    struct Segment
    {
        StoredObject object;
        size_t file_offset = 0;
        size_t size = 0;
    };

    void build(const StoredObjects & objects);

    /// The segment containing `file_offset`, or nullptr if at or past `totalSize`.
    const Segment * findObjectAt(size_t file_offset) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
