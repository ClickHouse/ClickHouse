#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Maps logical file offsets to (object, offset-within-object).
/// Used to abstract many storage objects behind a single logical file.
class OffsetMap
{
public:
    /// One object's placement in the concatenated file: `logical_offset` is the object's start
    /// offset in the file, `size` its byte length.
    struct Segment
    {
        StoredObject object;
        size_t logical_offset = 0;
        size_t size = 0;
    };

    /// Objects are concatenated in their input order to form the logical file.
    void build(const StoredObjects & objects);

    /// The segment containing `logical_offset`, or nullptr if it is at or past `totalSize`.
    const Segment * findObjectAt(size_t logical_offset) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
