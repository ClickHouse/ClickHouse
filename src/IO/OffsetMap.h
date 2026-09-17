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
    /// Objects are concatenated in their input order to form the logical file.
    void build(const StoredObjects & objects);

    /// Find the object containing `logical_offset`, or nullptr if it is at or past
    /// `totalSize`. When given, `object_logical_start_offset` returns that object's start
    /// offset in the logical file.
    const StoredObject * findObjectAt(size_t logical_offset, size_t * object_logical_start_offset = nullptr) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    struct Segment
    {
        StoredObject object;
        size_t logical_offset = 0;
        size_t size = 0;
    };

    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
