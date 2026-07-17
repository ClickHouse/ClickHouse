#pragma once

#include <IO/ChainedBuffers.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Maps file offsets to (object, offset-within-object), abstracting many storage objects behind
/// a single file. Offsets are in the concatenated-file space: object sizes are byte lengths as
/// stored, so any payload/header (logical vs physical) split is the caller's concern, not this map's.
class OffsetMap
{
public:
    /// One object's placement in the concatenated file: `file_offset` is the object's start
    /// offset in the file, `size` its byte length.
    struct Segment
    {
        StoredObject object;
        size_t file_offset = 0;
        size_t size = 0;
    };

    /// One object's slice of a file range: `object_offset` is the start within the object,
    /// `size` the bytes it contributes. A range spanning N objects maps to N of these, in file order.
    struct ObjectRange
    {
        StoredObject object;
        size_t object_offset = 0;
        size_t size = 0;
    };

    /// Objects are concatenated in their input order to form the file.
    void build(const StoredObjects & objects);

    /// The segment containing `file_offset`, or nullptr if it is at or past `totalSize`.
    const Segment * findObjectAt(size_t file_offset) const;

    /// The objects overlapping `file_range`, each clipped to the overlap. A single range may span
    /// several objects; concatenating the returned sizes reconstructs `file_range.size` (clamped to
    /// `totalSize`). Used to fetch a window that crosses object boundaries.
    VectorWithMemoryTracking<ObjectRange> map(ByteRange file_range) const;

    size_t totalSize() const { return total_size; }

    bool hasUnknownSize() const { return total_size == StoredObject::UnknownSize; }

private:
    VectorWithMemoryTracking<Segment> segments;
    size_t total_size = 0;
};

}
