#include <IO/OffsetMap.h>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void OffsetMap::build(const StoredObjects & objects)
{
    segments.clear();
    total_size = 0;

    for (const auto & obj : objects)
    {
        if (obj.bytes_size == StoredObject::UnknownSize)
        {
            /// An unknown-size object must appear alone: logical offsets for
            /// anything following it cannot be computed.
            if (objects.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OffsetMap: unknown-size object is only supported in single-object pipelines (got {} objects)",
                    objects.size());
            total_size = StoredObject::UnknownSize;
            segments.push_back(Segment{
                .object = obj,
                .logical_offset = 0,
                .size = StoredObject::UnknownSize,
            });
            return;
        }
        segments.push_back(Segment{
            .object = obj,
            .logical_offset = total_size,
            .size = obj.bytes_size,
        });
        total_size += obj.bytes_size;
    }
}

const StoredObject * OffsetMap::findObjectAt(size_t logical_offset, size_t * object_logical_start_offset) const
{
    /// Linear scan: the segment count equals the file's object count, a handful at most.
    for (const auto & seg : segments)
    {
        if (seg.logical_offset <= logical_offset && logical_offset < seg.logical_offset + seg.size)
        {
            if (object_logical_start_offset)
                *object_logical_start_offset = seg.logical_offset;
            return &seg.object;
        }
    }
    return nullptr;
}

}
