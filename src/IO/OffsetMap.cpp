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
            /// An unknown-size object must appear alone: file offsets for
            /// anything following it cannot be computed.
            if (objects.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OffsetMap: unknown-size object is only supported in single-object pipelines (got {} objects)",
                    objects.size());
            total_size = StoredObject::UnknownSize;
            segments.push_back(Segment{
                .object = obj,
                .file_offset = 0,
                .size = StoredObject::UnknownSize,
            });
            return;
        }
        segments.push_back(Segment{
            .object = obj,
            .file_offset = total_size,
            .size = obj.bytes_size,
        });
        total_size += obj.bytes_size;
    }
}

const OffsetMap::Segment * OffsetMap::findObjectAt(size_t file_offset) const
{
    /// Linear scan: the segment count equals the file's object count, a handful at most.
    for (const auto & seg : segments)
        if (seg.file_offset <= file_offset && file_offset < seg.file_offset + seg.size)
            return &seg;
    return nullptr;
}

VectorWithMemoryTracking<OffsetMap::ObjectRange> OffsetMap::map(ByteRange file_range) const
{
    VectorWithMemoryTracking<ObjectRange> result;
    const size_t req_end = file_range.end();
    for (const auto & seg : segments)
    {
        const size_t seg_end = seg.file_offset + seg.size;
        if (seg_end <= file_range.offset || seg.file_offset >= req_end)
            continue;

        const size_t overlap_start = std::max(seg.file_offset, file_range.offset);
        const size_t overlap_end = std::min(seg_end, req_end);
        /// Objects start at their own offset 0, so the object-local start is measured from `file_offset`.
        result.push_back(ObjectRange{
            .object = seg.object,
            .object_offset = overlap_start - seg.file_offset,
            .size = overlap_end - overlap_start,
        });
    }
    return result;
}

}
