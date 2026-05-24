#include <IO/OffsetMap.h>

#include <algorithm>

namespace DB
{

void OffsetMap::build(const StoredObjects & objects)
{
    segments.clear();
    total_size = 0;

    for (const auto & obj : objects)
    {
        segments.push_back(Segment{
            .object = obj,
            .object_offset = 0,
            .logical_offset = total_size,
            .size = obj.bytes_size,
        });
        total_size += obj.bytes_size;
    }
}

const StoredObject * OffsetMap::findObjectAt(size_t logical_offset, size_t * object_file_offset) const
{
    /// Linear scan — `segments.size()` is bounded by the file's object
    /// count, typically <= a handful even for gather-mode reads.
    for (const auto & seg : segments)
    {
        if (seg.logical_offset <= logical_offset && logical_offset < seg.logical_offset + seg.size)
        {
            if (object_file_offset)
                *object_file_offset = seg.logical_offset;
            return &seg.object;
        }
    }
    return nullptr;
}

std::vector<OffsetMap::PhysicalRange> OffsetMap::map(ByteRange logical_range) const
{
    std::vector<PhysicalRange> result;

    for (const auto & seg : segments)
    {
        size_t seg_start = seg.logical_offset;
        size_t seg_end = seg_start + seg.size;
        size_t req_end = logical_range.end();

        if (seg_end <= logical_range.offset || seg_start >= req_end)
            continue;

        size_t overlap_start = std::max(seg_start, logical_range.offset);
        size_t overlap_end = std::min(seg_end, req_end);
        size_t offset_in_object = seg.object_offset + (overlap_start - seg_start);

        result.push_back(PhysicalRange{
            .object = seg.object,
            .object_offset = offset_in_object,
            .size = overlap_end - overlap_start,
        });
    }

    return result;
}

}
