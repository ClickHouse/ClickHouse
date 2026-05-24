#include <IO/OffsetMap.h>

#include <Common/Exception.h>

#include <algorithm>

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
    has_unknown_size = false;

    for (const auto & obj : objects)
    {
        if (obj.bytes_size == StoredObject::UnknownSize)
        {
            /// Unknown-size objects (S3 `HEAD` without `Content-Length`,
            /// `stat()` failure on local disk) can only appear ALONE — we
            /// can't compute logical offsets for objects that follow an
            /// unknown-size one. In practice the only caller passing
            /// `UnknownSize` is `StorageObjectStorageSource`, which always
            /// reads single objects.
            if (objects.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OffsetMap: unknown-size object is only supported in single-object pipelines (got {} objects)",
                    objects.size());
            has_unknown_size = true;
            total_size = StoredObject::UnknownSize;
            segments.push_back(Segment{
                .object = obj,
                .object_offset = 0,
                .logical_offset = 0,
                .size = StoredObject::UnknownSize,
            });
            return;
        }
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
