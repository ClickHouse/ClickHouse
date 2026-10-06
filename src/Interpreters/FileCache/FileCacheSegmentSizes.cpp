#include <Interpreters/FileCache/FileCacheSegmentSizes.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{

namespace
{

UInt64 load(const std::atomic<Int64> & counter)
{
    const Int64 value = counter.load(std::memory_order_relaxed);
    /// Each file segment subtracts only what it added before, under its own lock.
    chassert(value >= 0);
    return static_cast<UInt64>(std::max<Int64>(value, 0));
}

}

FileCacheSegmentSizes::Class FileCacheSegmentSizes::getClass(size_t range_size) const
{
    const auto bucket = std::lower_bound(BOUNDS.begin(), BOUNDS.end(), range_size) - BOUNDS.begin();
    return Class{.bucket = static_cast<UInt8>(bucket), .large = range_size > boundary_alignment};
}

void FileCacheSegmentSizes::add(Class size_class, Int64 segments_delta, Int64 bytes_delta)
{
    segments[size_class.bucket].fetch_add(segments_delta, std::memory_order_relaxed);
    bytes[size_class.bucket].fetch_add(bytes_delta, std::memory_order_relaxed);
    if (size_class.large)
        large_bytes.fetch_add(bytes_delta, std::memory_order_relaxed);
}

FileCacheSegmentSizes::Buckets FileCacheSegmentSizes::getBuckets() const
{
    Buckets result;
    for (size_t i = 0; i < NUM_BUCKETS; ++i)
        result[i] = Bucket{.segments = load(segments[i]), .bytes = load(bytes[i])};
    return result;
}

UInt64 FileCacheSegmentSizes::getLargeBytes() const
{
    return load(large_bytes);
}

String FileCacheSegmentSizes::getBucketName(size_t bucket)
{
    return bucket < BOUNDS.size() ? std::to_string(BOUNDS[bucket]) : "inf";
}

}
