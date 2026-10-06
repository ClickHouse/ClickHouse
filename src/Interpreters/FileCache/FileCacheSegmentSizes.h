#pragma once

#include <base/types.h>
#include <base/unit.h>

#include <array>
#include <atomic>

namespace DB
{

/// The file segments of one `FileCache` by their range size: the number of segments and their reserved
/// bytes in each size bucket, and the reserved bytes of large segments (range > `boundary_alignment`).
/// A file segment counts from its first reservation (or its load on startup) until its removal.
/// Unbound file segments (temporary data) do not count.
class FileCacheSegmentSizes
{
public:
    /// Inclusive upper bounds of the buckets. The last bucket has no upper bound.
    static constexpr std::array<size_t, 6> BOUNDS = {512_KiB, 1_MiB, 2_MiB, 4_MiB, 8_MiB, 16_MiB};
    static constexpr size_t NUM_BUCKETS = BOUNDS.size() + 1;

    /// The size class of one file segment.
    struct Class
    {
        UInt8 bucket = 0;
        bool large = false;
    };

    struct Bucket
    {
        UInt64 segments = 0;
        UInt64 bytes = 0;
    };
    using Buckets = std::array<Bucket, NUM_BUCKETS>;

    explicit FileCacheSegmentSizes(size_t boundary_alignment_) : boundary_alignment(boundary_alignment_) {}

    Class getClass(size_t range_size) const;

    void add(Class size_class, Int64 segments, Int64 bytes);

    Buckets getBuckets() const;
    UInt64 getLargeBytes() const;

    /// The upper bound of the bucket in bytes, or "inf" for the last one.
    static String getBucketName(size_t bucket);

private:
    const size_t boundary_alignment;
    std::array<std::atomic<Int64>, NUM_BUCKETS> segments{};
    std::array<std::atomic<Int64>, NUM_BUCKETS> bytes{};
    std::atomic<Int64> large_bytes = 0;
};

}
