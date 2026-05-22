#pragma once

#include <IO/Rope.h>
#include <base/types.h>

#include <memory>
#include <vector>

namespace DB
{

struct CacheKey
{
    String path;
    String version;
};

struct CacheLookupResult
{
    std::vector<ByteRange> hit_ranges;
    std::vector<ByteRange> miss_ranges;

    bool allHit() const { return miss_ranges.empty(); }
    bool allMiss() const { return hit_ranges.empty(); }
};

/// Handle returned by cache lookup. Pins results, provides get/put.
/// RAII — destructor releases pins.
class ICacheHandle
{
public:
    virtual ~ICacheHandle() = default;

    /// What's cached, what's not (in this cache's granularity).
    virtual CacheLookupResult status() const = 0;

    /// Read cached data as a rope slice. ByteRange must be within hit_ranges.
    virtual Rope get(ByteRange range) = 0;

    /// Provide data for a miss range. ByteRange must be within miss_ranges.
    /// Cache may copy (disk cache) or take ownership (page cache).
    /// First writer wins on a per-segment basis. Returns the number of bytes
    /// that actually landed in the cache; can be less than `range.size` when
    /// we lost the downloader race on some segments, reservation failed
    /// (cache full), or the handle is in bypass mode (returns 0).
    virtual size_t put(ByteRange range, Rope data) = 0;
};

/// Cache provider interface. ReadPipeline configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    /// Lookup a range. Returns a handle that pins the result.
    /// The cache determines granularity internally.
    virtual std::unique_ptr<ICacheHandle> lookup(CacheKey key, ByteRange range) = 0;

    virtual String name() const = 0;
};

}
