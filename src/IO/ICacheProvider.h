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
    std::vector<Range> hit_ranges;
    std::vector<Range> miss_ranges;

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

    /// Read cached data as a rope slice. Range must be within hit_ranges.
    virtual RopeSlice get(Range range) = 0;

    /// Provide data for a miss range. Range must be within miss_ranges.
    /// Cache may copy (disk cache) or take ownership (page cache).
    /// First writer wins. Returns true if this writer won.
    /// On success, the miss range becomes a hit range.
    virtual bool put(Range range, RopeSlice data) = 0;
};

/// Cache provider interface. ReadPipeline configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    /// Lookup a range. Returns a handle that pins the result.
    /// The cache determines granularity internally.
    virtual std::unique_ptr<ICacheHandle> lookup(CacheKey key, Range range) = 0;

    virtual String name() const = 0;
};

}
