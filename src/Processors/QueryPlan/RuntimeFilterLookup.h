#pragma once

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

class BloomFilter;
using BloomFilterConstPtr = std::shared_ptr<const BloomFilter>;

/// Store and find per-query runtime filters that are used for optimizing some kinds of JOINs
/// by early pre-filtering of the left side of the JOIN.
struct IRuntimeFilterLookup : boost::noncopyable
{
    virtual ~IRuntimeFilterLookup() = default;

    /// Add runtime filter with the specified name
    virtual void add(const String & name, std::unique_ptr<BloomFilter> bloom_filter) = 0;

    /// Get filter by name
    virtual BloomFilterConstPtr find(const String & name) const = 0;
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

}
