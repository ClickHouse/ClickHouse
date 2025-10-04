#pragma once

#include <Interpreters/Set.h>
#include <Interpreters/BloomFilter.h>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

/// As long as the number of unique values is small they are stored in a Set but when it grows beyond the limit
/// the values are moved into a BloomFilter.
class RuntimeFilter
{
public:
    RuntimeFilter(
        const DataTypePtr & filter_column_target_type,
        UInt64 exact_values_limit_,
        UInt64 bloom_filter_bytes_,
        UInt64 bloom_filter_hash_functions_);

    void insert(ColumnPtr values);

    /// No more insert()-s after this call, only find()-s
    void finishInsert();

    /// Looks up each value and returns column of Bool-s
    ColumnPtr find(ColumnPtr values) const;

    /// Add all keys from one filter to the other so that destination filter contains the union of both filters.
    void addAllFrom(const RuntimeFilter & source);

private:
    void insertIntoBloomFilter(ColumnPtr values);
    void switchToBloomFilter();

    const UInt64 exact_values_limit;
    const UInt64 bloom_filter_bytes;
    const UInt64 bloom_filter_hash_functions;

    BloomFilterPtr bloom_filter;
    SetPtr exact_values;
};

using RuntimeFilterConstPtr = std::shared_ptr<const RuntimeFilter>;

/// Store and find per-query runtime filters that are used for optimizing some kinds of JOINs
/// by early pre-filtering of the left side of the JOIN.
struct IRuntimeFilterLookup : boost::noncopyable
{
    virtual ~IRuntimeFilterLookup() = default;

    /// Add runtime filter with the specified name
    virtual void add(const String & name, std::unique_ptr<RuntimeFilter> bloom_filter) = 0;

    /// Get filter by name
    virtual RuntimeFilterConstPtr find(const String & name) const = 0;
};

using RuntimeFilterLookupPtr = std::shared_ptr<IRuntimeFilterLookup>;

}
