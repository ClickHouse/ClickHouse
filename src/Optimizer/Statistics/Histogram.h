#pragma once

#include <base/types.h>

namespace DB
{

struct Bucket;
using Buckets = std::vector<Bucket>;

struct Bucket
{
    /// Lower bound of the bucket.
    Float64 lower;
    /// Upper bound of the bucket.
    Float64 upper;
    /// How many rows in the bucket.
    Float64 count;
    /// Number of distinct value in the bucket.
    Float64 ndv;
};

/// Histogram of a column, used to eliminate row count.
/// Not used right now
class Histogram
{
public:
    Histogram(Buckets buckets_) : buckets(std::move(buckets_)) { }

    Float64 eliminateEA(Float64 value);
    Float64 eliminateNE(Float64 value);
    Float64 eliminateGE(Float64 value);
    Float64 eliminateLE(Float64 value);
    Float64 eliminateBetween(Float64 low, Float64 high);

private:
    Buckets buckets;
};

}
