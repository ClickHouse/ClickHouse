#pragma once

#include <base/types.h>
#include <cstddef>
#include <utility>
#include <span>
#include <roaring/roaring.hh>

namespace DB
{

struct MergeTreeIndexTextParams;

using PostingList = roaring::Roaring;

/** Coarse posting lists trade the precision of a text index for its size.
  *
  * Instead of row ids, the posting list of a token may store bucket ids (row >> level) at some level L,
  * so that a single stored value covers a range of 2^L rows. Fewer and smaller values are stored,
  * but the posting list becomes a superset of the exact one: it is expanded back into the ranges of row ids on read,
  * so the index may return false positives, which are filtered out by the following steps of the query pipeline.
  *
  * The level is chosen per token at serialize time (see `coarsenPostings`),
  * so that rare tokens, whose posting lists fit into the budget, keep their exact row ids.
  */

/// Row ids are UInt32, so a bucket at a level above this cannot represent any row.
constexpr UInt32 MAX_COARSE_LEVEL = 31;

/// Parameters needed to coarsen one token's postings at serialize time.
struct CoarseSerializationParams
{
    /// Coarsen when the number of stored values exceeds this. 0 disables coarsening.
    UInt64 budget = 0;
    /// Cap for the chosen coarse level: floor(log2(coarse_granularity)).
    UInt32 max_level = 0;

    bool enabled() const { return budget > 0 && max_level > 0; }
};

/// Computes the coarsening budget for the given amount of data:
/// S = ceil(num_rows_covered / coarse_granularity), i.e. the number of buckets of coarse_granularity rows covering the data.
CoarseSerializationParams makeCoarseSerializationParams(const MergeTreeIndexTextParams & params, UInt64 num_rows_covered);

/// Finds the finest level L in [1, max_level] such that the number of distinct buckets (row >> L)
/// does not exceed the budget (max_level if none fits) and returns the bucket list at that level.
std::pair<PostingList, UInt32> coarsenPostings(const PostingList & postings, UInt64 budget, UInt32 max_level);
/// The values must be strictly increasing.
std::pair<PostingList, UInt32> coarsenPostings(std::span<const UInt32> postings, UInt64 budget, UInt32 max_level);

/// Expands all bucket ids of a coarse posting list into ranges of row ids.
PostingList expandCoarsePostings(const PostingList & buckets, UInt32 level);

/// Adds decoded values of a posting list, which must be increasing, into a `PostingList` of rows.
///
/// If `level` is zero, the values are row ids and are added as they are.
/// Otherwise they are bucket ids of a coarse posting list, and each bucket b is expanded
/// to a row-level superset: the closed row range [b << level, ((b + 1) << level) - 1].
///
/// The values are accepted incrementally, so that a codec can expand them while decoding.
class PostingsAppender
{
public:
    PostingsAppender(PostingList & rows_, UInt32 level_);

    void add(UInt32 value);
    void addMany(std::span<const UInt32> values);

    /// Flushes the pending run of buckets. Must be called once, after the last value is added.
    void finalize();

private:
    void addCoarse(UInt32 value);
    void flushRun();

    PostingList & rows;
    UInt32 level;

    /// Half-open row range of the run of adjacent buckets that is not flushed yet.
    UInt64 run_begin = 0;
    UInt64 run_end = 0;
    bool has_run = false;
};

}
