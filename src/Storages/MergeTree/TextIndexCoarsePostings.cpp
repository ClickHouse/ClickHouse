#include <Storages/MergeTree/TextIndexCoarsePostings.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Common/PODArray.h>
#include <base/defines.h>

#include <algorithm>
#include <array>
#include <bit>
#include <functional>
#include <limits>

namespace DB
{

CoarseSerializationParams makeCoarseSerializationParams(const MergeTreeIndexTextParams & params, UInt64 num_rows_covered)
{
    /// Buckets of less than 2 rows are exact posting lists, so coarsening is disabled.
    if (params.coarse_granularity < 2 || num_rows_covered == 0)
        return {};

    CoarseSerializationParams coarse_params;
    coarse_params.budget = (num_rows_covered + params.coarse_granularity - 1) / params.coarse_granularity;
    coarse_params.max_level = std::min(static_cast<UInt32>(std::bit_width(params.coarse_granularity)) - 1, MAX_COARSE_LEVEL);
    return coarse_params;
}

std::pair<PostingList, UInt32> coarsenPostings(const PostingList & postings, UInt64 budget, UInt32 max_level)
{
    PaddedPODArray<UInt32> postings_array(postings.cardinality());
    postings.toUint32Array(postings_array.data());
    return coarsenPostings(postings_array, budget, max_level);
}

std::pair<PostingList, UInt32> coarsenPostings(std::span<const UInt32> postings, UInt64 budget, UInt32 max_level)
{
    chassert(max_level > 0 && max_level < 32);
    chassert(postings.size() > budget);
    chassert(std::ranges::adjacent_find(postings, std::ranges::greater_equal{}) == postings.end());

    if (postings.empty())
        return {PostingList(), 0};

    /// A bucket at level L is (value >> L), so two values share a bucket iff they agree on every bit above position L - 1:
    ///     (a >> L) == (b >> L)  iff  (a ^ b) < 2^L  iff  msb(a ^ b) < L,
    /// where msb(x) is the index of the highest set bit, that is std::bit_width(x) - 1.
    /// So a single number h = msb(a ^ b) decides all levels for a pair of values at once:
    /// they fall into different buckets up to level h and into the same bucket from level h + 1 on.
    /// Values that merged never split again, hence the number of buckets is non-increasing in the level.
    ///
    /// The values are sorted, so the values of one bucket form a contiguous run
    /// and every bucket boundary is exactly one pair of adjacent values with h >= L.
    /// Only adjacent pairs are therefore needed to count the buckets at every level:
    ///     buckets(L) = number of values - number of adjacent pairs with h < L.
    ///
    /// Step 1: count the number of adjacent pairs by h (the number of buckets merged at each level)
    std::array<UInt64, 32> buckets_merged_at_level{};

    for (size_t i = 1; i < postings.size(); ++i)
    {
        UInt64 highest_differing_bit = std::bit_width(postings[i - 1] ^ postings[i]) - 1;
        ++buckets_merged_at_level[highest_differing_bit];
    }

    /// Step 2: walk the levels up from 0 and calculate buckets(L) until the budget is met.
    UInt32 level = 0;
    UInt64 num_distinct_buckets = postings.size();

    while (level < max_level)
    {
        num_distinct_buckets -= buckets_merged_at_level[level];
        ++level;

        if (num_distinct_buckets <= budget)
            break;
    }

    /// Step 3: write out the bucket ids at the chosen level. Equal buckets are adjacent because the
    /// values are sorted, so skipping the repeats keeps the sequence passed to addBulk increasing.
    PostingList buckets;
    roaring::BulkContext context;

    UInt32 previous_bucket = postings[0] >> level;
    buckets.addBulk(context, previous_bucket);

    for (size_t i = 1; i < postings.size(); ++i)
    {
        UInt32 bucket = postings[i] >> level;

        if (bucket != previous_bucket)
        {
            buckets.addBulk(context, bucket);
            previous_bucket = bucket;
        }
    }

    buckets.runOptimize();
    return {std::move(buckets), level};
}

PostingList expandCoarsePostings(const PostingList & buckets, UInt32 level)
{
    chassert(level > 0);

    /// Materializing the buckets is much cheaper than walking them one by one using roaring iterator.
    PaddedPODArray<UInt32> buckets_array(buckets.cardinality());
    buckets.toUint32Array(buckets_array.data());

    PostingList rows;
    PostingsAppender appender(rows, level);
    appender.addMany(buckets_array);
    appender.finalize();
    return rows;
}

PostingsAppender::PostingsAppender(PostingList & rows_, UInt32 level_)
    : rows(rows_)
    , level(level_)
{
    chassert(level < 32);
}

void PostingsAppender::add(UInt32 value)
{
    if (level == 0)
    {
        rows.add(value);
        return;
    }

    addCoarse(value);
}

void PostingsAppender::addCoarse(UInt32 value)
{
    UInt64 range_begin = static_cast<UInt64>(value) << level;
    UInt64 range_end = range_begin + (UInt64(1) << level);

    if (has_run && run_end == range_begin)
    {
        run_end = range_end;
        return;
    }

    flushRun();

    run_begin = range_begin;
    run_end = range_end;
    has_run = true;
}

void PostingsAppender::addMany(std::span<const UInt32> values)
{
    if (level == 0)
    {
        rows.addMany(values.size(), values.data());
        return;
    }

    for (UInt32 value : values)
        addCoarse(value);
}

void PostingsAppender::finalize()
{
    if (level == 0)
        return;

    flushRun();
    /// The expanded rows are contiguous ranges, which are much cheaper to store as run containers.
    rows.runOptimize();
}

void PostingsAppender::flushRun()
{
    if (!has_run)
        return;

    const static constexpr UInt64 row_domain_end = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
    UInt64 end = std::min(run_end, row_domain_end);

    if (run_begin < end)
        rows.addRangeClosed(static_cast<UInt32>(run_begin), static_cast<UInt32>(end - 1));

    has_run = false;
}

}
