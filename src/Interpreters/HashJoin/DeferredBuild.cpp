#include <Interpreters/HashJoin/DeferredBuild.h>

#include <Core/Defines.h>
#include <Interpreters/RowRefs.h>
#include <base/getPageSize.h>
#include <base/scope_guard.h>
#include <Common/FailPoint.h>

#include <algorithm>
#include <cmath>
#include <memory>

namespace DB
{

namespace FailPoints
{
extern const char hash_join_deferred_build_refresh_every_block[];
}

namespace
{

/// The bytes that an `Arena` with the default chunk sizes allocates for `demand` bytes in allocations of at most
/// `max_allocation` bytes each. A chunk wastes up to one allocation at its end.
size_t arenaBytes(size_t demand, size_t max_allocation)
{
    /// The defaults of `Arena`, which `BuildSlots` uses.
    static constexpr size_t initial_size = 4096;
    static constexpr size_t growth_factor = 2;
    static constexpr size_t linear_growth_threshold = 128 * 1024 * 1024;
    static constexpr size_t pad_right = PADDING_FOR_SIMD - 1;
    static const size_t page_size = static_cast<size_t>(::getPageSize());

    const size_t min_chunk = max_allocation + pad_right;
    size_t allocated = 0;
    size_t usable = 0;
    size_t chunk = 0;
    while (usable < demand)
    {
        if (chunk == 0)
            chunk = std::max(min_chunk, initial_size);
        else if (chunk < linear_growth_threshold)
            chunk = std::max(min_chunk, chunk * growth_factor);
        else
            chunk = (min_chunk + linear_growth_threshold - 1) / linear_growth_threshold * linear_growth_threshold;
        chunk = (chunk + page_size - 1) / page_size * page_size;
        allocated += chunk;
        usable += chunk - min_chunk;
    }
    return allocated;
}

}

DeferredBuildStreamCounts::DeferredBuildStreamCounts(size_t num_clauses, size_t num_buckets)
{
    for (size_t clause = 0; clause < num_clauses; ++clause)
        clauses.emplace_back(num_buckets);
}

DeferredBuildState::DeferredBuildState(
    size_t num_streams, std::vector<DeferredBuildMapShape> shapes_, size_t num_buckets_, size_t num_slots_)
    : shapes(std::move(shapes_))
    , num_buckets(num_buckets_)
    , num_slots(num_slots_)
    , streams(num_streams)
    , live_sketches(shapes.size())
{
}

DeferredBuildState::~DeferredBuildState()
{
    for (auto & stream : streams)
        delete stream.load(std::memory_order_acquire);
}

DeferredBuildStreamCounts & DeferredBuildState::streamCounts(size_t stream)
{
    if (auto * counts = streams[stream].load(std::memory_order_relaxed))
        return *counts;
    auto counts = std::make_unique<DeferredBuildStreamCounts>(shapes.size(), num_buckets);
    streams[stream].store(counts.get(), std::memory_order_release);
    return *counts.release();
}

DeferredBuildState::ClauseTotals DeferredBuildState::totals(size_t clause) const
{
    ClauseTotals result;
    result.bucket_rows.resize(num_buckets);
    result.bucket_key_bytes.resize(num_buckets);
    DeferredBuildSketch merged;
    for (const auto & stream : streams)
    {
        const auto * counts = stream.load(std::memory_order_acquire);
        if (!counts)
            continue;
        const auto & clause_counts = counts->clauses[clause];
        merged.merge(clause_counts.sketch);
        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
        {
            result.bucket_rows[bucket] += clause_counts.bucket_rows[bucket];
            result.bucket_key_bytes[bucket] += clause_counts.bucket_key_bytes[bucket];
        }
        result.max_key_bytes = std::max<size_t>(result.max_key_bytes, clause_counts.max_key_bytes.load(std::memory_order_relaxed));
    }
    for (const UInt64 bucket_rows : result.bucket_rows)
        result.rows += bucket_rows;
    /// The sketch counts distinct hashes and can exceed the rows, but there are never more distinct keys than rows.
    result.estimate = std::min<size_t>(merged.size(), result.rows);
    return result;
}

size_t DeferredBuildState::projectedBytes() const
{
    std::vector<ClauseProjection> clauses(shapes.size());
    for (size_t clause = 0; clause < shapes.size(); ++clause)
    {
        auto clause_totals = totals(clause);
        auto & projection = clauses[clause];
        projection.estimate = clause_totals.estimate;
        projection.rows = clause_totals.rows;
        projection.max_key_bytes = clause_totals.max_key_bytes;
        projection.bucket_reserves = deferredBuildBucketReserves(clause_totals.estimate, clause_totals.bucket_rows, clause_totals.rows);
        projection.slot_rows.assign(num_slots, 0);
        projection.slot_key_bytes.assign(num_slots, 0);
        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
        {
            projection.slot_rows[bucket % num_slots] += clause_totals.bucket_rows[bucket];
            projection.slot_key_bytes[bucket % num_slots] += clause_totals.bucket_key_bytes[bucket];
        }
    }

    size_t null_map_bytes = 0;
    for (const auto & stream : streams)
        if (const auto * counts = stream.load(std::memory_order_acquire))
            null_map_bytes += counts->null_map_bytes.load(std::memory_order_relaxed);

    return projectedBytes(clauses, null_map_bytes);
}

size_t DeferredBuildState::liveProjectedBytes()
{
    const size_t rows = rows_stored.load(std::memory_order_relaxed);
    const size_t at_rows = projection_at_rows.load(std::memory_order_acquire);
    bool refresh = rows > at_rows + at_rows / 16;
    fiu_do_on(FailPoints::hash_join_deferred_build_refresh_every_block, { refresh = true; });
    if (!refresh)
        return cached_projection.load(std::memory_order_relaxed);

    bool not_refreshing = false;
    if (!refreshing.compare_exchange_strong(not_refreshing, true, std::memory_order_acquire))
        return cached_projection.load(std::memory_order_relaxed);
    SCOPE_EXIT({ refreshing.store(false, std::memory_order_release); });

    std::vector<ClauseProjection> clauses(shapes.size());
    std::vector<size_t> key_bytes(shapes.size());
    size_t null_map_bytes = 0;
    for (const auto & stream : streams)
    {
        auto * counts = stream.load(std::memory_order_acquire);
        if (!counts)
            continue;
        null_map_bytes += counts->null_map_bytes.load(std::memory_order_relaxed);
        for (size_t clause = 0; clause < shapes.size(); ++clause)
        {
            auto & clause_counts = counts->clauses[clause];
            if (clause_counts.sketch.takeChanged())
                live_sketches[clause].mergeLive(clause_counts.sketch);
            clauses[clause].rows += clause_counts.rows.load(std::memory_order_relaxed);
            key_bytes[clause] += clause_counts.key_bytes.load(std::memory_order_relaxed);
            clauses[clause].max_key_bytes
                = std::max<size_t>(clauses[clause].max_key_bytes, clause_counts.max_key_bytes.load(std::memory_order_relaxed));
        }
    }

    for (size_t clause = 0; clause < shapes.size(); ++clause)
    {
        auto & projection = clauses[clause];
        projection.estimate = std::min<size_t>(live_sketches[clause].size(), projection.rows);
        /// The rows per bucket are not known before the streams finish, so every bucket gets an even share.
        const std::vector<UInt64> even_bucket_rows(num_buckets, (projection.rows + num_buckets - 1) / num_buckets);
        projection.bucket_reserves = deferredBuildBucketReserves(projection.estimate, even_bucket_rows, projection.rows);
        projection.slot_rows.assign(num_slots, (projection.rows + num_slots - 1) / num_slots);
        projection.slot_key_bytes.assign(num_slots, (key_bytes[clause] + num_slots - 1) / num_slots);
    }

    const size_t result = projectedBytes(clauses, null_map_bytes);
    cached_projection.store(result, std::memory_order_relaxed);
    projection_at_rows.store(rows, std::memory_order_release);
    return result;
}

size_t DeferredBuildState::projectedBytes(const std::vector<ClauseProjection> & clauses, size_t null_map_bytes) const
{
    /// See `RowRefList::insert`: a key with duplicates takes a node, and every `SLOTS` more rows take another one.
    static constexpr size_t row_list_node_bytes = sizeof(RowRefList::Batch);
    static constexpr size_t rows_per_row_list_node = RowRefList::Batch::SLOTS;

    size_t bytes = null_map_bytes;
    std::vector<size_t> slot_demand(num_slots);
    std::vector<size_t> slot_max_allocation(num_slots);
    for (size_t clause = 0; clause < clauses.size(); ++clause)
    {
        const auto & shape = shapes[clause];
        const auto & projection = clauses[clause];

        const size_t empty_bucket_bytes = shape.bucket_buffer_bytes(0);
        for (const size_t reserve : projection.bucket_reserves)
            bytes += shape.bucket_buffer_bytes(reserve) - empty_bucket_bytes;

        if (projection.rows == 0)
            continue;

        /// Only distinct keys are copied. This is an estimate, not a bound: the duplicates may be the shorter keys.
        if (shape.keys_in_arena)
        {
            const double distinct_share = std::min(1.0, static_cast<double>(projection.estimate) / static_cast<double>(projection.rows));
            for (size_t slot = 0; slot < num_slots; ++slot)
            {
                slot_demand[slot] += static_cast<size_t>(std::ceil(static_cast<double>(projection.slot_key_bytes[slot]) * distinct_share));
                slot_max_allocation[slot] = std::max(slot_max_allocation[slot], projection.max_key_bytes);
            }
        }

        if (shape.row_lists)
        {
            /// The sketch may count up to 6% too many keys, which would under-count the duplicates that take nodes.
            const size_t duplicates = projection.rows - std::min(projection.rows, projection.estimate - projection.estimate / 16);
            const size_t keys_with_duplicates = std::min(projection.estimate, duplicates);
            const size_t nodes = keys_with_duplicates + (duplicates + rows_per_row_list_node - 1) / rows_per_row_list_node;
            for (size_t slot = 0; slot < num_slots; ++slot)
            {
                const size_t slot_nodes = std::min(
                    projection.slot_rows[slot],
                    static_cast<size_t>(std::ceil(static_cast<double>(nodes) * static_cast<double>(projection.slot_rows[slot])
                                                  / static_cast<double>(projection.rows))));
                slot_demand[slot] += slot_nodes * row_list_node_bytes;
                slot_max_allocation[slot] = std::max(slot_max_allocation[slot], row_list_node_bytes);
            }
        }
    }

    for (size_t slot = 0; slot < num_slots; ++slot)
        bytes += arenaBytes(slot_demand[slot], slot_max_allocation[slot]);
    return bytes;
}

std::vector<size_t> deferredBuildBucketReserves(size_t estimate, const std::vector<UInt64> & bucket_rows, size_t rows)
{
    std::vector<size_t> reserves(bucket_rows.size());
    if (rows == 0)
        return reserves;

    const size_t target = std::min(estimate, rows);
    if (bucket_rows.size() == 1)
    {
        reserves[0] = target;
        return reserves;
    }

    const double even_share = static_cast<double>(target) / static_cast<double>(bucket_rows.size());
    /// Four standard deviations of the number of keys a bucket gets when the keys spread evenly.
    const auto with_spread = static_cast<size_t>(std::ceil(even_share + 4 * std::sqrt(even_share)));
    const auto max_row_share = static_cast<size_t>(4 * even_share);
    for (size_t bucket = 0; bucket < bucket_rows.size(); ++bucket)
    {
        const auto row_share = static_cast<size_t>(std::ceil(
            static_cast<double>(estimate) * static_cast<double>(bucket_rows[bucket]) / static_cast<double>(rows)));
        reserves[bucket] = std::min<size_t>(bucket_rows[bucket], std::max(with_spread, std::min(row_share, max_row_share)));
    }
    return reserves;
}

}
