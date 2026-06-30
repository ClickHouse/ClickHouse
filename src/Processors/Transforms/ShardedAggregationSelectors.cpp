#include <Processors/Transforms/ShardedAggregationSelectors.h>

#include <algorithm>
#include <limits>
#include <Columns/ColumnsNumber.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/MapToRange.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace
{

/// Analyzes one input stream while it is still warming up. It does two jobs over the same window: it
/// detects the frequent ("hot") keys — counting each key by a fast 32-bit hash and promoting a key into the
/// shared hot set once its running count crosses the threshold — and, from the distinct-key count it
/// gathered, it decides the low-cardinality fallback. Once the window passes we stop counting and free the
/// memory.
///
/// Each input stream has its own detector, so the counting is lock-free; only promotion reaches into the
/// shared `HotKeyState`, and that path is guarded by its mutex.
class WarmupDetector
{
public:
    explicit WarmupDetector(size_t num_cold)
        : tau(computeTau(num_cold))
        , warmup_rows(computeWarmupRows(num_cold))
        , fallback_max_distinct(computeFallbackMaxDistinct(warmup_rows, num_cold))
    {
    }

    /// Whether the warmup decided the key cardinality is too low to be worth scattering, so the input
    /// selector should route every row to the merge path instead. Set once when the window closes; read
    /// lock-free by the selector on the same thread.
    bool shouldRouteAllToMerge() const { return route_all_to_merge; }

    void observe(const PaddedPODArray<UInt32> & hashes, const Columns & key_columns, size_t num_rows, HotKeyState & state)
    {
        if (done)
            return;

        for (size_t i = 0; i < num_rows; ++i)
        {
            const UInt32 h = hashes[i];
            ++total_rows;

            HashMap<UInt32, UInt32>::LookupResult it = nullptr;
            bool inserted = false;
            counts.emplace(h, it, inserted);
            if (inserted)
                it->getMapped() = 0;
            const UInt32 count = ++it->getMapped();

            if (!promoted_local.contains(h))
            {
                /// The threshold is (R - 1) / (S - 1) of the rows seen so far, but never below the
                /// MIN_HOT_COUNT floor. The number of hot keys bounds itself: at most about 1 / tau keys,
                /// which is roughly 2 * S (the number of cold shards), can ever clear this.
                const UInt64 threshold = std::max<UInt64>(MIN_HOT_COUNT, static_cast<UInt64>(static_cast<double>(total_rows) * tau));
                if (count >= threshold)
                {
                    state.promote(key_columns, i, h);
                    promoted_local.insert(h);
                }
            }

            if (total_rows >= warmup_rows)
            {
                /// Decide the low-cardinality fallback from the distinct-key count gathered so far, before
                /// the counter table is freed. Sharding makes every row pay a scatter copy to avoid a merge
                /// that costs about `distinct_keys * num_cold` key-merges; when that merge is cheaper than
                /// the rows we would copy, scattering is not worth it and we route everything to the merge
                /// path.
                route_all_to_merge = counts.size() < fallback_max_distinct;

                done = true;
                counts = HashMap<UInt32, UInt32>{}; // The window has closed, so we free the counter table.
                promoted_local.clear();
                break;
            }
        }
    }

private:
    /// The shard imbalance we are willing to tolerate, called R below. We treat a key as hot once it alone
    /// would push its cold shard past R times the average shard load, which works out to a frequency
    /// threshold of (R - 1) / (S - 1) of the rows, where S is the number of cold shards. We check that
    /// threshold against the row count seen so far.
    ///
    /// For example, with R = 1.5 and S = 8 the threshold is (1.5 - 1) / (8 - 1), about 7.1%. All rows of
    /// such a key go to a single shard. That shard then carries its 7.1%, plus its 1/8 share of the other
    /// 92.9%, for about 18.7% of all rows. The average shard carries only 1 / 8 = 12.5%, so the key's shard
    /// is 1.5 times the average, exactly the R we chose.
    static constexpr double MAX_IMBALANCE_RATIO = 1.5;

    /// A floor on the count needed to promote, so that we never promote a key seen only a handful of times
    /// on what would just be noise in its frequency estimate.
    static constexpr UInt64 MIN_HOT_COUNT = 512;

    /// The low-cardinality fallback threshold (κ in the cost model). We fall back to the hot path when the
    /// merge sharding avoids, about `distinct_keys * num_cold` key-merges, is cheaper than κ times the rows
    /// the scatter would copy.
    static constexpr double FALLBACK_MERGE_SCATTER_RATIO = 1.5;

    /// The length of the warmup window adapts to the shard count. A key right at the hot threshold appears
    /// in only a fraction (R - 1) / (S - 1) of the rows, and we need to see at least MIN_HOT_COUNT of its
    /// rows before we promote it. At that frequency, seeing MIN_HOT_COUNT of them takes about
    /// MIN_HOT_COUNT * (S - 1) / (R - 1) rows, so we size the window to a small multiple of that
    /// (WARMUP_SAFETY_FACTOR times), which lets even a borderline-hot key be caught.
    static constexpr UInt64 WARMUP_SAFETY_FACTOR = 2;
    static constexpr UInt64 WARMUP_ROWS_MIN = 1ULL << 17; /// 131,072
    static constexpr UInt64 WARMUP_ROWS_MAX = 1ULL << 24; /// 16,777,216
    static_assert(WARMUP_ROWS_MAX <= std::numeric_limits<UInt32>::max());

    /// The fraction of the rows a key must reach to be promoted, (R - 1) / (S - 1), with S the shard count.
    static double computeTau(size_t num_cold) { return (MAX_IMBALANCE_RATIO - 1.0) / static_cast<double>(num_cold > 1 ? num_cold - 1 : 1); }

    /// The warmup window length for this shard count, clamped to [WARMUP_ROWS_MIN, WARMUP_ROWS_MAX].
    static UInt64 computeWarmupRows(size_t num_cold)
    {
        const double rows = static_cast<double>(WARMUP_SAFETY_FACTOR * MIN_HOT_COUNT) / (MAX_IMBALANCE_RATIO - 1.0)
            * static_cast<double>(num_cold > 1 ? num_cold - 1 : 1);
        return std::clamp<UInt64>(static_cast<UInt64>(rows), WARMUP_ROWS_MIN, WARMUP_ROWS_MAX);
    }

    static UInt64 computeFallbackMaxDistinct(UInt64 warmup_rows, size_t num_cold)
    {
        return std::max<UInt64>(
            1, static_cast<UInt64>(FALLBACK_MERGE_SCATTER_RATIO * static_cast<double>(warmup_rows) / static_cast<double>(num_cold)));
    }

    /// The fraction of the rows a key must reach to be promoted.
    const double tau;

    const UInt64 warmup_rows;

    /// The cardinality threshold for the low-cardinality fallback (see `computeFallbackMaxDistinct`).
    const UInt64 fallback_max_distinct;

    HashMap<UInt32, UInt32> counts;
    std::unordered_set<UInt32> promoted_local;
    UInt64 total_rows = 0;
    bool done = false;
    bool route_all_to_merge = false;
};

Columns gatherKeyColumns(const Columns & columns, const ColumnNumbers & key_positions)
{
    Columns key_columns;
    key_columns.reserve(key_positions.size());
    for (auto position : key_positions)
        key_columns.push_back(columns[position]);
    return key_columns;
}

/// The whole 32-bit hash of the key columns. The single source of truth for the hash, so the input
/// selector and the hot-state scatter route a key identically.
PaddedPODArray<UInt32> computeKeyHash(const Columns & columns, const ColumnNumbers & key_positions, size_t num_rows)
{
    PaddedPODArray<UInt32> hash(num_rows, WEAK_HASH32_INITIAL_VALUE);
    for (auto position : key_positions)
        columns[position]->computeHashInto(0, num_rows, hash.data(), false);
    return hash;
}

/// Maps each row to its cold shard by mapping the whole key hash onto [0, num_cold) with `mapToRange`.
/// Using the whole hash, rather than just its low bits, keeps keys from clustering into a few downstream
/// hash-table buckets.
void mapHashToColdShards(const PaddedPODArray<UInt32> & hash, size_t num_cold, IColumn::Selector & selector)
{
    mapToRange(hash.data(), hash.size(), static_cast<UInt32>(num_cold), selector.data());
}

}

ChunkRoutingSelector makeInputHotColdSelector(HotKeyStatePtr state, ColumnNumbers key_positions, size_t num_cold_shards, size_t merge_output)
{
    auto detector = std::make_shared<WarmupDetector>(num_cold_shards);

    return [state, positions = std::move(key_positions), num_cold = num_cold_shards, merge_output, detector](
               const Columns & columns) -> ChunkRouting
    {
        const size_t num_rows = columns.empty() ? 0 : columns.front()->size();
        if (num_rows == 0)
            return {};

        /// The warmup found the key cardinality too low to be worth scattering: route the whole chunk to the
        /// merge path (port `merge_output`).
        if (detector->shouldRouteAllToMerge())
        {
            ChunkRouting routing;
            routing.whole_chunk_output = merge_output;
            return routing;
        }

        const PaddedPODArray<UInt32> hash = computeKeyHash(columns, positions, num_rows);

        const Columns key_columns = gatherKeyColumns(columns, positions);
        detector->observe(hash, key_columns, num_rows, *state);

        IColumn::Selector selector(num_rows);
        mapHashToColdShards(hash, num_cold, selector);

        /// Now override the hot rows to go to the merge path (the `merge_output` port).
        if (auto mask_column = state->buildHotMask(key_columns))
        {
            const auto & mask = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
            for (size_t i = 0; i < num_rows; ++i)
                if (mask[i])
                    selector[i] = merge_output;
        }

        ChunkRouting routing;
        routing.selector = std::move(selector);
        return routing;
    };
}

ChunkRoutingSelector makeColdHashSelector(ColumnNumbers key_positions, size_t num_cold_shards)
{
    return [positions = std::move(key_positions), num_cold = num_cold_shards](const Columns & columns) -> ChunkRouting
    {
        const size_t num_rows = columns.empty() ? 0 : columns.front()->size();
        if (num_rows == 0)
            return {};

        const PaddedPODArray<UInt32> hash = computeKeyHash(columns, positions, num_rows);

        IColumn::Selector selector(num_rows);
        mapHashToColdShards(hash, num_cold, selector);

        ChunkRouting routing;
        routing.selector = std::move(selector);
        return routing;
    };
}

}
