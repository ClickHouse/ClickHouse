#pragma once

#include <Common/HyperLogLogCounter.h>

#include <atomic>
#include <deque>
#include <vector>

namespace DB
{

/// The finalizer of MurmurHash3, a bijection of 32-bit words. It mixes the low 32 bits of the hash of the map before
/// the sketch splits them into an index and a rank: the hash of the map can be a CRC, whose bits are linear in the bits
/// of the key, so the index and the rank it gives are not independent.
struct JoinKeyHashMix
{
    UInt32 operator()(UInt32 x) const
    {
        x ^= x >> 16;
        x *= 0x85ebca6b;
        x ^= x >> 13;
        x *= 0xc2b2ae35;
        x ^= x >> 16;
        return x;
    }
};

/// 8 KiB, standard error about 1.15%.
using DeferredBuildSketch = HyperLogLogCounter<
    13,
    UInt32,
    JoinKeyHashMix,
    UInt32,
    double,
    TrivialBiasEstimator,
    HyperLogLogMode::FullFeatured,
    DenominatorMode::StableIfBig,
    HyperLogLogRegisters::Bytes,
    /*track_changes=*/true>;

/// What one build stream of a deferred build counted while it stored its blocks, for each ON clause: the keys of
/// the rows that the insert will take (non-NULL and passing the ON condition), and their number per bucket of the map.
/// The stream is the only writer. While it writes, other threads read the atomic totals, and the sketch with
/// `mergeLive`. The per-bucket arrays are read only after it finished.
struct DeferredBuildStreamCounts
{
    struct Clause
    {
        explicit Clause(size_t num_buckets) : bucket_rows(num_buckets), bucket_key_bytes(num_buckets) {}

        DeferredBuildSketch sketch;
        std::vector<UInt64> bucket_rows;
        /// The bytes of the string keys, duplicates included. The maps copy only the distinct keys into an arena.
        std::vector<UInt64> bucket_key_bytes;
        std::atomic<UInt64> rows = 0;
        std::atomic<UInt64> key_bytes = 0;
        std::atomic<UInt64> max_key_bytes = 0;
    };

    DeferredBuildStreamCounts(size_t num_clauses, size_t num_buckets);

    /// A deque, because the sketch cannot move.
    std::deque<Clause> clauses;
    /// The null maps that a `RIGHT` or `FULL` join keeps for the stored blocks of this stream.
    std::atomic<UInt64> null_map_bytes = 0;
};

/// What the size projection needs to know about the maps of one clause. Taken when the join is created, so that the
/// projection never reads the maps, which a switch to `GraceHashJoin` frees.
struct DeferredBuildMapShape
{
    /// The bytes of the buffer of a bucket reserved for `keys` keys.
    size_t (*bucket_buffer_bytes)(size_t keys) = nullptr;
    /// Whether the map copies each distinct string key into the arena of the slot.
    bool keys_in_arena = false;
    /// Whether the map keeps a list of rows per key, in nodes of `RowRefList::Batch` in the arena of the slot.
    bool row_lists = false;
};

/// The counts of every build stream of a deferred build, and the projection of the bytes the build will take. Each
/// stream creates its counts at its first block and is their only writer. Everything lives until the join is destroyed.
class DeferredBuildState
{
public:
    DeferredBuildState(size_t num_streams, std::vector<DeferredBuildMapShape> shapes_, size_t num_buckets, size_t num_slots);
    ~DeferredBuildState();

    /// Called by stream `stream` only.
    DeferredBuildStreamCounts & streamCounts(size_t stream);

    /// Once every stream has finished: the estimated number of distinct keys of clause `clause`, and the rows per bucket.
    struct ClauseTotals
    {
        size_t estimate = 0;
        std::vector<UInt64> bucket_rows;
        std::vector<UInt64> bucket_key_bytes;
        size_t rows = 0;
        size_t max_key_bytes = 0;
    };
    ClauseTotals totals(size_t clause) const;

    void addStoredRows(size_t rows) { rows_stored.fetch_add(rows, std::memory_order_relaxed); }

    /// The bytes that the maps, their arenas and the null maps will take once the stored blocks are inserted, beyond
    /// the empty buffers of the maps. Once every stream has finished.
    size_t projectedBytes() const;

    /// The same while the streams store blocks: from the totals they publish, and with the even share of the rows in
    /// each bucket. A recompute merges the sketches of all streams, so it runs only after the stored rows grow by 1/16,
    /// and in one thread at a time; the others take the last value.
    size_t liveProjectedBytes();

private:
    struct ClauseProjection
    {
        size_t estimate = 0;
        size_t rows = 0;
        std::vector<size_t> bucket_reserves;
        /// Per slot.
        std::vector<size_t> slot_rows;
        std::vector<size_t> slot_key_bytes;
        size_t max_key_bytes = 0;
    };
    size_t projectedBytes(const std::vector<ClauseProjection> & clauses, size_t null_map_bytes) const;

    const std::vector<DeferredBuildMapShape> shapes;
    const size_t num_buckets;
    const size_t num_slots;
    std::vector<std::atomic<DeferredBuildStreamCounts *>> streams;

    std::atomic<UInt64> rows_stored = 0;
    std::atomic<UInt64> projection_at_rows = 0;
    std::atomic<UInt64> cached_projection = 0;
    /// Claimed by the thread that recomputes the live projection, which alone touches `live_sketches`.
    std::atomic<bool> refreshing = false;
    std::deque<DeferredBuildSketch> live_sketches;
};

/// The number of keys to reserve in each bucket of a map, from the estimate of distinct keys and the rows per bucket.
/// A bucket gets the even share of the estimate with room for its spread, or its share of the rows when that is larger,
/// up to four even shares: a bucket full of the duplicates of a hot key does not have the keys its rows suggest. No
/// bucket gets more than its rows. A bucket that gets more keys grows as in a streaming build.
std::vector<size_t> deferredBuildBucketReserves(size_t estimate, const std::vector<UInt64> & bucket_rows, size_t rows);

}
