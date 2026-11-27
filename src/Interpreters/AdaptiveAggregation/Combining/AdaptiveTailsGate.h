#pragma once

#include <algorithm>
#include <array>
#include <bitset>

#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

namespace DB
{

/// What the misses of a chunk of rows did at the open tails (see `AdaptiveTailsGate::addLookups`).
struct AdaptiveTailLookups
{
    size_t lookups = 0;
    /// The lookups that found a record of their key.
    size_t hits = 0;
};

/// Decides whether the misses of a frozen producer look the tails of their partitions up (see
/// `AdaptivePartitionTails`). A lookup, the tail it leaves behind and the writer's report of where the tail's
/// record went cost every miss, and pay only for the misses that find their key. So the tails stay open while
/// the lookups find enough keys, and close when they stop: the misses then stage records of their own, and
/// only the repeats of the previous miss combine.
///
/// While the tails are closed, the gate watches the records that the writer appends to a sample of the
/// partitions (see `watchClosedBatch`): a record whose key is that of the previous record of its partition
/// is a miss that an open tail would have combined. So the gate keeps measuring how often a lookup would find
/// its key, at no cost to the kernel's loop, and the tails come back when the data turns repetitive.
///
/// One credit follows the hits of the recent windows of lookups: every window takes a quarter of the credit
/// away and adds its hits per `window` lookups. While the tails are open, a window is `window` lookups or
/// more. A kernel takes a block through open tails in chunks of `chunk_rows` rows and reports after each
/// chunk what its lookups did, so that its loop carries nothing for the gate. While the tails are closed, a
/// window is `watch_window` watched records or more. The tails are open while the credit is at least what
/// windows with one hit in `min_hit_rate_inverse` lookups keep it at. They close as soon as a window takes
/// the credit below that, and reopen at the first block after the watched records have taken it back. So
/// tails that have been finding their keys outlast a few empty windows: after windows in which every third
/// lookup hit, three windows without a hit leave the tails open and the fourth closes them. And closed tails
/// reopen after one watched window in which half the records repeat, or after about four in which every
/// fifth does.
///
/// Closed tails are emptied, because the records staged past them are no tails. So after a reopening the
/// first lookups, as many as there are partitions, only fill the tails and are not judged.
///
/// The watched partitions hold a part of the keys, so they can miss repeats that sit in a few keys. Closed
/// tails therefore also open for a try after `retry_misses` misses, whatever the watch found.
///
/// The gate belongs to the tails of one frozen phase and one fill of the producer's records (see
/// `AdaptivePartitionTails`). So it starts again, open and without a history, whenever the producer makes
/// new tails: at a freeze, and after a spill of its records.
class AdaptiveTailsGate
{
public:
    /// The rows of a chunk, after which the kernel reports its lookups.
    static constexpr size_t chunk_rows = 1024;
    /// The lookups that a window of open tails has at least.
    static constexpr size_t window = 1024;
    /// The hit rate that keeps the tails open: one lookup in this many.
    static constexpr size_t min_hit_rate_inverse = 8;
    /// The watched records that a window of closed tails has at least.
    static constexpr size_t watch_window = 256;
    /// The misses after which closed tails open for a try.
    static constexpr size_t retry_misses = 1024 * window;

    /// `layout_` is the layout of the producer's partitions.
    explicit AdaptiveTailsGate(const AdaptivePartitionLayout & layout_) : layout(layout_) { }

    [[nodiscard]] bool isOpen() const { return open; }

    /// Starts a block, and returns whether its misses look the tails up. Closed tails reopen when the watch
    /// has taken the credit back, or for a try.
    bool startBlock()
    {
        if (!open && (credit >= credit_floor || closed_misses >= retry_misses))
        {
            open = true;
            filling = true;
            lookups = 0;
            hits = 0;
            credit = std::max(credit, credit_floor);
        }
        return open;
    }

    /// Takes the lookups of a chunk through open tails, and returns whether the tails stay open. The caller
    /// empties tails that close before the next block (see `AdaptivePartitionTails::clear`).
    bool addLookups(AdaptiveTailLookups chunk)
    {
        lookups += chunk.lookups;
        hits += chunk.hits;
        if (lookups < (filling ? layout.numPartitions() : window))
            return true;
        if (filling)
        {
            filling = false;
            lookups = 0;
            hits = 0;
            return true;
        }
        endWindow();
        if (credit >= credit_floor)
            return true;
        close();
        return false;
    }

    /// Counts `missed_rows` misses staged while the tails are closed toward the next try.
    void addClosedMisses(size_t missed_rows) { closed_misses += missed_rows; }

    /// Watches a batch of `records` records with `hashes`, in staging order, that the writer is about to
    /// append while the tails are closed. Of the last records of the batch, those that go to the watched
    /// partitions are compared with the previous record of their partition: an equal hash is a miss that an
    /// open tail would have combined. The watch covers about `watch_window` such comparisons, which is one
    /// window, and it takes the batch's last records, so that it sees what the data has just turned into.
    /// The caller guarantees that the tails are closed.
    void watchClosedBatch(const UInt64 * hashes, size_t records);

private:
    /// The credit of windows with exactly the hit rate that keeps the tails open: each takes a quarter away
    /// and adds `window / min_hit_rate_inverse` hits.
    static constexpr size_t credit_floor = 4 * window / min_hit_rate_inverse;

    /// Ends a window. It may have more or fewer lookups than `window`, so its hits count in proportion.
    void endWindow()
    {
        credit = credit - credit / 4 + hits * window / lookups;
        lookups = 0;
        hits = 0;
    }

    /// Closes the tails. The records staged while the tails were open went unwatched, so the watch starts
    /// anew.
    void close()
    {
        open = false;
        closed_misses = 0;
        watched_seen.reset();
    }

    const AdaptivePartitionLayout layout;
    bool open = true;
    /// Whether the tails are filling after they opened.
    bool filling = true;
    /// The lookups, or the watched records, of the current window so far, and the hits among them.
    size_t lookups = 0;
    size_t hits = 0;
    size_t credit = credit_floor;
    /// The misses staged since the tails closed.
    size_t closed_misses = 0;

    /// The watched partitions are the first partition of every bucket. For each of them, the hash of the last
    /// record that `watchClosedBatch` saw go to it, and whether it saw one since the watch started anew.
    std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> watched_hashes{};
    std::bitset<ADAPTIVE_AGGREGATION_NUM_BUCKETS> watched_seen;
};

}
