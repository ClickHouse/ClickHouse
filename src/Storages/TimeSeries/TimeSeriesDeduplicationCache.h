#pragma once

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <base/types.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <unordered_map>


namespace DB
{
class Block;

/// Remembers the rows recently written to a target table of a TimeSeries table, so that the same rows
/// aren't written with every insert (the description of a metric family and the tags of a time series arrive with every scrape).
///
/// A row is identified by its key column, and the cache remembers the latest row of each key as the hash of all its values.
/// A row with a changed value is written and replaces the remembered one, so the target table gets the changes in their order.
/// Collapsing the duplicates is the job of the target table's engine (`ReplacingMergeTree` or `AggregatingMergeTree`).
///
/// An entry expires a fixed time after the row was written, hits don't prolong it. So every row is written again at least once
/// per expiration period, and any difference between the cache and the table disappears within that period.
/// When the cache is full, the entries used only once are evicted first, then the least recently used ones (the `SLRU` policy).
/// The size of the cache is estimated from the number of entries.
///
/// The cache is local to the server and doesn't notice direct changes to the target table.
/// `SYSTEM DROP TIME SERIES CACHES` clears it without waiting for the expiration.
class TimeSeriesDeduplicationCache
{
public:
    using KeyHash = UInt128;
    using RowHash = UInt128;

    /// The rows which one insert is going to write. They are kept apart from the cache until the insert is finished,
    /// because a failed insert must not make the next inserts skip its rows.
    struct PendingRows
    {
        /// Maps the key of a pending row to the hash of its latest values. Holds at most as many rows as the cache,
        /// which bounds the memory of an insert. A row beyond that limit is written even if it repeats within the insert.
        std::unordered_map<KeyHash, RowHash, UInt128TrivialHash> rows;

        /// The generation of the cache when the first row was added. If the cache is cleared after that, the pending rows
        /// may be gone from the table, so they are forgotten, and the ones already in the cache don't count.
        size_t generation = 0;
    };

    TimeSeriesDeduplicationCache(
        size_t max_size_bytes_,
        UInt64 expiration_seconds_,
        CurrentMetrics::Metric entries_metric,
        CurrentMetrics::Metric bytes_metric,
        ProfileEvents::Event hits_event_,
        ProfileEvents::Event misses_event_);

    /// Changes the limits of the cache. The entries beyond a smaller size are removed at once.
    void setLimits(size_t max_size_bytes, UInt64 expiration_seconds_);

    /// Forgets all the entries. The rows pending in the running inserts are forgotten too.
    void clear();

    /// Returns `block` without the rows already written by earlier inserts (found in the cache) and without the rows pending
    /// in the same insert (found in `pending_rows`). A row is identified by the column `key_column_index` and counts as the same
    /// only if all its values are equal. The other rows are added to `pending_rows`, pass them to `markRowsAsWritten`
    /// after they are written.
    /// The rows with `filter[row] == 0` (if `filter` is passed) are removed too, without being looked up.
    Block filterOutWrittenRows(const Block & block, size_t key_column_index, PendingRows & pending_rows, PaddedPODArray<UInt8> filter = {});

    /// Moves the pending rows to the cache, so that `filterOutWrittenRows` skips them next time, and leaves `pending_rows` empty.
    /// Called after the pending rows have been written to the table.
    void markRowsAsWritten(PendingRows && pending_rows);

private:
    using TimePoint = std::chrono::steady_clock::time_point;

    /// The latest row written for a key.
    struct WrittenRow
    {
        RowHash row_hash{};
        size_t generation = 0;
        TimePoint written_at{};
    };

    struct WrittenRowWeight
    {
        size_t operator()(const WrittenRow &) const { return APPROXIMATE_ENTRY_SIZE; }
    };

    using Cache = CacheBase<KeyHash, WrittenRow, UInt128TrivialHash, WrittenRowWeight>;

    /// The approximate heap footprint of one entry: the node of the hash map with the key and the cell of the cache policy,
    /// the node of the recency list with the key, and the shared value with its control block.
    static constexpr size_t APPROXIMATE_ENTRY_SIZE
        = (sizeof(KeyHash) + 6 * sizeof(void *)) + (sizeof(KeyHash) + 2 * sizeof(void *)) + (sizeof(WrittenRow) + 3 * sizeof(void *));

    /// Resets `filter` for the rows of `block` already written or pending, and adds the other rows passing the filter to `pending_rows`.
    /// Returns the number of rows passing the filter.
    size_t excludeWrittenRowsFromFilter(
        const Block & block, size_t key_column_index, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows);

    /// An entry expires after the expiration period, or at once if the cache was cleared after the entry was added.
    bool isExpired(const WrittenRow & written_row, TimePoint now) const;

    Cache cache;
    std::atomic<UInt64> expiration_seconds;
    std::atomic<size_t> max_pending_rows;
    const ProfileEvents::Event hits_event;
    const ProfileEvents::Event misses_event;

    /// Incremented by `clear`.
    std::atomic<size_t> current_generation = 0;
};

using TimeSeriesDeduplicationCachePtr = std::shared_ptr<TimeSeriesDeduplicationCache>;

}
