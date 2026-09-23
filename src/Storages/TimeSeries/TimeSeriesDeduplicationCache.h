#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>
#include <base/types.h>

#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>


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
/// The oldest entries are also evicted when the cache is full. The size of the cache is estimated from the number of entries.
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
        /// The latest row of each key. Holds at most as many rows as the cache, which bounds the memory of an insert.
        /// A row beyond that limit is written even if it repeats within the insert.
        std::unordered_map<KeyHash, RowHash, UInt128TrivialHash> rows;

        /// The generation of the cache when the first row was added. The rows aren't added to the cache if it was cleared
        /// while the insert was running, because they may be gone from the table.
        size_t generation = 0;
    };

    TimeSeriesDeduplicationCache(
        size_t max_size_bytes_,
        UInt64 expiration_seconds_,
        CurrentMetrics::Metric entries_metric_,
        CurrentMetrics::Metric bytes_metric_,
        ProfileEvents::Event hits_event_,
        ProfileEvents::Event misses_event_);
    ~TimeSeriesDeduplicationCache();

    /// Changes the limits of the cache. The entries beyond a smaller limit are removed at once.
    void setLimits(size_t max_size_bytes, UInt64 expiration_seconds_);

    /// Forgets all the entries. The pending rows of the running inserts aren't added to the cache after that.
    void clear();

    /// Returns `block` without the rows already written by earlier inserts (found in the cache) or by the same insert
    /// (found in `pending_rows`). A row is identified by the column `key_column_index` and counts as written only if all its values
    /// are the same. The other rows are added to `pending_rows`, pass them to `markRowsAsWritten` after they are written.
    /// The rows filtered out by `filter` (if it's passed) are removed too, without being looked up.
    Block filterOutWrittenRows(const Block & block, size_t key_column_index, PendingRows & pending_rows, PaddedPODArray<UInt8> filter = {});

    /// Moves the pending rows to the cache, so that `filterOutWrittenRows` skips them next time.
    void markRowsAsWritten(const PendingRows & pending_rows);

private:
    using TimePoint = std::chrono::steady_clock::time_point;
    using BatchId = UInt64;

    /// The keys of the rows written by one insert, they share the time of writing.
    struct Batch
    {
        BatchId batch_id = 0;
        TimePoint written_at{};
        std::vector<KeyHash> keys;
    };

    /// The batches are ordered by the time of writing, the oldest one is at the front.
    using Batches = std::deque<Batch>;

    /// The latest row written for a key.
    struct WrittenRow
    {
        RowHash row_hash;

        /// The batch holding the key. A replaced row leaves the key in its older batch, where it's skipped.
        BatchId batch_id;
    };

    using WrittenRows = std::unordered_map<KeyHash, WrittenRow, UInt128TrivialHash>;

    /// The approximate heap footprint of one entry: the key in its batch, and the node of the map with a bucket.
    /// The size limit of the cache is converted to a number of entries with it.
    static constexpr size_t APPROXIMATE_ENTRY_SIZE = sizeof(KeyHash) + sizeof(KeyHash) + sizeof(WrittenRow) + 3 * sizeof(void *);

    /// Resets `filter` for the rows of `block` already written and adds the other rows passing the filter to `pending_rows`.
    /// Returns the number of rows passing the filter.
    size_t excludeWrittenRowsFromFilter(
        const Block & block, size_t key_column_index, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows);

    /// Returns the current time for a new batch or for an expiration check, must be called under the mutex.
    TimePoint getCurrentTime() const TSA_REQUIRES(mutex);

    /// Removes the entries written at least `expiration_seconds` before `now`, batch by batch from the front.
    void removeExpiredEntries(TimePoint now) TSA_REQUIRES(mutex);

    /// Removes the specified number of the oldest entries, batch by batch.
    void removeOldestEntries(size_t count) TSA_REQUIRES(mutex);

    size_t max_entries TSA_GUARDED_BY(mutex);
    UInt64 expiration_seconds TSA_GUARDED_BY(mutex);
    const CurrentMetrics::Metric entries_metric;
    const CurrentMetrics::Metric bytes_metric;
    const ProfileEvents::Event hits_event;
    const ProfileEvents::Event misses_event;

    Batches batches TSA_GUARDED_BY(mutex);
    WrittenRows written_rows TSA_GUARDED_BY(mutex);

    /// The number of keys in all the batches. The size limit applies to it, because a replaced row still takes a place in its older batch.
    size_t num_entries TSA_GUARDED_BY(mutex) = 0;

    BatchId last_batch_id TSA_GUARDED_BY(mutex) = 0;

    /// Incremented by `clear`, see `PendingRows::generation`.
    size_t current_generation TSA_GUARDED_BY(mutex) = 0;

    mutable std::mutex mutex;
};

using TimeSeriesDeduplicationCachePtr = std::shared_ptr<TimeSeriesDeduplicationCache>;

}
