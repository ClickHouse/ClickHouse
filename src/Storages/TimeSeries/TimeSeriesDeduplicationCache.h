#pragma once

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <base/types.h>

#include <chrono>
#include <memory>
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

    /// The rows which one insert is going to write, mapping the key of a row to the hash of its latest values. They are kept
    /// apart from the cache until the insert is finished, because a failed insert must not make the next inserts skip its rows.
    /// The map holds at most as many rows as the cache. When a block overflows it, the keys of the block's last written rows are
    /// forgotten and removed from the cache too, so that an older row of such a key isn't skipped when it's written again.
    using PendingRows = std::unordered_map<KeyHash, RowHash, UInt128TrivialHash>;

    TimeSeriesDeduplicationCache(
        size_t max_size_bytes_,
        UInt64 expiration_seconds_,
        CurrentMetrics::Metric entries_metric,
        CurrentMetrics::Metric bytes_metric,
        ProfileEvents::Event hits_event_,
        ProfileEvents::Event misses_event_);

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

    /// A row of a block passing the filter, with the hash of its key and the hash of all its values.
    struct HashedRow
    {
        KeyHash key_hash;
        RowHash row_hash;
        size_t row_in_filter;
    };
    using HashedRows = std::vector<HashedRow>;

    /// The entries of the cache for the keys of `HashedRows`, in the same order. An entry is null if the key isn't in the cache.
    using WrittenRows = std::vector<Cache::MappedPtr>;

    /// Resets `filter` for the rows of `block` already written or pending, and adds the other rows passing the filter
    /// to `pending_rows`, which is then trimmed to the capacity of the cache. Returns the number of rows passing the filter.
    size_t excludeWrittenRowsFromFilter(
        const Block & block, size_t key_column_index, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows);

    /// Calculates the hashes of the rows of `block` passing `filter`.
    static HashedRows hashRows(const Block & block, size_t key_column_index, const PaddedPODArray<UInt8> & filter);

    /// Resets `filter` for the duplicate rows and returns the number of such duplicate rows.
    /// A row is considered a duplicate in one of the following cases:
    /// - its key is already pending in this insert and the row is the same as the pending row;
    /// - its key isn't pending and the row is the same as the row written by an earlier insert (the entry in `written_rows`),
    ///   and that entry hasn't expired.
    /// Each other row becomes the pending row.
    size_t excludeDuplicateRowsFromFilter(
        const HashedRows & hashed_rows,
        const WrittenRows & written_rows,
        PaddedPODArray<UInt8> & filter,
        PendingRows & pending_rows) const;

    /// Trims `pending_rows` to the capacity of the cache, starting from the keys of the last written rows of the block
    /// (the rows passing `filter`). The trimmed keys are removed from the cache too, see `PendingRows`.
    void trimPendingRows(
        const HashedRows & hashed_rows,
        const WrittenRows & written_rows,
        PendingRows & pending_rows,
        const PaddedPODArray<UInt8> & filter);

    /// Whether `expiration_seconds` have passed since `written_row` was written: such an entry doesn't make a row a duplicate.
    bool isExpired(const WrittenRow & written_row, TimePoint now) const;

    Cache cache;
    const UInt64 expiration_seconds;
    const size_t max_pending_rows;
    const ProfileEvents::Event hits_event;
    const ProfileEvents::Event misses_event;
};

using TimeSeriesDeduplicationCachePtr = std::shared_ptr<TimeSeriesDeduplicationCache>;

}
