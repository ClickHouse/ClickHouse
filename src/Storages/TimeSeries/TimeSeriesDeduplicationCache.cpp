#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Core/Block.h>

#include <algorithm>


namespace DB
{

namespace
{
    /// Converts the size limit to the number of entries. A non-zero limit allows at least one entry, zero allows none.
    size_t maxEntriesForSize(size_t max_size_bytes, size_t entry_size)
    {
        return max_size_bytes ? std::max<size_t>(1, max_size_bytes / entry_size) : 0;
    }

    /// Returns `block` without the rows filtered out, `num_passed_rows` is the number of rows passing the filter.
    Block applyFilter(const Block & block, const PaddedPODArray<UInt8> & filter, size_t num_passed_rows)
    {
        if (num_passed_rows == block.rows())
            return block;
        if (!num_passed_rows)
            return block.cloneEmpty();

        Columns filtered_columns;
        filtered_columns.reserve(block.columns());
        for (const auto & column : block)
            filtered_columns.push_back(column.column->filter(filter, num_passed_rows));
        return block.cloneWithColumns(filtered_columns);
    }
}

TimeSeriesDeduplicationCache::TimeSeriesDeduplicationCache(
    size_t max_size_bytes_,
    UInt64 expiration_seconds_,
    CurrentMetrics::Metric entries_metric_,
    CurrentMetrics::Metric bytes_metric_,
    ProfileEvents::Event hits_event_,
    ProfileEvents::Event misses_event_)
    : max_entries(maxEntriesForSize(max_size_bytes_, APPROXIMATE_ENTRY_SIZE))
    , expiration_seconds(expiration_seconds_)
    , entries_metric(entries_metric_)
    , bytes_metric(bytes_metric_)
    , hits_event(hits_event_)
    , misses_event(misses_event_)
{
}

TimeSeriesDeduplicationCache::~TimeSeriesDeduplicationCache()
{
    std::lock_guard lock{mutex};
    CurrentMetrics::sub(entries_metric, num_entries);
    CurrentMetrics::sub(bytes_metric, num_entries * APPROXIMATE_ENTRY_SIZE);
}

Block TimeSeriesDeduplicationCache::filterOutWrittenRows(
    const Block & block, size_t key_column_index, PendingRows & pending_rows, PaddedPODArray<UInt8> filter)
{
    size_t num_rows = block.rows();
    size_t num_rows_to_insert = 0;
    if (filter.empty())
    {
        filter.resize_fill(num_rows, true);
        num_rows_to_insert = num_rows;
    }
    else
    {
        chassert(filter.size() == num_rows);
        num_rows_to_insert = countBytesInFilter(filter);
    }

    if (num_rows_to_insert)
        num_rows_to_insert = excludeWrittenRowsFromFilter(block, key_column_index, filter, pending_rows);

    return applyFilter(block, filter, num_rows_to_insert);
}

size_t TimeSeriesDeduplicationCache::excludeWrittenRowsFromFilter(
    const Block & block, size_t key_column_index, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows)
{
    size_t num_hits = 0;
    size_t num_misses = 0;

    /// Decides a row whose key is pending in this insert, `pending_row_hash` is the hash of the latest row written for the key.
    auto skip_or_update_pending_row = [&](size_t row_in_filter, RowHash row_hash, RowHash & pending_row_hash)
    {
        if (pending_row_hash == row_hash)
        {
            /// The same row was already written by this insert, so it's skipped.
            filter[row_in_filter] = false;
            ++num_hits;
        }
        else
        {
            /// The row has other values than the latest row written for the key, so it's written after that row
            /// and becomes the latest one.
            pending_row_hash = row_hash;
            ++num_misses;
        }
    };

    /// The rows of the keys new to this insert, they are looked up in the cache under the mutex.
    struct NewRow
    {
        KeyHash key_hash;
        RowHash row_hash;
        size_t row_in_filter;
    };
    std::vector<NewRow> new_rows;

    /// The hashes are calculated before locking the mutex.
    const auto & key_column = *block.getByPosition(key_column_index).column;
    for (size_t row = 0; row != filter.size(); ++row)
    {
        if (!filter[row])
            continue;

        SipHash key_hash_calculator;
        key_column.updateHashWithValue(row, key_hash_calculator);
        KeyHash key_hash = key_hash_calculator.get128();

        SipHash row_hash_calculator;
        for (const auto & column : block)
            column.column->updateHashWithValue(row, row_hash_calculator);
        RowHash row_hash = row_hash_calculator.get128();

        /// The rows of the keys already pending are decided here, the rows of the other keys under the mutex. Both parts keep
        /// the order of the rows, and all the rows of one key go to the same part, so the changes of a row are written in order.
        auto pending_it = pending_rows.rows.find(key_hash);
        if (pending_it != pending_rows.rows.end())
            skip_or_update_pending_row(row, row_hash, pending_it->second);
        else
            new_rows.push_back({.key_hash = key_hash, .row_hash = row_hash, .row_in_filter = row});
    }

    /// The rows of the new keys are looked up in the cache.
    if (!new_rows.empty())
    {
        std::lock_guard lock{mutex};

        /// The expired entries are removed first, so that they don't count as hits.
        removeExpiredEntries(getCurrentTime());

        /// The insert remembers the state of the cache when it starts adding its rows.
        if (pending_rows.rows.empty())
            pending_rows.generation = current_generation;

        for (const auto & [key_hash, row_hash, row_in_filter] : new_rows)
        {
            /// The key wasn't pending before the mutex was locked, but it can occur in the block several times: then its first row
            /// has been added to the pending rows by this loop, and the next rows are compared with it, not with the cache.
            auto pending_it = pending_rows.rows.find(key_hash);
            if (pending_it != pending_rows.rows.end())
            {
                skip_or_update_pending_row(row_in_filter, row_hash, pending_it->second);
                continue;
            }

            /// The same row was written by an earlier insert recently, so it's skipped.
            auto written_it = written_rows.find(key_hash);
            if ((written_it != written_rows.end()) && (written_it->second.row_hash == row_hash))
            {
                filter[row_in_filter] = false;
                ++num_hits;
                continue;
            }

            /// The map is capped by the capacity of the cache, see PendingRows::rows.
            if (pending_rows.rows.size() < max_entries)
                pending_rows.rows.emplace(key_hash, row_hash);
            ++num_misses;
        }
    }

    ProfileEvents::increment(hits_event, num_hits);
    ProfileEvents::increment(misses_event, num_misses);
    return num_misses;
}

void TimeSeriesDeduplicationCache::markRowsAsWritten(const PendingRows & pending_rows)
{
    std::lock_guard lock{mutex};

    /// Check if the cache was cleared while the insert was running.
    if (pending_rows.generation != current_generation)
    {
        /// The rows of the insert may be gone from the table, so they aren't added to the cache.
        return;
    }

    auto now = getCurrentTime();
    removeExpiredEntries(now);

    Batch batch{.batch_id = ++last_batch_id, .written_at = now, .keys = {}};
    batch.keys.reserve(pending_rows.rows.size());
    for (const auto & [key_hash, row_hash] : pending_rows.rows)
    {
        WrittenRow written_row{.row_hash = row_hash, .batch_id = batch.batch_id};
        auto [it, inserted] = written_rows.try_emplace(key_hash, written_row);
        if (!inserted)
        {
            /// The key is already in the cache, check if the row is the same.
            if (it->second.row_hash == row_hash)
            {
                /// The same row was added by a concurrent insert meanwhile.
                continue;
            }

            /// The row of the key has changed, so the new row takes the place of the old one in the map and gets a new time
            /// of writing. The key stays in the batch of the old row too, it's skipped there when that batch is removed.
            it->second = written_row;
        }
        batch.keys.push_back(key_hash);
    }

    if (batch.keys.empty())
        return;

    num_entries += batch.keys.size();
    CurrentMetrics::add(entries_metric, batch.keys.size());
    CurrentMetrics::add(bytes_metric, batch.keys.size() * APPROXIMATE_ENTRY_SIZE);
    batches.push_back(std::move(batch));

    if (num_entries > max_entries)
        removeOldestEntries(num_entries - max_entries);
}

void TimeSeriesDeduplicationCache::setLimits(size_t max_size_bytes, UInt64 expiration_seconds_)
{
    std::lock_guard lock{mutex};
    max_entries = maxEntriesForSize(max_size_bytes, APPROXIMATE_ENTRY_SIZE);
    expiration_seconds = expiration_seconds_;

    /// The expired entries are removed first, so that they don't take the place of the live ones when the size shrinks.
    removeExpiredEntries(getCurrentTime());
    if (num_entries > max_entries)
        removeOldestEntries(num_entries - max_entries);
}

TimeSeriesDeduplicationCache::TimePoint TimeSeriesDeduplicationCache::getCurrentTime() const
{
    /// The mutex must be locked: the batches are kept ordered by the time of writing, and `removeExpiredEntries`
    /// and `removeOldestEntries` look only at the front of the deque. If the time were taken before locking the mutex,
    /// two inserts could take their times in one order and push their batches in the other, and then a batch would stay
    /// in the cache until the newer batch in front of it expires.
    return std::chrono::steady_clock::now();
}

void TimeSeriesDeduplicationCache::removeExpiredEntries(TimePoint now)
{
    while (!batches.empty())
    {
        /// The elapsed time is compared in seconds, so that any value of the setting fits without overflow.
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - batches.front().written_at).count();
        if ((elapsed_seconds < 0) || (static_cast<UInt64>(elapsed_seconds) < expiration_seconds))
            break;
        removeOldestEntries(batches.front().keys.size());
    }
}

void TimeSeriesDeduplicationCache::removeOldestEntries(size_t count)
{
    size_t num_removed = 0;
    while ((num_removed < count) && !batches.empty())
    {
        /// The rows of a batch share the time of writing, so they can be removed in any order.
        auto & oldest_batch = batches.front();
        while ((num_removed < count) && !oldest_batch.keys.empty())
        {
            /// The key is skipped if its row was replaced by a newer batch.
            auto it = written_rows.find(oldest_batch.keys.back());
            if ((it != written_rows.end()) && (it->second.batch_id == oldest_batch.batch_id))
                written_rows.erase(it);
            oldest_batch.keys.pop_back();
            ++num_removed;
        }
        if (oldest_batch.keys.empty())
            batches.pop_front();
    }

    num_entries -= num_removed;
    CurrentMetrics::sub(entries_metric, num_removed);
    CurrentMetrics::sub(bytes_metric, num_removed * APPROXIMATE_ENTRY_SIZE);
}

void TimeSeriesDeduplicationCache::clear()
{
    std::lock_guard lock{mutex};
    CurrentMetrics::sub(entries_metric, num_entries);
    CurrentMetrics::sub(bytes_metric, num_entries * APPROXIMATE_ENTRY_SIZE);
    num_entries = 0;
    written_rows.clear();
    batches.clear();
    ++current_generation;
}

}
