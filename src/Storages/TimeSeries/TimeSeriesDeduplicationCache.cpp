#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Core/Block.h>


namespace DB
{

namespace
{
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
    CurrentMetrics::Metric entries_metric,
    CurrentMetrics::Metric bytes_metric,
    ProfileEvents::Event hits_event_,
    ProfileEvents::Event misses_event_)
    : cache(bytes_metric, entries_metric, max_size_bytes_)
    , expiration_seconds(expiration_seconds_)
    , max_pending_rows(max_size_bytes_ / APPROXIMATE_ENTRY_SIZE)
    , hits_event(hits_event_)
    , misses_event(misses_event_)
{
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
    /// The cache was cleared after the insert added its first pending row, so the pending rows may be gone from the table.
    /// They are forgotten, and the next rows are looked up in the cache again.
    if (pending_rows.generation != current_generation)
        pending_rows.rows.clear();

    size_t num_hits = 0;
    size_t num_misses = 0;

    /// Decides a row whose key is pending in this insert, `pending_row_hash` is the hash of the latest pending row of the key.
    auto skip_or_update_pending_row = [&](size_t row_in_filter, RowHash row_hash, RowHash & pending_row_hash)
    {
        if (pending_row_hash == row_hash)
        {
            /// The same row is already pending, so it's skipped.
            filter[row_in_filter] = false;
            ++num_hits;
        }
        else
        {
            /// The row has other values than the pending row of the key, so it passes the filter and becomes the pending row.
            pending_row_hash = row_hash;
            ++num_misses;
        }
    };

    /// The rows of the keys new to this insert, they are looked up in the cache.
    struct NewRow
    {
        KeyHash key_hash;
        RowHash row_hash;
        size_t row_in_filter;
    };
    std::vector<NewRow> new_rows;

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

        /// The rows of the keys already pending are decided here, the rows of the other keys after the lookup. Both parts keep
        /// the order of the rows, and all the rows of one key go to the same part, so the changes of a row are written in order.
        auto pending_it = pending_rows.rows.find(key_hash);
        if (pending_it != pending_rows.rows.end())
            skip_or_update_pending_row(row, row_hash, pending_it->second);
        else
            new_rows.push_back({.key_hash = key_hash, .row_hash = row_hash, .row_in_filter = row});
    }

    if (!new_rows.empty())
    {
        /// The insert remembers the generation before looking into the cache, so that its rows don't count
        /// if the cache is cleared from now on.
        if (pending_rows.rows.empty())
            pending_rows.generation = current_generation;

        std::vector<KeyHash> key_hashes;
        key_hashes.reserve(new_rows.size());
        for (const auto & new_row : new_rows)
            key_hashes.push_back(new_row.key_hash);
        auto written_rows = cache.getMany(key_hashes);

        auto now = std::chrono::steady_clock::now();
        size_t max_pending_rows_value = max_pending_rows;

        for (size_t i = 0; i != new_rows.size(); ++i)
        {
            const auto & [key_hash, row_hash, row_in_filter] = new_rows[i];

            /// The key wasn't pending before the lookup, but it can occur in the block several times: then its first row
            /// has been added to the pending rows by this loop, and the next rows are compared with it, not with the cache.
            auto pending_it = pending_rows.rows.find(key_hash);
            if (pending_it != pending_rows.rows.end())
            {
                skip_or_update_pending_row(row_in_filter, row_hash, pending_it->second);
                continue;
            }

            /// The same row was written by an earlier insert and its entry hasn't expired, so it's skipped.
            const auto & written_row = written_rows[i];
            if (written_row && (written_row->row_hash == row_hash) && !isExpired(*written_row, now))
            {
                filter[row_in_filter] = false;
                ++num_hits;
                continue;
            }

            /// The map is capped by the capacity of the cache, see PendingRows::rows.
            if (pending_rows.rows.size() < max_pending_rows_value)
                pending_rows.rows.emplace(key_hash, row_hash);
            ++num_misses;
        }
    }

    ProfileEvents::increment(hits_event, num_hits);
    ProfileEvents::increment(misses_event, num_misses);
    return num_misses;
}

bool TimeSeriesDeduplicationCache::isExpired(const WrittenRow & written_row, TimePoint now) const
{
    /// An entry of an earlier generation was added by an insert which didn't notice that the cache was cleared.
    if (written_row.generation != current_generation)
        return true;

    /// The elapsed time is compared in seconds, so that any value of the setting fits without overflow.
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - written_row.written_at).count();
    return (elapsed_seconds >= 0) && (static_cast<UInt64>(elapsed_seconds) >= expiration_seconds);
}

void TimeSeriesDeduplicationCache::markRowsAsWritten(PendingRows && pending_rows_)
{
    PendingRows pending_rows = std::move(pending_rows_);

    /// The cache was cleared while the insert was running, so its rows may be gone from the table. They wouldn't count anyway
    /// because of their generation, this check just saves the work. A clear during the loop below is handled the same way.
    if (pending_rows.generation != current_generation)
        return;

    /// A row already in the cache is replaced: either its values have changed, or a concurrent insert has just written the same
    /// row, and then the time of writing is correct anyway.
    auto now = std::chrono::steady_clock::now();
    for (const auto & [key_hash, row_hash] : pending_rows.rows)
    {
        WrittenRow written_row{.row_hash = row_hash, .generation = pending_rows.generation, .written_at = now};
        cache.set(key_hash, std::make_shared<WrittenRow>(written_row));
    }
}

void TimeSeriesDeduplicationCache::setLimits(size_t max_size_bytes, UInt64 expiration_seconds_)
{
    cache.setMaxSizeInBytes(max_size_bytes);
    max_pending_rows = max_size_bytes / APPROXIMATE_ENTRY_SIZE;
    expiration_seconds = expiration_seconds_;
}

void TimeSeriesDeduplicationCache::clear()
{
    ++current_generation;
    cache.clear();
}

}
