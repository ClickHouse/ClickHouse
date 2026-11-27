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
    if (num_rows == 0)
        return block.cloneEmpty();

    if (!filter.empty() && (countBytesInFilter(filter) == 0))
        return block.cloneEmpty();

    if (filter.empty())
        filter.resize_fill(num_rows, true);
    chassert(filter.size() == num_rows);

    size_t num_passed_rows = excludeWrittenRowsFromFilter(block, key_column_index, filter, pending_rows);
    return applyFilter(block, filter, num_passed_rows);
}

size_t TimeSeriesDeduplicationCache::excludeWrittenRowsFromFilter(
    const Block & block, size_t key_column_index, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows)
{
    auto hashed_rows = hashRows(block, key_column_index, filter);

    std::vector<KeyHash> key_hashes;
    key_hashes.reserve(hashed_rows.size());
    for (const auto & hashed_row : hashed_rows)
        key_hashes.push_back(hashed_row.key_hash);
    auto written_rows = cache.getMany(key_hashes);

    size_t num_hits = excludeDuplicateRowsFromFilter(hashed_rows, written_rows, filter, pending_rows);
    trimPendingRows(hashed_rows, written_rows, pending_rows, filter);

    size_t num_misses = hashed_rows.size() - num_hits;
    ProfileEvents::increment(hits_event, num_hits);
    ProfileEvents::increment(misses_event, num_misses);
    return num_misses;
}

TimeSeriesDeduplicationCache::HashedRows TimeSeriesDeduplicationCache::hashRows(
    const Block & block, size_t key_column_index, const PaddedPODArray<UInt8> & filter)
{
    size_t num_rows = block.rows();
    HashedRows hashed_rows;
    hashed_rows.reserve(num_rows);

    const auto & key_column = *block.getByPosition(key_column_index).column;
    for (size_t row = 0; row != num_rows; ++row)
    {
        if (!filter[row])
            continue;

        SipHash key_hash_calculator;
        key_column.updateHashWithValue(row, key_hash_calculator);

        SipHash row_hash_calculator;
        for (const auto & column : block)
            column.column->updateHashWithValue(row, row_hash_calculator);

        hashed_rows.push_back({.key_hash = key_hash_calculator.get128(), .row_hash = row_hash_calculator.get128(), .row_in_filter = row});
    }
    return hashed_rows;
}

size_t TimeSeriesDeduplicationCache::excludeDuplicateRowsFromFilter(
    const HashedRows & hashed_rows, const WrittenRows & written_rows, PaddedPODArray<UInt8> & filter, PendingRows & pending_rows) const
{
    size_t num_excluded_rows = 0;
    auto now = std::chrono::steady_clock::now();

    for (size_t i = 0; i != hashed_rows.size(); ++i)
    {
        const auto & [key_hash, row_hash, row_in_filter] = hashed_rows[i];

        /// The row of a key pending in this insert is compared with the pending row, not with the row written by an earlier insert.
        auto pending_it = pending_rows.find(key_hash);
        if (pending_it != pending_rows.end())
        {
            auto & pending_row_hash = pending_it->second;
            if (pending_row_hash == row_hash)
            {
                /// The same row is already pending, so it's skipped.
                filter[row_in_filter] = false;
                ++num_excluded_rows;
            }
            else
            {
                /// The row has other values than the pending row of the key, so it's written and becomes the pending row.
                pending_row_hash = row_hash;
            }
            continue;
        }

        /// The same row was written by an earlier insert and its entry hasn't expired, so it's skipped.
        const auto & written_row = written_rows[i];
        if (written_row && (written_row->row_hash == row_hash) && !isExpired(*written_row, now))
        {
            filter[row_in_filter] = false;
            ++num_excluded_rows;
            continue;
        }

        /// The row is new or its values have changed, so it's written and becomes the pending row of the key.
        pending_rows.emplace(key_hash, row_hash);
    }

    return num_excluded_rows;
}

void TimeSeriesDeduplicationCache::trimPendingRows(
    const HashedRows & hashed_rows, const WrittenRows & written_rows, PendingRows & pending_rows, const PaddedPODArray<UInt8> & filter)
{
    /// The map is capped by the capacity of the cache, see PendingRows.
    for (size_t i = hashed_rows.size(); (i > 0) && (pending_rows.size() > max_pending_rows); --i)
    {
        const auto & hashed_row = hashed_rows[i - 1];

        /// A duplicate row doesn't count, a key is forgotten by its last written row.
        if (!filter[hashed_row.row_in_filter])
            continue;

        if (pending_rows.erase(hashed_row.key_hash) && written_rows[i - 1])
            cache.remove(hashed_row.key_hash);
    }
}

bool TimeSeriesDeduplicationCache::isExpired(const WrittenRow & written_row, TimePoint now) const
{
    /// The elapsed time is compared in seconds, so that any value of the setting fits without overflow.
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - written_row.written_at).count();
    return (elapsed_seconds >= 0) && (static_cast<UInt64>(elapsed_seconds) >= expiration_seconds);
}

void TimeSeriesDeduplicationCache::markRowsAsWritten(PendingRows && pending_rows_)
{
    PendingRows pending_rows = std::move(pending_rows_);

    /// A row already in the cache is replaced: either its values have changed, or a concurrent insert has just written the same
    /// row, and then the time of writing is correct anyway.
    auto now = std::chrono::steady_clock::now();
    for (const auto & [key_hash, row_hash] : pending_rows)
    {
        WrittenRow written_row{.row_hash = row_hash, .written_at = now};
        cache.set(key_hash, std::make_shared<WrittenRow>(written_row));
    }
}

}
