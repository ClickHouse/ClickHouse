#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <base/unaligned.h>

#include <optional>
#include <random>


namespace DB
{

namespace
{
    /// An entry lives at least this part of the expiration period: the rest is cut off by a random amount,
    /// so the rows written together don't expire together.
    constexpr Float64 MIN_LIFETIME_RATIO = 0.8;

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

    struct IDComponentReader
    {
        enum class ValueKind : UInt8
        {
            UInt64,
            UInt128,
            UUID,
            Fixed16,
        };

        enum class IndexKind : UInt8
        {
            Direct,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
        };

        const void * values = nullptr;
        const void * indexes = nullptr;
        ValueKind value_kind = ValueKind::UInt64;
        IndexKind index_kind = IndexKind::Direct;

        static std::optional<IDComponentReader> tryCreate(const IColumn & column)
        {
            IDComponentReader reader;
            const IColumn * value_column = &column;

            if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column))
            {
                if (lc->nestedIsNullable())
                    return std::nullopt;

                value_column = lc->getDictionary().getNestedColumn().get();
                const IColumn & idx_col = lc->getIndexes();
                switch (lc->getSizeOfIndexType())
                {
                    case sizeof(UInt8):
                        reader.indexes = assert_cast<const ColumnUInt8 &>(idx_col).getData().data();
                        reader.index_kind = IndexKind::UInt8;
                        break;
                    case sizeof(UInt16):
                        reader.indexes = assert_cast<const ColumnUInt16 &>(idx_col).getData().data();
                        reader.index_kind = IndexKind::UInt16;
                        break;
                    case sizeof(UInt32):
                        reader.indexes = assert_cast<const ColumnUInt32 &>(idx_col).getData().data();
                        reader.index_kind = IndexKind::UInt32;
                        break;
                    case sizeof(UInt64):
                        reader.indexes = assert_cast<const ColumnUInt64 &>(idx_col).getData().data();
                        reader.index_kind = IndexKind::UInt64;
                        break;
                    default:
                        return std::nullopt;
                }
            }

            if (const auto * col_u64 = typeid_cast<const ColumnUInt64 *>(value_column))
            {
                reader.values = col_u64->getData().data();
                reader.value_kind = ValueKind::UInt64;
                return reader;
            }
            if (const auto * col_uuid = typeid_cast<const ColumnUUID *>(value_column))
            {
                reader.values = col_uuid->getData().data();
                reader.value_kind = ValueKind::UUID;
                return reader;
            }
            if (const auto * col_u128 = typeid_cast<const ColumnUInt128 *>(value_column))
            {
                reader.values = col_u128->getData().data();
                reader.value_kind = ValueKind::UInt128;
                return reader;
            }
            if (const auto * col_fixed = typeid_cast<const ColumnFixedString *>(value_column);
                col_fixed && (col_fixed->getN() == 16))
            {
                reader.values = col_fixed->getChars().data();
                reader.value_kind = ValueKind::Fixed16;
                return reader;
            }

            return std::nullopt;
        }

        [[nodiscard]] ALWAYS_INLINE UInt128 read(size_t row) const
        {
            size_t pos = row;
            switch (index_kind)
            {
                case IndexKind::Direct:
                    break;
                case IndexKind::UInt8:
                    pos = static_cast<const UInt8 *>(indexes)[row];
                    break;
                case IndexKind::UInt16:
                    pos = static_cast<const UInt16 *>(indexes)[row];
                    break;
                case IndexKind::UInt32:
                    pos = static_cast<const UInt32 *>(indexes)[row];
                    break;
                case IndexKind::UInt64:
                    pos = static_cast<const UInt64 *>(indexes)[row];
                    break;
            }

            switch (value_kind)
            {
                case ValueKind::UInt64:
                {
                    UInt128 res{};
                    res.items[UInt128::_impl::little(0)] = static_cast<const UInt64 *>(values)[pos];
                    return res;
                }
                case ValueKind::UUID:
                    return static_cast<const UUID *>(values)[pos].toUnderType();
                case ValueKind::UInt128:
                    return static_cast<const UInt128 *>(values)[pos];
                case ValueKind::Fixed16:
                    return unalignedLoad<UInt128>(static_cast<const UInt8 *>(values) + pos * 16);
            }
        }
    };

    ALWAYS_INLINE std::pair<UInt128, UInt128> makeTupleHashes(UInt128 first, UInt128 second)
    {
        constexpr size_t lo = UInt128::_impl::little(0);
        constexpr size_t hi = UInt128::_impl::little(1);

        UInt64 first_lo = first.items[lo];
        UInt64 first_hi = first.items[hi];

        UInt128 key_hash;
        key_hash.items[lo] = second.items[lo] ^ intHash64(first_lo) ^ first_hi;
        key_hash.items[hi] = second.items[hi] ^ first_lo;

        return {key_hash, first};
    }

    template <typename IndexType, typename EmitRow>
    void hashTupleUInt64LowCardinalityUUIDRows(
        const UInt64 * first_values,
        const UUID * dict_values,
        const IndexType * indexes,
        const PaddedPODArray<UInt8> & filter,
        size_t num_rows,
        EmitRow && emit_row)
    {
        for (size_t row = 0; row != num_rows; ++row)
        {
            if (!filter[row])
                continue;

            UInt128 first{};
            first.items[UInt128::_impl::little(0)] = first_values[row];
            UInt128 second = dict_values[indexes[row]].toUnderType();
            auto [key_hash, row_hash] = makeTupleHashes(first, second);
            emit_row(key_hash, row_hash, row);
        }
    }

    template <typename EmitRow>
    bool tryHashRowsFromDefaultID(
        const IColumn & key_column,
        const PaddedPODArray<UInt8> & filter,
        size_t num_rows,
        EmitRow && emit_row)
    {
        if (const auto * tuple = typeid_cast<const ColumnTuple *>(&key_column); tuple && (tuple->tupleSize() == 2))
        {
            const IColumn & first_col = tuple->getColumn(0);
            const IColumn & second_col = tuple->getColumn(1);

            if (const auto * first_u64 = typeid_cast<const ColumnUInt64 *>(&first_col))
            {
                if (const auto * second_lc = typeid_cast<const ColumnLowCardinality *>(&second_col);
                    second_lc && !second_lc->nestedIsNullable())
                {
                    if (const auto * dict_uuid = typeid_cast<const ColumnUUID *>(second_lc->getDictionary().getNestedColumn().get()))
                    {
                        const UInt64 * first_values = first_u64->getData().data();
                        const UUID * dict_values = dict_uuid->getData().data();
                        const IColumn & idx_col = second_lc->getIndexes();
                        switch (second_lc->getSizeOfIndexType())
                        {
                            case sizeof(UInt8):
                                hashTupleUInt64LowCardinalityUUIDRows(
                                    first_values, dict_values, assert_cast<const ColumnUInt8 &>(idx_col).getData().data(),
                                    filter, num_rows, emit_row);
                                return true;
                            case sizeof(UInt16):
                                hashTupleUInt64LowCardinalityUUIDRows(
                                    first_values, dict_values, assert_cast<const ColumnUInt16 &>(idx_col).getData().data(),
                                    filter, num_rows, emit_row);
                                return true;
                            case sizeof(UInt32):
                                hashTupleUInt64LowCardinalityUUIDRows(
                                    first_values, dict_values, assert_cast<const ColumnUInt32 &>(idx_col).getData().data(),
                                    filter, num_rows, emit_row);
                                return true;
                            case sizeof(UInt64):
                                hashTupleUInt64LowCardinalityUUIDRows(
                                    first_values, dict_values, assert_cast<const ColumnUInt64 &>(idx_col).getData().data(),
                                    filter, num_rows, emit_row);
                                return true;
                            default:
                                break;
                        }
                    }
                }
            }

            auto first_reader = IDComponentReader::tryCreate(first_col);
            auto second_reader = first_reader ? IDComponentReader::tryCreate(second_col) : std::nullopt;
            if (!first_reader || !second_reader)
                return false;

            for (size_t row = 0; row != num_rows; ++row)
            {
                if (!filter[row])
                    continue;

                auto [key_hash, row_hash] = makeTupleHashes(first_reader->read(row), second_reader->read(row));
                emit_row(key_hash, row_hash, row);
            }
            return true;
        }

        auto reader = IDComponentReader::tryCreate(key_column);
        if (!reader)
            return false;

        for (size_t row = 0; row != num_rows; ++row)
        {
            if (!filter[row])
                continue;

            emit_row(reader->read(row), UInt128{}, row);
        }
        return true;
    }
}

TimeSeriesDeduplicationCache::TimeSeriesDeduplicationCache(
    size_t max_size_bytes_,
    UInt64 expiration_seconds_,
    CurrentMetrics::Metric entries_metric,
    CurrentMetrics::Metric bytes_metric,
    ProfileEvents::Event hits_event_,
    ProfileEvents::Event misses_event_,
    Clock clock_,
    bool key_identifies_row_)
    : cache(bytes_metric, entries_metric, max_size_bytes_)
    , expiration_seconds(expiration_seconds_)
    , max_pending_rows(max_size_bytes_ / APPROXIMATE_ENTRY_SIZE)
    , hits_event(hits_event_)
    , misses_event(misses_event_)
    , clock(std::move(clock_))
    , key_identifies_row(key_identifies_row_)
{
}

TimeSeriesDeduplicationCache::TimePoint TimeSeriesDeduplicationCache::currentTime() const
{
    return clock ? clock() : std::chrono::steady_clock::now();
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
    const Block & block, size_t key_column_index, const PaddedPODArray<UInt8> & filter) const
{
    size_t num_rows = block.rows();
    HashedRows hashed_rows;
    hashed_rows.reserve(num_rows);

    if (key_identifies_row)
    {
        ColumnPtr full_key_column = block.getByPosition(key_column_index).column->convertToFullColumnIfConst();
        full_key_column = recursiveRemoveSparse(full_key_column);
        const IColumn & key_column = *full_key_column;

        auto emit_row = [&](KeyHash key_hash, RowHash row_hash, size_t row)
        {
            hashed_rows.push_back({.key_hash = key_hash, .row_hash = row_hash, .row_in_filter = row});
        };

        if (tryHashRowsFromDefaultID(key_column, filter, num_rows, emit_row))
            return hashed_rows;

        for (size_t row = 0; row != num_rows; ++row)
        {
            if (!filter[row])
                continue;

            SipHash key_hash_calculator;
            key_column.updateHashWithValue(row, key_hash_calculator);
            hashed_rows.push_back({.key_hash = key_hash_calculator.get128(), .row_hash = RowHash{}, .row_in_filter = row});
        }
        return hashed_rows;
    }

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
    auto now = currentTime();

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

TimeSeriesDeduplicationCache::TimePoint TimeSeriesDeduplicationCache::getExpirationTime(TimePoint now, Float64 lifetime_ratio) const
{
    /// `steady_clock` counts nanoseconds in `Int64`, so an expiration period which doesn't fit means the entry never expires.
    auto max_lifetime_seconds = std::chrono::duration_cast<std::chrono::seconds>(TimePoint::max() - now).count();
    if (expiration_seconds >= static_cast<UInt64>(max_lifetime_seconds))
        return TimePoint::max();

    std::chrono::duration<Float64> lifetime{static_cast<Float64>(expiration_seconds) * lifetime_ratio};
    return now + std::chrono::duration_cast<TimePoint::duration>(lifetime);
}

bool TimeSeriesDeduplicationCache::isExpired(const WrittenRow & written_row, TimePoint now) const
{
    return now >= written_row.expiration_time;
}

void TimeSeriesDeduplicationCache::markRowsAsWritten(PendingRows && pending_rows_)
{
    PendingRows pending_rows = std::move(pending_rows_);

    std::vector<KeyHash> key_hashes;
    std::vector<Cache::MappedPtr> written_rows;
    key_hashes.reserve(pending_rows.size());
    written_rows.reserve(pending_rows.size());

    /// A row already in the cache is replaced: either its values have changed, or a concurrent insert has just written the same
    /// row, and then the time of writing is correct anyway.
    auto now = currentTime();
    std::uniform_real_distribution<Float64> lifetime_ratio_distribution(MIN_LIFETIME_RATIO, 1);
    for (const auto & [key_hash, row_hash] : pending_rows)
    {
        Float64 lifetime_ratio = lifetime_ratio_distribution(thread_local_rng);
        key_hashes.push_back(key_hash);
        written_rows.push_back(std::make_shared<WrittenRow>(
            WrittenRow{.row_hash = row_hash, .expiration_time = getExpirationTime(now, lifetime_ratio)}));
    }

    cache.setMany(key_hashes, written_rows);
}

}
