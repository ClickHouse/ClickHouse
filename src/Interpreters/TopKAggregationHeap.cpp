#include <Interpreters/TopKAggregationHeap.h>

#include <algorithm>
#include <array>
#include <utility>

#include <base/StringViewHash.h>
#include <base/memcmpSmall.h>
#include <base/unaligned.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/HashTable/Hash.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Processors/TopKThresholdTracker.h>

namespace DB
{

/// Exact keys the collated boundary rejected. The effective boundary only tightens while the heap is active, so a
/// rejected key stays rejected and its repeats need no locale comparison. Holds at most `slot_count` keys and
/// `pool_capacity` bytes. A window of lookups with few hits empties the cache and bypasses it for a while.
struct TopKRejectedKeyCache
{
    static constexpr size_t slot_count = 2048;
    static constexpr size_t max_probes = 8;
    static constexpr size_t pool_capacity = 8192;
    static constexpr size_t max_key_size = 255;
    static constexpr UInt32 window_size = 4096;
    static constexpr UInt64 min_bypass_rows = 1 << 16;
    static constexpr UInt64 max_bypass_rows = 1 << 24;

    /// A used slot holds the upper half of the key hash and 1 + the offset of the key in `pool`; 0 marks a free one.
    std::array<UInt32, slot_count> slots{};
    /// Per key: its size byte, then its bytes. Padded for `memequalSmallAllowOverflow15`.
    std::array<char, pool_capacity + 15> pool{};
    size_t pool_used = 0;

    bool active = true;
    UInt32 window_lookups = 0;
    UInt32 window_hits = 0;
    UInt64 bypass_rows = 0;
    UInt64 next_bypass_rows = min_bypass_rows;

    /// `ColumnString` pads its chars, so the 8 bytes from the start of any of its keys are readable.
    [[nodiscard]] static ALWAYS_INLINE UInt32 hashKey(std::string_view key)
    {
        if (key.size() > 8)
            return static_cast<UInt32>(StringViewHash{}(key));
        UInt64 word = unalignedLoadLittleEndian<UInt64>(key.data());
        if (key.size() < 8)
            word &= (1ULL << (key.size() * 8)) - 1;
        return static_cast<UInt32>(intHashCRC32(word, key.size()));
    }

    [[nodiscard]] ALWAYS_INLINE bool contains(std::string_view key, UInt32 hash) const
    {
        const UInt32 tag = hash & 0xFFFF0000;
        for (size_t i = 0; i < max_probes; ++i)
        {
            const UInt32 slot = slots[(hash + i) % slot_count];
            if (slot == 0)
                return false;
            if ((slot & 0xFFFF0000) != tag)
                continue;
            const char * entry = pool.data() + (slot & 0xFFFF) - 1;
            if (memequalSmallAllowOverflow15(entry + 1, static_cast<UInt8>(entry[0]), key.data(), key.size()))
                return true;
        }
        return false;
    }

    void insert(std::string_view key, UInt32 hash)
    {
        if (pool_used + 1 + key.size() > pool_capacity)
            return;
        for (size_t i = 0; i < max_probes; ++i)
        {
            UInt32 & slot = slots[(hash + i) % slot_count];
            if (slot != 0)
                continue;
            slot = (hash & 0xFFFF0000) | static_cast<UInt32>(pool_used + 1);
            pool[pool_used] = static_cast<char>(key.size());
            memcpy(pool.data() + pool_used + 1, key.data(), key.size());
            pool_used += 1 + key.size();
            return;
        }
    }

    /// Whether the boundary rejects the row; `above` compares it with the boundary when the cache cannot tell.
    template <typename Above>
    ALWAYS_INLINE bool rejects(std::string_view key, size_t row, UInt32 & hits, Above && above)
    {
        const UInt32 hash = hashKey(key);
        if (contains(key, hash))
        {
            chassert(above(row));
            ++hits;
            return true;
        }
        const bool rejected = above(row);
        if (rejected)
            insert(key, hash);
        return rejected;
    }

    /// Marks the rows of `[begin, end)` that the boundary rejects, a window of lookups at a time.
    template <typename Above>
    void fill(const ColumnString & column, UInt8 * bitmap, size_t begin, size_t end, Above && above)
    {
        const auto * offsets = column.getOffsets().data();
        const char * chars = reinterpret_cast<const char *>(column.getChars().data());
        size_t row = begin;
        while (row < end && active)
        {
            const size_t chunk_end = std::min<size_t>(end, row + (window_size - window_lookups));
            UInt32 lookups = 0;
            UInt32 hits = 0;
            for (; row < chunk_end; ++row)
            {
                const size_t offset = offsets[static_cast<ssize_t>(row) - 1];
                const std::string_view key(chars + offset, offsets[row] - offset);
                if (key.size() > max_key_size)
                {
                    bitmap[row] = above(row);
                    continue;
                }
                ++lookups;
                bitmap[row] = rejects(key, row, hits, above);
            }
            countLookups(lookups, hits);
        }
        for (; row < end; ++row)
            bitmap[row] = above(row);
    }

    ALWAYS_INLINE void countLookups(UInt32 lookups, UInt32 hits)
    {
        window_lookups += lookups;
        window_hits += hits;
        if (window_lookups < window_size)
            return;
        if (window_hits * 4 < window_lookups)
        {
            slots = {};
            pool_used = 0;
            active = false;
            bypass_rows = next_bypass_rows;
            next_bypass_rows = std::min(next_bypass_rows * 2, max_bypass_rows);
        }
        else
            next_bypass_rows = min_bypass_rows;
        window_lookups = 0;
        window_hits = 0;
    }

    void advance(UInt64 rows)
    {
        if (active)
            return;
        if (rows < bypass_rows)
        {
            bypass_rows -= rows;
            return;
        }
        bypass_rows = 0;
        active = true;
    }
};

TopKAggregationHeapBase::TopKAggregationHeapBase() = default;
TopKAggregationHeapBase::~TopKAggregationHeapBase() = default;
TopKAggregationHeapBase::TopKAggregationHeapBase(TopKAggregationHeapBase &&) noexcept = default;
TopKAggregationHeapBase & TopKAggregationHeapBase::operator=(TopKAggregationHeapBase &&) noexcept = default;

size_t TopKAggregationHeapBase::initBase(
    const ColumnRawPtrs & key_columns,
    size_t heap_key_count,
    size_t total_group_by_keys,
    size_t query_k,
    const VectorWithMemoryTracking<int> & dirs,
    const VectorWithMemoryTracking<int> & null_dirs,
    const VectorWithMemoryTracking<std::shared_ptr<Collator>> & cols,
    UInt64 observation_rows)
{
    is_prefix_mode = heap_key_count < total_group_by_keys;

    if (heap_key_count == 1)
    {
        init(
            *key_columns[0],
            query_k,
            dirs.empty() ? 1 : dirs[0],
            null_dirs.empty() ? 1 : null_dirs[0],
            cols);
    }
    else
    {
        ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
        init(heap_cols, query_k, dirs, null_dirs, cols);
    }

    /// The window must cover at least one full fill of the set plus as
    /// much again, so the skip rate is judged on a set that had a chance
    /// to establish its boundary.
    profitability_window = observation_rows == 0 ? 0 : std::max<UInt64>(observation_rows, 2 * next_trim_size);

    return next_trim_size + 1;
}

void TopKAggregationHeapBase::setK(size_t query_k)
{
    k = query_k;
    trim_slack = std::max<size_t>(1, k / 2);
    next_trim_size = k + trim_slack;
    tie_overflow_limit = k + max_tie_rows;
}

void TopKAggregationHeapBase::init(
    const IColumn & source_column,
    size_t query_k,
    int direction,
    int nulls_direction,
    const VectorWithMemoryTracking<std::shared_ptr<Collator>> & cols)
{
    directions = {direction};
    nulls_directions = {nulls_direction};
    collators = {cols.empty() ? nullptr : cols[0]};
    has_collators = collators[0] != nullptr;
    is_composite = false;
    setK(query_k);
    heap_column = source_column.cloneEmpty();
    const size_t reserve_hint = next_trim_size + 1;
    heap_column->reserve(reserve_hint);
    heap_indices.clear();
    heap_indices.reserve(reserve_hint);
    boundary_row = invalid_row;
    boundary_is_shared = shared_cache_column != nullptr;
    key_arena.reset();
    findLowCardinalityColumns();

    const TypeIndex type = heap_column->getDataType();
    typed_key_type = !has_collators && dispatchNumericKeyType(type, []<typename>() {}) ? type : TypeIndex::Nothing;

    if (has_collators && typeid_cast<const ColumnString *>(heap_column.get()))
        rejected_keys = std::make_unique<TopKRejectedKeyCache>();
    else
        rejected_keys.reset();
}

void TopKAggregationHeapBase::init(
    const ColumnRawPtrs & source_columns,
    size_t query_k,
    const VectorWithMemoryTracking<int> & dirs,
    const VectorWithMemoryTracking<int> & null_dirs,
    const VectorWithMemoryTracking<std::shared_ptr<Collator>> & cols)
{
    const size_t n = source_columns.size();
    is_composite = true;
    setK(query_k);

    directions.assign(n, 1);
    for (size_t i = 0; i < dirs.size() && i < n; ++i)
        directions[i] = dirs[i];

    nulls_directions.assign(n, 1);
    for (size_t i = 0; i < null_dirs.size() && i < n; ++i)
        nulls_directions[i] = null_dirs[i];

    collators.assign(n, nullptr);
    for (size_t i = 0; i < cols.size() && i < n; ++i)
        collators[i] = cols[i];
    has_collators = std::ranges::any_of(collators, [](const auto & collator) { return collator != nullptr; });

    MutableColumns sub_columns;
    sub_columns.reserve(n);
    for (const auto * col : source_columns)
        sub_columns.emplace_back(col->cloneEmpty());
    heap_column = ColumnTuple::create(std::move(sub_columns));
    const size_t reserve_hint = next_trim_size + 1;
    heap_column->reserve(reserve_hint);

    heap_indices.clear();
    heap_indices.reserve(reserve_hint);
    boundary_row = invalid_row;
    boundary_is_shared = shared_cache_column != nullptr;
    key_arena.reset();
    findLowCardinalityColumns();
    typed_key_type = TypeIndex::Nothing;
    rejected_keys.reset();
}

bool TopKAggregationHeapBase::shouldFreeze() const
{
    if (frozen || !heap_column)
        return false;

    const UInt64 window_observed = observed_rows - window_start_observed_rows;
    const UInt64 window_skipped = skipped_rows - window_start_skipped_rows;
    if (profitability_window && window_observed >= profitability_window
        && static_cast<Float64>(window_skipped) / static_cast<Float64>(window_observed) < 0.1 && evicted_keys < k
        && heap_indices.size() >= k)
        return true;

    return tie_overflow;
}

void TopKAggregationHeapBase::freezeBase()
{
    frozen = true;
    heap_column = nullptr;
    heap_indices = {};
    boundary_row = invalid_row;
    boundary_is_shared = false;
    shared_cache_column = nullptr;
    publish_pending = false;
    key_arena.reset();
    rejected_keys.reset();
    skip_bitmap = {};
    skip_compare_rows = {};
    skip_compare_results = {};
    batch_columns = {};
    batch_typed_data = nullptr;
    evicted_rows = {};
    trim_filter = {};
    trim_old_to_new = {};
}

const UInt8 * TopKAggregationHeapBase::startBatch(const ColumnRawPtrs & key_columns, size_t begin, size_t end)
{
    chassert(!frozen);
    batch_columns.assign(key_columns.begin(), key_columns.begin() + directions.size());
    batch_typed_data = nullptr;
    dispatchNumericKeyType(typed_key_type, [&]<typename T>()
    {
        batch_typed_data = assert_cast<const ColumnVector<T> &>(*batch_columns[0]).getData().data();
    });
    batch_end = end;
    /// Sized to the whole batch once, so that a later `fillSkipBitmap` keeps the pointer valid.
    skip_bitmap.resize(end);

    skip_bitmap_filled = hasBoundary();
    skip_bitmap_stale = !skip_bitmap_filled;
    if (skip_bitmap_filled)
        fillSkipBitmap(begin, end);
    else
        std::fill(skip_bitmap.data() + begin, skip_bitmap.data() + end, 0);
    return skip_bitmap.data();
}

bool TopKAggregationHeapBase::recheckRow(size_t row)
{
    if (!skip_bitmap_filled)
    {
        fillSkipBitmap(row, batch_end);
        skip_bitmap_filled = true;
        skip_bitmap_stale = false;
        return skip_bitmap[row];
    }
    return aboveBoundary(row);
}

void TopKAggregationHeapBase::fillSkipBitmap(size_t begin, size_t end)
{
    chassert(hasBoundary());
    UInt8 * bitmap = skip_bitmap.data();

    const bool typed = dispatchNumericKeyType(typed_key_type, [&]<typename T>()
    {
        const T * src = static_cast<const T *>(batch_typed_data);
        const T boundary = assert_cast<const ColumnVector<T> &>(boundaryColumn()).getData()[boundaryRow()];
        const int nulls_direction = nulls_directions[0];
        /// A row is skipped when `direction * compare(row, boundary) > 0`. The direction is 1 or -1, so this is
        /// `greater` or `less`, chosen once outside of the loop. clang 23 turns the three-way compare into a
        /// `ucmp`/`scmp` intrinsic, which the x86 backend expands to a scalar `cmp`/`sbb` per row instead of
        /// a vector compare (see `compareColumnImpl` in `ColumnVector.cpp`).
        if (directions[0] > 0)
        {
            for (size_t i = begin; i < end; ++i)
                bitmap[i] = CompareHelper<T>::greater(src[i], boundary, nulls_direction);
        }
        else
        {
            for (size_t i = begin; i < end; ++i)
                bitmap[i] = CompareHelper<T>::less(src[i], boundary, nulls_direction);
        }
    });
    if (typed)
        return;

    /// `compareColumn` has no collation.
    if (rejected_keys)
    {
        rejected_keys->fill(
            assert_cast<const ColumnString &>(*batch_columns[0]), bitmap, begin, end,
            [this](size_t row) { return aboveBoundaryImpl<true>(row); });
        return;
    }
    if (has_collators)
    {
        for (size_t i = begin; i < end; ++i)
            bitmap[i] = aboveBoundaryImpl<true>(i);
        return;
    }

    /// One virtual `compareColumn` per key column for the whole range. Each column compares only the rows tied with
    /// the boundary on the previous ones; the rows tied on all of them are left at 0, so they are kept.
    skip_compare_rows.resize(end - begin);
    iota(skip_compare_rows.data(), skip_compare_rows.size(), UInt64(begin));
    skip_compare_results.resize(batch_columns[0]->size());
    const IColumn & boundary = boundaryColumn();
    for (size_t i = 0; i < batch_columns.size() && !skip_compare_rows.empty(); ++i)
    {
        const IColumn & boundary_column = is_composite ? assert_cast<const ColumnTuple &>(boundary).getColumn(i) : boundary;
        batch_columns[i]->compareColumn(
            boundary_column, boundaryRow(), &skip_compare_rows, skip_compare_results, directions[i], nulls_directions[i]);
    }
    for (size_t i = begin; i < end; ++i)
        bitmap[i] = skip_compare_results[i] > 0;
}

bool TopKAggregationHeapBase::aboveBoundary(size_t row) const
{
    /// A numeric key is compared directly, without the virtual `compareAt`.
    bool above = false;
    const bool typed = dispatchNumericKeyType(typed_key_type, [&]<typename T>()
    {
        const T value = static_cast<const T *>(batch_typed_data)[row];
        const T boundary_value = assert_cast<const ColumnVector<T> &>(boundaryColumn()).getData()[boundaryRow()];
        above = directions[0] * CompareHelper<T>::compare(value, boundary_value, nulls_directions[0]) > 0;
    });
    if (typed)
        return above;
    return aboveBoundaryGeneric(row);
}

bool TopKAggregationHeapBase::aboveBoundaryGeneric(size_t row) const
{
    if (has_collators)
        return aboveBoundaryCollated(row);
    return aboveBoundaryImpl<false>(row);
}

template <bool with_collation>
bool TopKAggregationHeapBase::aboveBoundaryImpl(size_t row) const
{
    const IColumn & boundary = boundaryColumn();
    const size_t boundary_row_index = boundaryRow();

    if (!is_composite)
        return directions[0] * compareColumns<with_collation>(*batch_columns[0], row, boundary, boundary_row_index, 0) > 0;

    const auto & tuple = assert_cast<const ColumnTuple &>(boundary);
    for (size_t i = 0; i < batch_columns.size(); ++i)
    {
        const int cmp = compareColumns<with_collation>(*batch_columns[i], row, tuple.getColumn(i), boundary_row_index, i);
        if (cmp != 0)
            return directions[i] * cmp > 0;
    }
    return false;
}

bool TopKAggregationHeapBase::aboveBoundaryCollated(size_t row) const
{
    if (!rejected_keys || !rejected_keys->active)
        return aboveBoundaryImpl<true>(row);

    const std::string_view key = assert_cast<const ColumnString &>(*batch_columns[0]).getDataAt(row);
    if (key.size() > TopKRejectedKeyCache::max_key_size)
        return aboveBoundaryImpl<true>(row);

    UInt32 hits = 0;
    const bool rejected = rejected_keys->rejects(key, row, hits, [this](size_t r) { return aboveBoundaryImpl<true>(r); });
    rejected_keys->countLookups(1, hits);
    return rejected;
}

void TopKAggregationHeapBase::initBoundary()
{
    withComparator([&](auto cmp) { boundary_row = *std::max_element(heap_indices.begin(), heap_indices.end(), cmp); });
    boundary_changed = true;
    publish_pending = true;
    updateBoundaryChoice();
}

void TopKAggregationHeapBase::advanceRejectedKeys(UInt64 rows)
{
    rejected_keys->advance(rows);
}

int TopKAggregationHeapBase::compareRanked(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row) const
{
    if (!is_composite)
        return directions[0] * compareColumns(lhs, lhs_row, rhs, rhs_row, 0);

    const auto & lhs_tuple = assert_cast<const ColumnTuple &>(lhs);
    const auto & rhs_tuple = assert_cast<const ColumnTuple &>(rhs);
    for (size_t i = 0; i < directions.size(); ++i)
    {
        const int cmp = compareColumns(lhs_tuple.getColumn(i), lhs_row, rhs_tuple.getColumn(i), rhs_row, i);
        if (cmp != 0)
            return directions[i] * cmp;
    }
    return 0;
}

void TopKAggregationHeapBase::exchangeSharedBoundary()
{
    if (!shared_boundary || frozen)
        return;

    const bool publish = std::exchange(publish_pending, false);

    if (!publish && shared_boundary->version.load(std::memory_order_acquire) == shared_version_seen)
        return;

    {
        std::lock_guard lock(shared_boundary->mutex);

        if (publish && (!shared_boundary->key || compareRanked(*heap_column, boundary_row, *shared_boundary->key, 0) < 0))
        {
            auto key = heap_column->cloneEmpty();
            key->insertFrom(*heap_column, boundary_row);
            /// An aggregate state column copies a row by referencing the source column. The heap appends to its column in
            /// place, which needs it unshared, so the key takes its own copy of the row.
            key->ensureOwnership();
            shared_boundary->key = std::move(key);
            shared_boundary->version.fetch_add(1, std::memory_order_release);
        }

        const UInt64 version = shared_boundary->version.load(std::memory_order_relaxed);
        if (version != shared_version_seen)
        {
            chassert(shared_boundary->key);
            shared_cache_column = shared_boundary->key->cloneResized(1);
            shared_version_seen = version;
        }
    }

    updateBoundaryChoice();

    /// A window judged entirely on the local boundary says nothing about the shared one, which
    /// is what lets a thread whose local keys rank poorly skip. Restart it once, when the shared
    /// boundary first takes effect, so such a thread is not frozen right before it would pay
    /// off. Only once: a shared boundary that keeps tightening without producing skips must not
    /// postpone the freeze forever.
    if (boundary_is_shared && !window_restarted_for_shared)
    {
        window_restarted_for_shared = true;
        window_start_observed_rows = observed_rows;
        window_start_skipped_rows = skipped_rows;
    }
}

void TopKAggregationHeapBase::updateBoundaryChoice()
{
    skip_bitmap_stale = true;

    if (!shared_cache_column)
    {
        boundary_is_shared = false;
        return;
    }

    if (boundary_row == invalid_row)
    {
        boundary_is_shared = true;
        return;
    }

    boundary_is_shared = compareRanked(*shared_cache_column, 0, *heap_column, boundary_row) < 0;
}

void TopKAggregationHeapBase::publishBoundary()
{
    boundary_changed = false;

    if (!threshold_tracker || !heap_column || boundary_row == invalid_row)
        return;

    const IColumn & first_column = is_composite ? assert_cast<const ColumnTuple &>(*heap_column).getColumn(0) : *heap_column;

    Field boundary_value;
    first_column.get(boundary_row, boundary_value);
    threshold_tracker->testAndSet(boundary_value);
}

void TopKAggregationHeapBase::trimToK()
{
    evicted_rows.clear();

    if (heap_indices.size() <= k)
        return;

    withComparator([&](auto cmp)
    {
        std::nth_element(heap_indices.begin(), heap_indices.begin() + k - 1, heap_indices.end(), cmp);
        boundary_row = heap_indices[k - 1];
        boundary_changed = true;

        const auto tied_with_boundary
            = [&](size_t idx) { return !cmp(idx, boundary_row) && !cmp(boundary_row, idx); };
        const auto evict_begin = std::partition(heap_indices.begin() + k, heap_indices.end(), tied_with_boundary);

        evicted_rows.assign(evict_begin, heap_indices.end());
        heap_indices.erase(evict_begin, heap_indices.end());
    });

    evicted_keys += evicted_rows.size();

    publish_pending = true;
    updateBoundaryChoice();

    if (heap_indices.size() > next_trim_size)   /// a boundary tie-set blocked the trim
    {
        next_trim_size = heap_indices.size() + std::max(trim_slack, heap_indices.size() / 2);
        if (heap_indices.size() > tie_overflow_limit)
            tie_overflow = true;
    }
}

bool TopKAggregationHeapBase::startCompaction()
{
    const size_t col_size = heap_column->size();
    if (col_size <= heap_indices.size())
        return false;

    trim_filter.clear();
    trim_filter.resize_fill(col_size, 0);

    for (size_t idx : heap_indices)
        trim_filter[idx] = 1;

    trim_old_to_new.resize(col_size);
    size_t new_idx = 0;
    for (size_t i = 0; i < col_size; ++i)
    {
        if (!trim_filter[i])
            continue;
        trim_old_to_new[i] = new_idx;
        ++new_idx;
    }

    return true;
}

void TopKAggregationHeapBase::finishCompaction()
{
    heap_column->filter(trim_filter);
    compactDictionaries();

    for (auto & idx : heap_indices)
        idx = trim_old_to_new[idx];
    boundary_row = trim_old_to_new[boundary_row];
}

void TopKAggregationHeapBase::compactDictionaries()
{
    if (low_cardinality_columns.empty())
        return;

    if (is_composite)
    {
        auto & tuple = assert_cast<ColumnTuple &>(*heap_column);
        for (size_t i : low_cardinality_columns)
            assert_cast<ColumnLowCardinality &>(tuple.getColumn(i)).compactDictionaryInplace();
    }
    else
        assert_cast<ColumnLowCardinality &>(*heap_column).compactDictionaryInplace();
}

void TopKAggregationHeapBase::findLowCardinalityColumns()
{
    low_cardinality_columns.clear();

    if (is_composite)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < tuple.tupleSize(); ++i)
            if (typeid_cast<const ColumnLowCardinality *>(&tuple.getColumn(i)))
                low_cardinality_columns.push_back(i);
    }
    else if (typeid_cast<const ColumnLowCardinality *>(heap_column.get()))
        low_cardinality_columns.push_back(0);
}

int TopKAggregationHeapBase::compareHeapRowsComposite(size_t a, size_t b) const
{
    const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
    for (size_t i = 0; i < directions.size(); ++i)
    {
        const auto & col = tuple.getColumn(i);
        const int cmp = compareColumns(col, a, col, b, i);
        if (cmp != 0)
            return directions[i] * cmp;
    }
    return 0;
}

bool TopKAggregationHeapBase::GenericComparator::operator()(size_t a, size_t b) const
{
    if (owner->is_composite)
        return owner->compareHeapRowsComposite(a, b) < 0;

    const int cmp = owner->compareColumns(*owner->heap_column, a, *owner->heap_column, b, 0);
    return owner->directions[0] * cmp < 0;
}

}
