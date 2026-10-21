#include <Interpreters/TopKAggregationHeap.h>

#include <algorithm>
#include <utility>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnTuple.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Processors/TopKThresholdTracker.h>

namespace DB
{

size_t TopKAggregationHeapBase::initBase(
    const ColumnRawPtrs & key_columns,
    size_t heap_key_count,
    size_t total_group_by_keys,
    size_t query_k,
    const std::vector<int> & dirs,
    const std::vector<int> & null_dirs,
    const std::vector<std::shared_ptr<Collator>> & cols,
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
    const std::vector<std::shared_ptr<Collator>> & cols)
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
}

void TopKAggregationHeapBase::init(
    const ColumnRawPtrs & source_columns,
    size_t query_k,
    const std::vector<int> & dirs,
    const std::vector<int> & null_dirs,
    const std::vector<std::shared_ptr<Collator>> & cols)
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
}

bool TopKAggregationHeapBase::shouldFreeze() const
{
    if (frozen || !heap_column)
        return false;

    /// A heap with a `COLLATE` key tracks only the keys inserted after its late start, so it may never fill.
    const UInt64 window_observed = observed_rows - window_start_observed_rows;
    const UInt64 window_skipped = skipped_rows - window_start_skipped_rows;
    if (profitability_window && window_observed >= profitability_window
        && static_cast<Float64>(window_skipped) / static_cast<Float64>(window_observed) < 0.1 && evicted_keys < k
        && (heap_indices.size() >= k || has_collators))
        return true;

    return tie_overflow;
}

/// A heap with a `COLLATE` key tests every row with a locale comparison, which pays off only while the rows it skips
/// would have inserted new keys: a skipped repeat saves just a lookup into a table that stays small. So once it has
/// filled, the heap hashes each block's keys and estimates the share of rows it keeps out of the table as new keys:
/// over 1 key in 2^`collated_sample_bits` by hash, whose skipped rows count when the sample lacks the key, and bounded
/// by the rows whose hash is not in a direct-mapped table of `collated_recent_key_slots` recent hashes, which sees a
/// few hot keys the sample misses. A window of `collated_sample_window` sampled rows (2^`collated_sample_bits` times as many rows when
/// only the second estimate decides) with either below `collated_min_new_key_share` extends a run of repeats; the heap
/// freezes once the run reaches `collated_repeat_rows_per_skipped_key` rows per key skipped so far, because freezing
/// inserts those keys after all. Past `collated_sample_max_keys` sampled keys (about 2M skipped) only the recent hashes
/// of 1/`collated_block_slices_after_sample` of each block decide: a skipped key outside them would cost a lookup into
/// a large table, which the comparison wins.
/// Measured on 20M rows, `LIMIT 10`, 'en', heap off / without this check / with it: 1 thread, 100K keys in cycles
/// 268 / 494 / 274 ms, 1M 315 / 466 / 340 ms, 1M in descending order 328 / 740 / 324 ms, 5 keys after 100K unique ones
/// 140 / 331 / 106 ms; 16 threads, 1M keys in cycles 106 / 40 / 48 ms (as with the heap off when the run needs no
/// length). Past the sample, heap off / on: 1M keys in cycles after 3M unique ones 844 / 419 ms, and 97M rows of one
/// key after them 896 / 596 ms (1632 ms without the recent hashes).
static constexpr UInt32 collated_sample_bits = 6;
static constexpr UInt64 collated_sample_window = 256;
static constexpr double collated_min_new_key_share = 0.5;
static constexpr double collated_repeat_rows_per_skipped_key = 0.5;
static constexpr size_t collated_sample_max_keys = 32768;
static constexpr size_t collated_recent_key_slots = 4096;
static constexpr size_t collated_block_slices_after_sample = 8;

bool TopKAggregationHeapBase::sampleCollatedSkips(const ColumnRawPtrs & key_columns, size_t row_begin, size_t row_end)
{
    if (frozen || !heap_column || !profitability_window || observed_rows < 2 * next_trim_size || !hasBoundary()
        || row_end <= row_begin)
        return false;

    /// Past the sample, a slice of each block stands for all of it; the slice moves through the block from block to block.
    size_t begin = row_begin;
    size_t rows = row_end - row_begin;
    UInt64 weight = 1;
    if (skip_sample_done)
    {
        rows = std::max<size_t>(1, rows / collated_block_slices_after_sample);
        begin = std::min<size_t>(row_begin + (blocks_after_sample++ % collated_block_slices_after_sample) * rows, row_end - 1);
        weight = collated_block_slices_after_sample;
    }

    sample_window_rows += rows * weight;
    sample_hashes.resize(rows);
    for (size_t i = 0; i < key_columns.size(); ++i)
        key_columns[i]->computeHashInto(begin, begin + rows, sample_hashes.data(), i == 0);

    if (!skip_sample_done && !skipped_key_sample)
        skipped_key_sample = std::make_unique<HashSet<UInt32>>();

    /// Direct-mapped by the low bits; odd tags, so that an empty slot never matches.
    if (recent_key_slots.empty())
        recent_key_slots.resize(collated_recent_key_slots);
    const size_t slot_mask = recent_key_slots.size() - 1;

    const ColumnRawPtrs ranked_columns(key_columns.begin(), key_columns.begin() + directions.size());
    UInt64 recent_repeats_in_block = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        const UInt32 hash = sample_hashes[i];
        UInt32 & slot = recent_key_slots[hash & slot_mask];
        recent_repeats_in_block += slot == (hash | 1);
        slot = hash | 1;

        if (skip_sample_done || hash >> (32 - collated_sample_bits))
            continue;
        ++sample_rows;
        if (shouldSkipCollated(ranked_columns, begin + i))
            sample_new_keys += skipped_key_sample->insert(hash).second;
    }
    sample_recent_repeats += recent_repeats_in_block * weight;

    if (!skip_sample_done && skipped_key_sample->size() >= collated_sample_max_keys)
    {
        skip_sample_done = true;
        skipped_key_sample.reset();
        sample_rows = 0;
        sample_new_keys = 0;
    }

    const bool recent_repeats = static_cast<double>(sample_window_rows - sample_recent_repeats)
        < collated_min_new_key_share * static_cast<double>(sample_window_rows);
    const bool enough_rows = sample_window_rows >= (collated_sample_window << collated_sample_bits);
    /// Past the sample, a window covers whole rotations of the slice, so that it sees every part of the blocks.
    const bool window_done = skip_sample_done
        ? enough_rows && blocks_after_sample % collated_block_slices_after_sample == 0
        : sample_rows >= collated_sample_window || (recent_repeats && enough_rows);
    if (!window_done)
        return false;

    const bool repeats = recent_repeats
        || (!skip_sample_done
            && static_cast<double>(sample_new_keys) < collated_min_new_key_share * static_cast<double>(sample_rows));
    repeat_rows = repeats ? repeat_rows + sample_window_rows : 0;
    sample_rows = 0;
    sample_new_keys = 0;
    sample_window_rows = 0;
    sample_recent_repeats = 0;
    const size_t sampled_keys = skip_sample_done ? collated_sample_max_keys : skipped_key_sample->size();
    return repeats
        && static_cast<double>(repeat_rows)
        >= collated_repeat_rows_per_skipped_key * static_cast<double>(sampled_keys << collated_sample_bits);
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
    skip_bitmap = {};
    evicted_rows = {};
    trim_filter = {};
    trim_old_to_new = {};
    skipped_key_sample.reset();
    sample_hashes = {};
    recent_key_slots = {};
}

const UInt8 * TopKAggregationHeapBase::fillSkipBitmap(const void * source_typed_data, size_t begin, size_t end)
{
    chassert(!frozen);
    if (!source_typed_data)
        return nullptr;
    const UInt8 * result = nullptr;
    dispatchNumericKeyType(typed_key_type, [&]<typename T>()
    {
        chassert(hasBoundary());
        skip_bitmap.resize(end);
        const auto * src = reinterpret_cast<const T *>(source_typed_data);
        const auto & boundary_data = assert_cast<const ColumnVector<T> &>(boundaryColumn()).getData();
        const T boundary = boundary_data[boundaryRow()];
        const int direction = directions[0];
        const int nulls_direction = nulls_directions[0];
        for (size_t i = begin; i < end; ++i)
            skip_bitmap[i] = direction * CompareHelper<T>::compare(src[i], boundary, nulls_direction) > 0;
        result = skip_bitmap.data();
    });
    return result;
}

void TopKAggregationHeapBase::initBoundary()
{
    withComparator([&](auto cmp) { boundary_row = *std::max_element(heap_indices.begin(), heap_indices.end(), cmp); });
    boundary_changed = true;
    publish_pending = true;
    updateBoundaryChoice();
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

bool TopKAggregationHeapBase::shouldSkipCollated(const ColumnRawPtrs & source_columns, size_t source_row) const
{
    if (is_composite)
        return sourceAboveBoundaryComposite<true>(source_columns, source_row);
    return sourceAboveBoundary<true>(*source_columns[0], source_row);
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
