#include <Interpreters/TopKAggregationHeap.h>

#include <algorithm>
#include <utility>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnTuple.h>
#include <Common/typeid_cast.h>

namespace DB
{

size_t TopKAggregationHeapBase::initBase(
    const ColumnRawPtrs & key_columns,
    size_t heap_key_count,
    size_t total_group_by_keys,
    size_t query_k,
    const std::vector<int> & dirs,
    const std::vector<int> & null_dirs,
    UInt64 observation_rows)
{
    is_prefix_mode = heap_key_count < total_group_by_keys;

    if (heap_key_count == 1)
    {
        init(
            *key_columns[0],
            query_k,
            dirs.empty() ? 1 : dirs[0],
            null_dirs.empty() ? 1 : null_dirs[0]);
    }
    else
    {
        ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
        init(heap_cols, query_k, dirs, null_dirs);
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
    int nulls_direction)
{
    directions = {direction};
    nulls_directions = {nulls_direction};
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
    typed_key_type = dispatchNumericKeyType(type, []<typename>() {}) ? type : TypeIndex::Nothing;
}

void TopKAggregationHeapBase::init(
    const ColumnRawPtrs & source_columns,
    size_t query_k,
    const std::vector<int> & dirs,
    const std::vector<int> & null_dirs)
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

    if (profitability_window && observed_rows >= profitability_window
        && static_cast<Float64>(skipped_rows) / static_cast<Float64>(observed_rows) < 0.1 && evicted_keys < k
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
    skip_bitmap = {};
    evicted_rows = {};
    trim_filter = {};
    trim_old_to_new = {};
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
    publish_pending = true;
    updateBoundaryChoice();
}

int TopKAggregationHeapBase::compareRanked(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row) const
{
    if (!is_composite)
        return directions[0] * lhs.compareAt(lhs_row, rhs_row, rhs, nulls_directions[0]);

    const auto & lhs_tuple = assert_cast<const ColumnTuple &>(lhs);
    const auto & rhs_tuple = assert_cast<const ColumnTuple &>(rhs);
    for (size_t i = 0; i < directions.size(); ++i)
    {
        const int cmp = lhs_tuple.getColumn(i).compareAt(lhs_row, rhs_row, rhs_tuple.getColumn(i), nulls_directions[i]);
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

void TopKAggregationHeapBase::trimToK()
{
    evicted_rows.clear();

    if (heap_indices.size() <= k)
        return;

    withComparator([&](auto cmp)
    {
        std::nth_element(heap_indices.begin(), heap_indices.begin() + k - 1, heap_indices.end(), cmp);
        boundary_row = heap_indices[k - 1];

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
