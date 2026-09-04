#pragma once

#include <vector>
#include <algorithm>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Common/assert_cast.h>
#include <Common/VectorWithMemoryTracking.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#include <Interpreters/JIT/compileFunction.h>
#endif

namespace DB
{

class Block;
using IColumnPermutation = PaddedPODArray<size_t>;

/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct SortCursorImpl
{
    ColumnRawPtrs sort_columns;
    ColumnRawPtrs all_columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    size_t rows = 0;

    /** Determines order if comparing columns are equal.
      * Order is determined by number of cursor.
      *
      * Cursor number (always?) equals to number of merging part.
      * Therefore this field can be used to determine part number of current row (see ColumnGathererStream).
      */
    size_t order = 0;

    using NeedCollationFlags = VectorWithMemoryTracking<UInt8>;

    /** Should we use Collator to sort a column? */
    NeedCollationFlags need_collation;

    /** Is there at least one column with Collator. */
    bool has_collation = false;

    /** We could use SortCursorImpl in case when columns aren't sorted
      *  but we have their sorted permutation
      */
    IColumnPermutation * permutation = nullptr;

#if USE_EMBEDDED_COMPILER
    VectorWithMemoryTracking<ColumnData> raw_sort_columns_data;
#endif

    SortCursorImpl() = default;

    SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0, IColumnPermutation * perm = nullptr)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        reset(block, perm);
    }

    SortCursorImpl(
        const Block & header,
        const Columns & columns,
        size_t num_rows,
        const SortDescription & desc_,
        size_t order_ = 0,
        IColumnPermutation * perm = nullptr)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        reset(columns, header, num_rows, perm);
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block, IColumnPermutation * perm = nullptr);

    /// Set the cursor to the beginning of the new block.
    void reset(const Columns & columns, const Block & block, UInt64 num_rows, IColumnPermutation * perm = nullptr);

    size_t getRow() const
    {
        if (permutation)
            return (*permutation)[pos];
        return pos;
    }

    /// We need a possibility to change pos (see MergeJoin).
    size_t & getPosRef() { return pos; }
    size_t getPos() const { return pos; }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    bool isLast(size_t size) const { return pos + size >= rows; }
    bool isValid() const { return pos < rows; }

    void next() { ++pos; }
    void next(size_t size) { pos += size; }

    size_t getSize() const { return rows; }
    size_t rowsLeft() const { return rows - pos; }

/// Prevent using pos instead of getRow()
private:
    size_t pos = 0;
};

using SortCursorImpls = VectorWithMemoryTracking<SortCursorImpl>;


/// For easy copying.
template <typename Derived>
struct SortCursorHelper
{
    SortCursorImpl * impl;

    const Derived & derived() const { return static_cast<const Derived &>(*this); }

    explicit SortCursorHelper(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator-> () { return impl; }
    const SortCursorImpl * operator-> () const { return impl; }

    bool ALWAYS_INLINE greater(const SortCursorHelper & rhs) const
    {
        return derived().greaterAt(rhs.derived(), impl->getRow(), rhs.impl->getRow());
    }

    bool ALWAYS_INLINE greaterWithOffset(const SortCursorHelper & rhs, size_t lhs_offset, size_t rhs_offset) const
    {
        return derived().greaterAt(rhs.derived(), impl->getRow() + lhs_offset, rhs.impl->getRow() + rhs_offset);
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool ALWAYS_INLINE operator< (const SortCursorHelper & rhs) const
    {
        return derived().greater(rhs.derived());
    }

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool ALWAYS_INLINE totallyLessOrEquals(const SortCursorHelper & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !derived().greaterAt(rhs.derived(), impl->rows - 1, 0);
    }

    bool ALWAYS_INLINE totallyLess(const SortCursorHelper & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is less than the first row of the another cursor.
        return rhs.derived().template greaterAt<false>(derived(), 0, impl->rows - 1);
    }
};


struct SortCursor : SortCursorHelper<SortCursor>
{
    using SortCursorHelper<SortCursor>::SortCursorHelper;

    /// The specified row of this cursor is greater than the specified row of another cursor.
    template <bool consider_order = true>
    bool ALWAYS_INLINE greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
#if USE_EMBEDDED_COMPILER
        if (impl->desc.compiled_sort_description && rhs.impl->desc.compiled_sort_description)
        {
            chassert(impl->raw_sort_columns_data.size() == rhs.impl->raw_sort_columns_data.size());

            auto sort_description_func_typed = reinterpret_cast<JITSortDescriptionFunc>(impl->desc.compiled_sort_description);
            /// JIT-compiled functions lack the type metadata prologue that UBSan's
            /// -fsanitize=function expects before every indirect call. When the JIT
            /// code sits at a page boundary the pre-call read hits unmapped memory.
            /// NOLINTNEXTLINE(bugprone-signed-char-misuse,cert-str34-c) -- JIT comparator returns -1/0/1, sign is meaningful
            int res = callJITFunction(sort_description_func_typed, lhs_pos, rhs_pos, impl->raw_sort_columns_data.data(), rhs.impl->raw_sort_columns_data.data());

            if (res > 0)
                return true;
            if (res < 0)
                return false;

            if constexpr (consider_order)
                return impl->order > rhs.impl->order;
            else
                return false;
        }
#endif

        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            const auto & desc = impl->desc[i];
            int direction = desc.direction;
            int nulls_direction = desc.nulls_direction;
            int res = direction * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);

            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }

        if constexpr (consider_order)
            return impl->order > rhs.impl->order;
        else
            return false;
    }
};


/// For the case with a single column and when there is no order between different cursors.
struct SimpleSortCursor : SortCursorHelper<SimpleSortCursor>
{
    using SortCursorHelper<SimpleSortCursor>::SortCursorHelper;

    template <bool consider_order = true>
    bool ALWAYS_INLINE greaterAt(const SimpleSortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        int res = 0;

#if USE_EMBEDDED_COMPILER
        if (impl->desc.compiled_sort_description && rhs.impl->desc.compiled_sort_description)
        {
            chassert(impl->raw_sort_columns_data.size() == rhs.impl->raw_sort_columns_data.size());

            auto sort_description_func_typed = reinterpret_cast<JITSortDescriptionFunc>(impl->desc.compiled_sort_description);
            res = callJITFunction(sort_description_func_typed, lhs_pos, rhs_pos, impl->raw_sort_columns_data.data(), rhs.impl->raw_sort_columns_data.data()); // NOLINT(bugprone-signed-char-misuse,cert-str34-c)
        }
        else
#endif
        {
            const auto & desc = impl->desc[0];
            int direction = desc.direction;
            int nulls_direction = desc.nulls_direction;
            res = direction * impl->sort_columns[0]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[0]), nulls_direction);
        }

        if constexpr (consider_order)
            return res ? res > 0 : impl->order > rhs.impl->order;
        else
            return res > 0;
    }
};

template <typename ColumnType>
struct SpecializedSingleColumnSortCursor : SortCursorHelper<SpecializedSingleColumnSortCursor<ColumnType>>
{
    using SortCursorHelper<SpecializedSingleColumnSortCursor>::SortCursorHelper;

    template <bool consider_order = true>
    bool ALWAYS_INLINE greaterAt(const SortCursorHelper<SpecializedSingleColumnSortCursor> & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        auto & this_impl = this->impl;

        auto & lhs_columns = this_impl->sort_columns;
        auto & rhs_columns = rhs.impl->sort_columns;

        chassert(lhs_columns.size() == 1);
        chassert(rhs_columns.size() == 1);

        const auto & lhs_column = assert_cast<const ColumnType &>(*lhs_columns[0]);
        const auto & rhs_column = assert_cast<const ColumnType &>(*rhs_columns[0]);

        const auto & desc = this->impl->desc[0];

        int res = desc.direction * lhs_column.compareAt(lhs_pos, rhs_pos, rhs_column, desc.nulls_direction);

        if constexpr (consider_order)
            return res ? res > 0 : this_impl->order > rhs.impl->order;
        else
            return res > 0;
    }
};

template <typename ColumnType>
struct SpecializedSingleNullableColumnSortCursor : SortCursorHelper<SpecializedSingleNullableColumnSortCursor<ColumnType>>
{
    using SortCursorHelper<SpecializedSingleNullableColumnSortCursor>::SortCursorHelper;

    template <bool consider_order = true>
    bool ALWAYS_INLINE greaterAt(const SortCursorHelper<SpecializedSingleNullableColumnSortCursor> & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        auto & this_impl = this->impl;

        auto & lhs_columns = this_impl->sort_columns;
        auto & rhs_columns = rhs.impl->sort_columns;

        chassert(lhs_columns.size() == 1);
        chassert(rhs_columns.size() == 1);

        const auto & lhs_column = assert_cast<const ColumnNullable &>(*lhs_columns[0]);
        const auto & rhs_column = assert_cast<const ColumnNullable &>(*rhs_columns[0]);
        const auto & lhs_nullmap = lhs_column.getNullMapData();
        const auto & rhs_nullmap = rhs_column.getNullMapData();
        const auto & denull_lhs_column = assert_cast<const ColumnType &>(lhs_column.getNestedColumn());
        const auto & denull_rhs_column = assert_cast<const ColumnType &>(rhs_column.getNestedColumn());

        const auto & desc = this->impl->desc[0];

        auto get_compare_result = [&]() -> int
        {
            bool lval_is_null = lhs_nullmap[lhs_pos];
            bool rval_is_null = rhs_nullmap[rhs_pos];

            if (unlikely(lval_is_null || rval_is_null))
            {
                if (lval_is_null && rval_is_null)
                    return 0;
                return lval_is_null ? desc.nulls_direction : -desc.nulls_direction;
            }

            return denull_lhs_column.compareAt(lhs_pos, rhs_pos, denull_rhs_column, desc.nulls_direction);
        };


        int res = desc.direction * get_compare_result();
        if constexpr (consider_order)
            return res ? res > 0 : this_impl->order > rhs.impl->order;
        else
            return res > 0;
    }
};


/// Separate comparator for locale-sensitive string comparisons
struct SortCursorWithCollation : SortCursorHelper<SortCursorWithCollation>
{
    using SortCursorHelper<SortCursorWithCollation>::SortCursorHelper;

    template <bool consider_order = true>
    bool ALWAYS_INLINE greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            const auto & desc = impl->desc[i];
            int direction = desc.direction;
            int nulls_direction = desc.nulls_direction;
            int res = 0;
            if (impl->need_collation[i])
                res = impl->sort_columns[i]->compareAtWithCollation(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction, *impl->desc[i].collator);
            else
                res = impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);

            res *= direction;
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        if constexpr (consider_order)
            return impl->order > rhs.impl->order;
        else
            return false;
    }
};

enum class SortingQueueStrategy : uint8_t
{
    Default,
    Batch
};

enum class SortingQueueContainer : uint8_t
{
    /// Binary heap of cursors. The minimum cursor is identified in O(1) comparisons (it is at
    /// the front), reinsertion of the advanced front cursor costs up to 2 * log2(K) comparisons
    /// (only one comparison when the front cursor is still the minimum, which is the common
    /// case for clustered data).
    Heap,

    /// Sorted array of cursors. Reinsertion of the advanced front cursor costs one comparison
    /// when it is still the minimum and otherwise ceil(log2(K - 1)) further comparisons (a binary
    /// search among the other cursors) plus a shift of on average half of the array. The shifted
    /// elements are plain pointers, so the shift is cheap and this container
    /// wins when comparisons are expensive relative to the data structure maintenance: generic
    /// cursors compare through a virtual call per column (possibly over several columns),
    /// strings, or a collation. With too many cursors the shifts start to dominate, so above
    /// `max_array_container_size` cursors the implementation falls back to the heap behavior
    /// (a sorted array is a valid heap layout, which makes the switch free).
    Array
};

/// Comparisons through these cursors are expensive: a loop with a virtual call per column
/// (possibly over several columns), or a collation. For them the number of comparisons matters
/// more than the data structure maintenance costs, and the sorted array container performs
/// fewer of them than the heap (see `SortingQueueContainer`). Measured on 64-way merges of
/// non-clustered data: the multi-column cursor is 7-8% faster with the sorted array, while the
/// specialized single-column cursors (including strings, whose comparisons usually resolve
/// within the first bytes) are faster with the heap.
template <typename Cursor>
inline constexpr bool sort_cursor_compare_is_expensive = false;

template <> inline constexpr bool sort_cursor_compare_is_expensive<SortCursor> = true;
template <> inline constexpr bool sort_cursor_compare_is_expensive<SortCursorWithCollation> = true;

/// The runtime counterpart of `sort_cursor_compare_is_expensive` for code that always merges
/// through the generic `SortCursor` regardless of the sort description (the special merge
/// algorithms: Replacing/Summing/Aggregating/Collapsing/VersionedCollapsing/Graphite).
/// Mirrors the cursor selection in `SortQueueVariants`: a single-column description without
/// a collation would get a specialized cursor with a cheap comparator there, so such merges
/// should keep the heap container even though the cursor type says otherwise.
inline bool sortDescriptionCompareIsExpensive(const SortDescription & description)
{
    if (description.size() > 1)
        return true;

    for (const auto & column_description : description)
        if (column_description.collator)
            return true;

    return false;
}

/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data streams).
template <typename Cursor, SortingQueueStrategy strategy, SortingQueueContainer container = SortingQueueContainer::Heap>
class SortingQueueImpl
{
public:
    SortingQueueImpl() = default;

    /// `enable_array_container` allows to keep the heap behavior at runtime when the actual
    /// comparator is known to be cheap even though the cursor type is marked expensive (e.g.
    /// a single-column sorting key merged through the generic `SortCursor` by the special
    /// merge algorithms). It only matters for the Array container.
    ///
    /// `enable_batch_detection` (only for the Batch strategy) allows to skip the detection of
    /// batches longer than one row: `current` then reports a batch of one row (or the whole
    /// remainder of the last cursor), and the queue is restructured after every row exactly
    /// like with the Default strategy. This is for clients that can not make use of batches
    /// and merge through a cheap comparator: for them the detection is pure overhead when the
    /// keys are interleaved between the cursors.
    template <typename Cursors>
    explicit SortingQueueImpl(Cursors & cursors, bool enable_array_container = true, bool enable_batch_detection = true)
    {
        if constexpr (strategy == SortingQueueStrategy::Batch)
            batch_detection_enabled = enable_batch_detection;

        size_t size = cursors.size();
        queue.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            if (cursors[i].empty())
                continue;

            queue.emplace_back(&cursors[i]);
        }

        if constexpr (container == SortingQueueContainer::Array)
        {
            /// The decision is based on the number of sources rather than the number of
            /// non-empty cursors: `push` re-adds sources that were empty here or removed
            /// later, so the queue can grow up to the number of sources.
            array_mode = enable_array_container && size <= max_array_container_size;
        }

        if (isArrayMode())
            std::sort(queue.begin(), queue.end(), [](const Cursor & lhs, const Cursor & rhs) { return rhs.greater(lhs); });
        else
            std::make_heap(queue.begin(), queue.end());

        if constexpr (strategy == SortingQueueStrategy::Batch)
        {
            if (!queue.empty())
                updateBatchSize();
        }
    }

    bool isValid() const { return !queue.empty(); }

    Cursor & current() requires (strategy == SortingQueueStrategy::Default)
    {
        return queue.front();
    }

    std::pair<Cursor *, size_t> current() requires (strategy == SortingQueueStrategy::Batch)
    {
        return {&queue.front(), batch_size};
    }

    size_t size() { return queue.size(); }

    Cursor & nextChild() { return queue[nextChildIndex()]; }

    void ALWAYS_INLINE next() requires (strategy == SortingQueueStrategy::Default)
    {
        chassert(isValid());

        if (!queue.front()->isLast())
        {
            queue.front()->next();
            updateTop(true /*check_in_order*/);
        }
        else
        {
            removeTop();
        }
    }

    void ALWAYS_INLINE next(size_t batch_size_value) requires (strategy == SortingQueueStrategy::Batch)
    {
        chassert(isValid());
        chassert(batch_size_value <= batch_size);
        chassert(batch_size_value > 0);

        batch_size -= batch_size_value;
        if (batch_size > 0)
        {
            queue.front()->next(batch_size_value);
            return;
        }

        if (!queue.front()->isLast(batch_size_value))
        {
            queue.front()->next(batch_size_value);
            /// The batch detection has already established that the next row of the front
            /// cursor is not less than the second cursor's row, so no need to check it again.
            /// Without the detection the order is unknown and must be checked.
            updateTop(/*check_in_order=*/ !batch_detection_enabled);
        }
        else
        {
            removeTop();
        }
    }

    void replaceTop(Cursor new_top)
    {
        queue.front() = new_top;
        updateTop(true /*check_in_order*/);
    }

    void removeTop()
    {
        if (isArrayMode())
        {
            queue.erase(queue.begin());
        }
        else
        {
            std::pop_heap(queue.begin(), queue.end());
            queue.pop_back();
            next_child_idx = 0;
        }

        if constexpr (strategy == SortingQueueStrategy::Batch)
        {
            if (queue.empty())
                batch_size = 0;
            else
                updateBatchSize();
        }
    }

    void push(SortCursorImpl & cursor)
    {
        if (isArrayMode())
        {
            Cursor new_cursor(&cursor);
            queue.insert(queue.begin() + arrayUpperBound(new_cursor, 0), new_cursor);

            if constexpr (container == SortingQueueContainer::Array)
            {
                /// A sorted array is a valid heap layout, so the switch is free.
                if (queue.size() > max_array_container_size)
                    array_mode = false;
            }
        }
        else
        {
            queue.emplace_back(&cursor);
            std::push_heap(queue.begin(), queue.end());
            next_child_idx = 0;
        }

        if constexpr (strategy == SortingQueueStrategy::Batch)
            updateBatchSize();
    }

private:
    using Container = VectorWithMemoryTracking<Cursor>;
    Container queue;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_child_idx = 0;
    size_t batch_size = 0;

    /// Whether batches longer than one row are detected (only with the Batch strategy, see the constructor).
    bool batch_detection_enabled = true;

    /// Whether the queue currently operates as a sorted array (only with the Array container,
    /// see `SortingQueueContainer`). Above `max_array_container_size` cursors the shifts of the
    /// sorted array become noticeable, so the queue switches to the heap behavior.
    static constexpr size_t max_array_container_size = 256;
    bool array_mode = false;

    bool ALWAYS_INLINE isArrayMode() const
    {
        if constexpr (container == SortingQueueContainer::Heap)
            return false;
        else
            return array_mode;
    }

    /// The first position in [first, queue.size()) whose cursor is greater than the given one.
    size_t ALWAYS_INLINE arrayUpperBound(const Cursor & cursor, size_t first) const
    {
        size_t lo = first;
        size_t hi = queue.size();
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (queue[mid].greater(cursor))
                hi = mid;
            else
                lo = mid + 1;
        }
        return lo;
    }

    size_t ALWAYS_INLINE nextChildIndex()
    {
        if (isArrayMode())
            return 1;

        if (next_child_idx == 0)
        {
            next_child_idx = 1;

            if (queue.size() > 2 && queue[1].greater(queue[2]))
                ++next_child_idx;
        }

        return next_child_idx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void ALWAYS_INLINE updateTop(bool check_in_order)
    {
        size_t size = queue.size();
        if (size < 2)
            return;

        if (isArrayMode())
        {
            /// Check if the front cursor is still the minimum.
            if (check_in_order && queue[1].greater(queue[0]))
            {
                if constexpr (strategy == SortingQueueStrategy::Batch)
                    updateBatchSize();
                return;
            }

            /// The front cursor is known to be not less than the second one: either the check
            /// above has just failed, or the caller has established it (the batch strategy ends
            /// a batch exactly when the next row of the front cursor is not less than the second
            /// cursor's row). Reinsert the advanced front cursor at its sorted position among the
            /// remaining ones: ceil(log2(size - 1)) comparisons, then a shift of plain pointers.
            /// With two cursors this costs no comparisons at all.
            Cursor top = queue[0];
            size_t upper = arrayUpperBound(top, 2);
            if (upper == 2)
            {
                /// The common case of interleaved keys (and the only case with two cursors):
                /// a swap instead of a call to `memmove` for a single element.
                std::swap(queue[0], queue[1]);
            }
            else
            {
                std::move(queue.begin() + 1, queue.begin() + upper, queue.begin());
                queue[upper - 1] = top;
            }

            if constexpr (strategy == SortingQueueStrategy::Batch)
                updateBatchSize();
            return;
        }

        auto begin = queue.begin();

        size_t child_idx = nextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (check_in_order && (*child_it).greater(*begin))
        {
            if constexpr (strategy == SortingQueueStrategy::Batch)
                updateBatchSize();
            return;
        }

        next_child_idx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size)
                break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && (*child_it).greater(*(child_it + 1)))
            {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!((*child_it).greater(top)));
        *curr_it = std::move(top);

        if constexpr (strategy == SortingQueueStrategy::Batch)
            updateBatchSize();
    }

    /// Update batch size of elements that client can extract from current cursor.
    /// The common cases are handled inline: a single cursor takes its whole remainder, and a
    /// batch of one row (interleaved keys) costs exactly one comparison - the same comparison the
    /// heap would spend to notice that the advanced front cursor is not the minimum anymore, so
    /// the batch strategy is never more expensive than the default one here. Longer batches are
    /// detected out of line, so that the hot loops of the merging algorithms stay compact.
    void ALWAYS_INLINE updateBatchSize()
    {
        chassert(!queue.empty());

        auto & begin_cursor = *queue.begin();
        size_t rows_left = begin_cursor->getSize() - begin_cursor->getPosRef();

        if (queue.size() == 1)
        {
            batch_size = rows_left;
            return;
        }

        batch_size = 1;
        if (!batch_detection_enabled)
            return;

        auto & next_child_cursor = *(queue.begin() + nextChildIndex());

        if (batch_size < rows_left && next_child_cursor.greaterWithOffset(begin_cursor, 0, batch_size))
            updateBatchSizeSlow(begin_cursor, next_child_cursor, rows_left);
    }

    /// The second row of the front cursor is known to be extractable too: find the end of the batch.
    void NO_INLINE updateBatchSizeSlow(const Cursor & begin_cursor, const Cursor & next_child_cursor, size_t rows_left)
    {
        batch_size = 2;

        /// Linear detection at most 16 elements to quickly find a small batch size.
        /// This heuristic helps to avoid the overhead of the checks below for small batches.
        constexpr size_t max_linear_detection = 16;
        size_t i = 0;
        while (i < max_linear_detection && batch_size < rows_left
               && next_child_cursor.greaterWithOffset(begin_cursor, 0, batch_size))
        {
            ++batch_size;
            ++i;
        }

        if (i < max_linear_detection || batch_size == rows_left)
            return;

        /// The batch is larger than the linear detection limit. Check the last row of the
        /// cursor: if even it is less than the next cursor's current row, the whole remainder
        /// forms one batch. This makes merging cursors with non-intersecting key ranges (e.g.
        /// parts sorted by the primary key that do not overlap) cost O(1) comparisons here
        /// instead of a binary search over the remainder. When the check does not hit, it
        /// costs one comparison and excludes the last row from the search below.
        if (next_child_cursor.greaterWithOffset(begin_cursor, 0, rows_left - 1))
        {
            batch_size = rows_left;
            return;
        }

        /// Galloping detection: bracket the end of the batch with exponentially growing steps.
        /// Compared to an immediate binary search over the whole remainder, this costs
        /// O(log(batch size)) comparisons on rows near the current position instead of
        /// O(log(rows left)) comparisons on far rows that miss caches - which matters when the
        /// batch is much smaller than the block. The last row is already known to be not
        /// extractable, so it bounds the bracket.
        size_t start_offset = batch_size;
        size_t end_offset = rows_left - 1;
        for (size_t step = max_linear_detection; start_offset + step < end_offset; step *= 2)
        {
            if (!next_child_cursor.greaterWithOffset(begin_cursor, 0, start_offset + step))
            {
                end_offset = start_offset + step;
                break;
            }
            start_offset += step + 1;
        }

        /// Binary search for the exact end of the batch inside the bracketed range.
        while (start_offset < end_offset)
        {
            size_t mid_offset = start_offset + (end_offset - start_offset) / 2;
            if (next_child_cursor.greaterWithOffset(begin_cursor, 0, mid_offset))
                start_offset = mid_offset + 1;
            else
                end_offset = mid_offset;
        }
        batch_size = start_offset;
    }
};

template <typename Cursor>
using SortingQueue = SortingQueueImpl<Cursor, SortingQueueStrategy::Default>;

template <typename Cursor>
using SortingQueueBatch = SortingQueueImpl<Cursor, SortingQueueStrategy::Batch>;

/// The queue type used for the given cursor: cursors with expensive comparators get the sorted
/// array container, the rest get the heap (see `SortingQueueContainer`).
template <typename Cursor, SortingQueueStrategy strategy>
using SortingQueueForCursor = SortingQueueImpl<
    Cursor,
    strategy,
    sort_cursor_compare_is_expensive<Cursor> ? SortingQueueContainer::Array : SortingQueueContainer::Heap>;

/** SortQueueVariants allow to specialize sorting queue for concrete types and sort description.
  * To access queue variant callOnVariant method must be used.
  * To access batch queue variant callOnBatchVariant method must be used.
  */
class SortQueueVariants
{
public:
    SortQueueVariants() = default;

    SortQueueVariants(const DataTypes & sort_description_types, const SortDescription & sort_description);

    SortQueueVariants(const Block & header, const SortDescription & sort_description)
        : SortQueueVariants(extractSortDescriptionTypesFromHeader(header, sort_description), sort_description)
    {
    }

    template <typename Func>
    decltype(auto) callOnVariant(Func && func)
    {
        return std::visit(func, default_queue_variants);
    }

    template <typename Func>
    decltype(auto) callOnBatchVariant(Func && func)
    {
        return std::visit(func, batch_queue_variants);
    }

    bool variantSupportJITCompilation() const
    {
        return std::holds_alternative<SortingQueueForCursor<SimpleSortCursor, SortingQueueStrategy::Default>>(default_queue_variants)
            || std::holds_alternative<SortingQueueForCursor<SortCursor, SortingQueueStrategy::Default>>(default_queue_variants)
            || std::holds_alternative<SortingQueueForCursor<SortCursorWithCollation, SortingQueueStrategy::Default>>(default_queue_variants);
    }

private:
    template <typename Cursor>
    void initializeQueues()
    {
        default_queue_variants = SortingQueueForCursor<Cursor, SortingQueueStrategy::Default>();
        batch_queue_variants = SortingQueueForCursor<Cursor, SortingQueueStrategy::Batch>();
    }

    static DataTypes extractSortDescriptionTypesFromHeader(const Block & header, const SortDescription & sort_description);

    template <SortingQueueStrategy strategy>
    using QueueVariants = std::variant<
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt8>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UInt256>>, strategy>,

        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int8>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Int256>>, strategy>,

        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<BFloat16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Float32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<Float64>>, strategy>,

        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal256>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<DateTime64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnDecimal<Time64>>, strategy>,

        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<UUID>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<IPv4>>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnVector<IPv6>>, strategy>,

        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnString>, strategy>,
        SortingQueueForCursor<SpecializedSingleColumnSortCursor<ColumnFixedString>, strategy>,

        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt8>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UInt256>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int8>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Int256>>, strategy>,

        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<BFloat16>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Float32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<Float64>>, strategy>,

        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<Decimal32>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<Decimal64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<Decimal128>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<Decimal256>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<DateTime64>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnDecimal<Time64>>, strategy>,

        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<UUID>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<IPv4>>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnVector<IPv6>>, strategy>,

        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnString>, strategy>,
        SortingQueueForCursor<SpecializedSingleNullableColumnSortCursor<ColumnFixedString>, strategy>,

        SortingQueueForCursor<SimpleSortCursor, strategy>,
        SortingQueueForCursor<SortCursor, strategy>,
        SortingQueueForCursor<SortCursorWithCollation, strategy>>;

    using DefaultQueueVariants = QueueVariants<SortingQueueStrategy::Default>;
    using BatchQueueVariants = QueueVariants<SortingQueueStrategy::Batch>;

    DefaultQueueVariants default_queue_variants;
    BatchQueueVariants batch_queue_variants;
};

template <typename TLeftColumns, typename TRightColumns>
bool less(const TLeftColumns & lhs, const TRightColumns & rhs, size_t i, size_t j, const SortDescriptionWithPositions & descr)
{
    for (const auto & elem : descr)
    {
        size_t ind = elem.column_number;
        int res = elem.base.direction * lhs[ind]->compareAt(i, j, *rhs[ind], elem.base.nulls_direction);
        if (res < 0)
            return true;
        if (res > 0)
            return false;
    }

    return false;
}

namespace detail
{
/// column(i)` is the i-th key column and `hint(i)` its nan_direction_hint. Stops
/// early once the run is a single row.
template <typename GetColumn, typename GetHint>
size_t equalRangeEndAcrossColumns(size_t count, size_t begin, size_t end, GetColumn && column, GetHint && hint)
{
    size_t run_end = end;
    for (size_t i = 0; i < count; ++i)
    {
        run_end = column(i)->getEqualRangeEndAssumeSorted(begin, run_end, hint(i));
        if (run_end <= begin + 1)
            break; /// single-row run: cannot shrink further
    }
    return run_end;
}
}

/** Multi-column overloads of IColumn::getEqualRangeEndAssumeSorted: find the end (exclusive) of the run
  * of rows that share an equal key across ALL the given key columns, starting at `begin`, within the
  * SORTED range [begin, end). Narrows sequentially - each key column shrinks the candidate end via the
  * per-column method (within column k-1's equal range, column k is itself sorted).
  */
template <typename TColumns>
size_t getEqualRangeEndAssumeSorted(const TColumns & columns, size_t begin, size_t end, int nan_direction_hint)
{
    return detail::equalRangeEndAcrossColumns(
        columns.size(), begin, end, [&](size_t i) -> decltype(auto) { return columns[i]; }, [&](size_t) { return nan_direction_hint; });
}

/** Same as above, but the key columns are selected from `columns` by `positions` (`columns[positions[k]]` is
  * the k-th key column).
  */
template <typename TColumns>
size_t getEqualRangeEndAssumeSorted(
    const TColumns & columns, const std::vector<size_t> & positions, size_t begin, size_t end, int nan_direction_hint)
{
    return detail::equalRangeEndAcrossColumns(
        positions.size(), begin, end, [&](size_t i) -> decltype(auto) { return columns[positions[i]]; }, [&](size_t) { return nan_direction_hint; });
}

/** Same as above, but sort description aware.
  */
template <typename TColumns, typename TSortDescription>
requires requires (const TSortDescription & d) { d.size(); d[0].nulls_direction; }
size_t getEqualRangeEndAssumeSorted(const TColumns & columns, const TSortDescription & descr, size_t begin, size_t end)
{
    return detail::equalRangeEndAcrossColumns(
        descr.size(), begin, end, [&](size_t i) -> decltype(auto) { return columns[i]; }, [&](size_t i) { return descr[i].nulls_direction; });
}
}
