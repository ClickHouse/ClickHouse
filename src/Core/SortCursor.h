#pragma once

#include <cassert>
#include <vector>
#include <algorithm>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>

#include "config_core.h"

#if USE_EMBEDDED_COMPILER
#include <Interpreters/JIT/compileFunction.h>
#endif

namespace DB
{

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

    using NeedCollationFlags = std::vector<UInt8>;

    /** Should we use Collator to sort a column? */
    NeedCollationFlags need_collation;

    /** Is there at least one column with Collator. */
    bool has_collation = false;

    /** We could use SortCursorImpl in case when columns aren't sorted
      *  but we have their sorted permutation
      */
    IColumn::Permutation * permutation = nullptr;

#if USE_EMBEDDED_COMPILER
    std::vector<ColumnData> raw_sort_columns_data;
#endif

    SortCursorImpl() = default;

    SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0, IColumn::Permutation * perm = nullptr)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        reset(block, perm);
    }

    SortCursorImpl(
        const Block & header,
        const Columns & columns,
        const SortDescription & desc_,
        size_t order_ = 0,
        IColumn::Permutation * perm = nullptr)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        reset(columns, header, perm);
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block, IColumn::Permutation * perm = nullptr) { reset(block.getColumns(), block, perm); }

    /// Set the cursor to the beginning of the new block.
    void reset(const Columns & columns, const Block & block, IColumn::Permutation * perm = nullptr)
    {
        all_columns.clear();
        sort_columns.clear();
#if USE_EMBEDDED_COMPILER
        raw_sort_columns_data.clear();
#endif

        size_t num_columns = columns.size();

        for (size_t j = 0; j < num_columns; ++j)
            all_columns.push_back(columns[j].get());

        for (size_t j = 0, size = desc.size(); j < size; ++j)
        {
            auto & column_desc = desc[j];
            size_t column_number = block.getPositionByName(column_desc.column_name);
            sort_columns.push_back(columns[column_number].get());

#if USE_EMBEDDED_COMPILER
            if (desc.compiled_sort_description)
                raw_sort_columns_data.emplace_back(getColumnData(sort_columns.back()));
#endif
            need_collation[j] = desc[j].collator != nullptr && sort_columns.back()->isCollationSupported();
            has_collation |= need_collation[j];
        }

        pos = 0;
        rows = all_columns[0]->size();
        permutation = perm;
    }

    size_t getRow() const
    {
        if (permutation)
            return (*permutation)[pos];
        return pos;
    }

    /// We need a possibility to change pos (see MergeJoin).
    size_t & getPosRef() { return pos; }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    bool isValid() const { return pos < rows; }
    void next() { ++pos; }

/// Prevent using pos instead of getRow()
private:
    size_t pos = 0;
};

using SortCursorImpls = std::vector<SortCursorImpl>;


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
};


struct SortCursor : SortCursorHelper<SortCursor>
{
    using SortCursorHelper<SortCursor>::SortCursorHelper;

    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool ALWAYS_INLINE greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
#if USE_EMBEDDED_COMPILER
        if (impl->desc.compiled_sort_description && rhs.impl->desc.compiled_sort_description)
        {
            assert(impl->raw_sort_columns_data.size() == rhs.impl->raw_sort_columns_data.size());

            auto sort_description_func_typed = reinterpret_cast<JITSortDescriptionFunc>(impl->desc.compiled_sort_description);
            int res = sort_description_func_typed(lhs_pos, rhs_pos, impl->raw_sort_columns_data.data(), rhs.impl->raw_sort_columns_data.data()); /// NOLINT

            if (res > 0)
                return true;
            if (res < 0)
                return false;

            return impl->order > rhs.impl->order;
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

        return impl->order > rhs.impl->order;
    }
};


/// For the case with a single column and when there is no order between different cursors.
struct SimpleSortCursor : SortCursorHelper<SimpleSortCursor>
{
    using SortCursorHelper<SimpleSortCursor>::SortCursorHelper;

    bool ALWAYS_INLINE greaterAt(const SimpleSortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        const auto & desc = impl->desc[0];
        int direction = desc.direction;
        int nulls_direction = desc.nulls_direction;

        bool result = false;

#if USE_EMBEDDED_COMPILER
        if (impl->desc.compiled_sort_description && rhs.impl->desc.compiled_sort_description)
        {
            assert(impl->raw_sort_columns_data.size() == rhs.impl->raw_sort_columns_data.size());

            auto sort_description_func_typed = reinterpret_cast<JITSortDescriptionFunc>(impl->desc.compiled_sort_description);
            int jit_result = sort_description_func_typed(lhs_pos, rhs_pos, impl->raw_sort_columns_data.data(), rhs.impl->raw_sort_columns_data.data()); /// NOLINT
            result = jit_result > 0;
        }
        else
#endif
        {
            int non_jit_result = impl->sort_columns[0]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[0]), nulls_direction);
            result = (non_jit_result != 0 && ((non_jit_result > 0) == (direction > 0)));
        }

        return result;
    }
};


/// Separate comparator for locale-sensitive string comparisons
struct SortCursorWithCollation : SortCursorHelper<SortCursorWithCollation>
{
    using SortCursorHelper<SortCursorWithCollation>::SortCursorHelper;

    bool ALWAYS_INLINE greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            const auto & desc = impl->desc[i];
            int direction = desc.direction;
            int nulls_direction = desc.nulls_direction;
            int res;
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
        return impl->order > rhs.impl->order;
    }
};


/** Allows to fetch data from multiple sort cursors in sorted order (merging sorted data streams).
  * TODO: Replace with "Loser Tree", see https://en.wikipedia.org/wiki/K-way_merge_algorithm
  */
template <typename Cursor>
class SortingHeap
{
public:
    SortingHeap() = default;

    template <typename Cursors>
    explicit SortingHeap(Cursors & cursors)
    {
        size_t size = cursors.size();
        queue.reserve(size);
        for (size_t i = 0; i < size; ++i)
            if (!cursors[i].empty())
                queue.emplace_back(&cursors[i]);
        std::make_heap(queue.begin(), queue.end());
    }

    bool isValid() const { return !queue.empty(); }

    Cursor & current() { return queue.front(); }

    size_t size() { return queue.size(); }

    Cursor & nextChild() { return queue[nextChildIndex()]; }

    void ALWAYS_INLINE next()
    {
        assert(isValid());

        if (!current()->isLast())
        {
            current()->next();
            updateTop();
        }
        else
            removeTop();
    }

    void replaceTop(Cursor new_top)
    {
        current() = new_top;
        updateTop();
    }

    void removeTop()
    {
        std::pop_heap(queue.begin(), queue.end());
        queue.pop_back();
        next_idx = 0;
    }

    void push(SortCursorImpl & cursor)
    {
        queue.emplace_back(&cursor);
        std::push_heap(queue.begin(), queue.end());
        next_idx = 0;
    }

private:
    using Container = std::vector<Cursor>;
    Container queue;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_idx = 0;

    size_t ALWAYS_INLINE nextChildIndex()
    {
        if (next_idx == 0)
        {
            next_idx = 1;

            if (queue.size() > 2 && queue[1] < queue[2])
                ++next_idx;
        }

        return next_idx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void ALWAYS_INLINE updateTop()
    {
        size_t size = queue.size();
        if (size < 2)
            return;

        auto begin = queue.begin();

        size_t child_idx = nextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        next_idx = 0;

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

            if ((child_idx + 1) < size && *child_it < *(child_it + 1))
            {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
    }
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
        else if (res > 0)
            return false;
    }

    return false;
}

}
