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


namespace DB
{

/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct SortCursorImpl
{
    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    size_t pos = 0;
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

    SortCursorImpl() {}

    SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        reset(block);
    }

    SortCursorImpl(const Columns & columns, const SortDescription & desc_, size_t order_ = 0)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
    {
        for (auto & column_desc : desc)
        {
            if (!column_desc.column_name.empty())
                throw Exception("SortDesctiption should contain column position if SortCursor was used without header.",
                        ErrorCodes::LOGICAL_ERROR);
        }
        reset(columns, {});
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block)
    {
        reset(block.getColumns(), block);
    }

    /// Set the cursor to the beginning of the new block.
    void reset(const Columns & columns, const Block & block)
    {
        all_columns.clear();
        sort_columns.clear();

        size_t num_columns = columns.size();

        for (size_t j = 0; j < num_columns; ++j)
            all_columns.push_back(columns[j].get());

        for (size_t j = 0, size = desc.size(); j < size; ++j)
        {
            auto & column_desc = desc[j];
            size_t column_number = !column_desc.column_name.empty()
                                   ? block.getPositionByName(column_desc.column_name)
                                   : column_desc.column_number;
            sort_columns.push_back(columns[column_number].get());

            need_collation[j] = desc[j].collator != nullptr && typeid_cast<const ColumnString *>(sort_columns.back());    /// TODO Nullable(String)
            has_collation |= need_collation[j];
        }

        pos = 0;
        rows = all_columns[0]->size();
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    bool isValid() const { return pos < rows; }
    void next() { ++pos; }
};

using SortCursorImpls = std::vector<SortCursorImpl>;


/// For easy copying.
struct SortCursor
{
    SortCursorImpl * impl;

    SortCursor(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator-> () { return impl; }
    const SortCursorImpl * operator-> () const { return impl; }

    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        return impl->order > rhs.impl->order;
    }

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool totallyLessOrEquals(const SortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const SortCursor & rhs) const
    {
        return greaterAt(rhs, impl->pos, rhs.impl->pos);
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator< (const SortCursor & rhs) const
    {
        return greater(rhs);
    }
};


/// Separate comparator for locale-sensitive string comparisons
struct SortCursorWithCollation
{
    SortCursorImpl * impl;

    SortCursorWithCollation(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator-> () { return impl; }
    const SortCursorImpl * operator-> () const { return impl; }

    bool greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res;
            if (impl->need_collation[i])
            {
                const ColumnString & column_string = assert_cast<const ColumnString &>(*impl->sort_columns[i]);
                res = column_string.compareAtWithCollation(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), *impl->desc[i].collator);
            }
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

    bool totallyLessOrEquals(const SortCursorWithCollation & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const SortCursorWithCollation & rhs) const
    {
        return greaterAt(rhs, impl->pos, rhs.impl->pos);
    }

    bool operator< (const SortCursorWithCollation & rhs) const
    {
        return greater(rhs);
    }
};


/** Allows to fetch data from multiple sort cursors in sorted order (merging sorted data streams).
  */
template <typename Cursor>
class SortingHeap
{
public:
    SortingHeap() = default;

    template <typename Cursors>
    SortingHeap(Cursors & cursors)
    {
        size_t size = cursors.size();
        queue.reserve(size);
        for (size_t i = 0; i < size; ++i)
            queue.emplace_back(&cursors[i]);
        std::make_heap(queue.begin(), queue.end());
    }

    bool isValid() const { return !queue.empty(); }

    Cursor & current() { return queue.front(); }

    void next()
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

private:
    using Container = std::vector<Cursor>;
    Container queue;

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    void updateTop()
    {
        size_t size = queue.size();
        if (size < 2)
            return;

        size_t child_idx = 1;
        auto begin = queue.begin();
        auto child_it = begin + 1;

        /// Right child exists and is greater than left child.
        if (size > 2 && *child_it < *(child_it + 1))
        {
            ++child_it;
            ++child_idx;
        }

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            if ((size - 2) / 2 < child_idx)
                break;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;
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

    void removeTop()
    {
        std::pop_heap(queue.begin(), queue.end());
        queue.pop_back();
    }
};

}
