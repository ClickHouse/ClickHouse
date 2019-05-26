#pragma once

#include <Common/typeid_cast.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
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

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block)
    {
        all_columns.clear();
        sort_columns.clear();

        size_t num_columns = block.columns();

        for (size_t j = 0; j < num_columns; ++j)
            all_columns.push_back(block.safeGetByPosition(j).column.get());

        for (size_t j = 0, size = desc.size(); j < size; ++j)
        {
            size_t column_number = !desc[j].column_name.empty()
                ? block.getPositionByName(desc[j].column_name)
                : desc[j].column_number;

            sort_columns.push_back(block.safeGetByPosition(column_number).column.get());

            need_collation[j] = desc[j].collator != nullptr && typeid_cast<const ColumnString *>(sort_columns.back());    /// TODO Nullable(String)
            has_collation |= need_collation[j];
        }

        pos = 0;
        rows = all_columns[0]->size();
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= rows; }
    void next() { ++pos; }
};


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
                const ColumnString & column_string = static_cast<const ColumnString &>(*impl->sort_columns[i]);
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

}
