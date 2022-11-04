#pragma once

#include <cassert>
#include <vector>
#include <algorithm>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/callOnTypeIndex.h>
#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

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
        int res = 0;

#if USE_EMBEDDED_COMPILER
        if (impl->desc.compiled_sort_description && rhs.impl->desc.compiled_sort_description)
        {
            assert(impl->raw_sort_columns_data.size() == rhs.impl->raw_sort_columns_data.size());

            auto sort_description_func_typed = reinterpret_cast<JITSortDescriptionFunc>(impl->desc.compiled_sort_description);
            res = sort_description_func_typed(lhs_pos, rhs_pos, impl->raw_sort_columns_data.data(), rhs.impl->raw_sort_columns_data.data()); /// NOLINT
        }
        else
#endif
        {
            const auto & desc = impl->desc[0];
            int direction = desc.direction;
            int nulls_direction = desc.nulls_direction;
            res = direction * impl->sort_columns[0]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[0]), nulls_direction);
        }

        if (res > 0)
            return true;
        if (res < 0)
            return false;

        return impl->order > rhs.impl->order;
    }
};

template <typename ColumnType>
struct SpecializedSingleColumnSortCursor : SortCursorHelper<SpecializedSingleColumnSortCursor<ColumnType>>
{
    using SortCursorHelper<SpecializedSingleColumnSortCursor>::SortCursorHelper;

    bool ALWAYS_INLINE greaterAt(const SortCursorHelper<SpecializedSingleColumnSortCursor> & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        auto & this_impl = this->impl;

        auto & lhs_columns = this_impl->sort_columns;
        auto & rhs_columns = rhs.impl->sort_columns;

        assert(lhs_columns.size() == 1);
        assert(rhs_columns.size() == 1);

        const auto & lhs_column = assert_cast<const ColumnType &>(*lhs_columns[0]);
        const auto & rhs_column = assert_cast<const ColumnType &>(*rhs_columns[0]);

        const auto & desc = this->impl->desc[0];

        int res = desc.direction * lhs_column.compareAt(lhs_pos, rhs_pos, rhs_column, desc.nulls_direction);

        if (res > 0)
            return true;
        if (res < 0)
            return false;

        return this_impl->order > rhs.impl->order;
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

enum class SortingQueueStrategy
{
    Default,
    Batch
};

/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data streams).
template <typename Cursor, SortingQueueStrategy strategy>
class SortingQueueImpl
{
public:
    SortingQueueImpl() = default;

    template <typename Cursors>
    explicit SortingQueueImpl(Cursors & cursors)
    {
        size_t size = cursors.size();
        queue.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            if (cursors[i].empty())
                continue;

            queue.emplace_back(&cursors[i]);
        }

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
        assert(isValid());

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
        assert(isValid());
        assert(batch_size_value <= batch_size);
        assert(batch_size_value > 0);

        batch_size -= batch_size_value;
        if (batch_size > 0)
        {
            queue.front()->next(batch_size_value);
            return;
        }

        if (!queue.front()->isLast(batch_size_value))
        {
            queue.front()->next(batch_size_value);
            updateTop(false /*check_in_order*/);
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
        std::pop_heap(queue.begin(), queue.end());
        queue.pop_back();
        next_child_idx = 0;

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
        queue.emplace_back(&cursor);
        std::push_heap(queue.begin(), queue.end());
        next_child_idx = 0;

        if constexpr (strategy == SortingQueueStrategy::Batch)
            updateBatchSize();
    }

private:
    using Container = std::vector<Cursor>;
    Container queue;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_child_idx = 0;
    size_t batch_size = 0;

    size_t ALWAYS_INLINE nextChildIndex()
    {
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

    /// Update batch size of elements that client can extract from current cursor
    void updateBatchSize()
    {
        assert(!queue.empty());

        auto & begin_cursor = *queue.begin();
        size_t min_cursor_size = begin_cursor->getSize();
        size_t min_cursor_pos = begin_cursor->getPosRef();

        if (queue.size() == 1)
        {
            batch_size = min_cursor_size - min_cursor_pos;
            return;
        }

        batch_size = 1;
        size_t child_idx = nextChildIndex();
        auto & next_child_cursor = *(queue.begin() + child_idx);

        if (min_cursor_pos + batch_size < min_cursor_size && next_child_cursor.greaterWithOffset(begin_cursor, 0, batch_size))
            ++batch_size;
        else
            return;

        if (unlikely(begin_cursor.totallyLessOrEquals(next_child_cursor)))
        {
            batch_size = min_cursor_size - min_cursor_pos;
            return;
        }

        while (min_cursor_pos + batch_size < min_cursor_size && next_child_cursor.greaterWithOffset(begin_cursor, 0, batch_size))
            ++batch_size;
    }
};

template <typename Cursor>
using SortingQueue = SortingQueueImpl<Cursor, SortingQueueStrategy::Default>;

template <typename Cursor>
using SortingQueueBatch = SortingQueueImpl<Cursor, SortingQueueStrategy::Batch>;

/** SortQueueVariants allow to specialize sorting queue for concrete types and sort description.
  * To access queue variant callOnVariant method must be used.
  * To access batch queue variant callOnBatchVariant method must be used.
  */
class SortQueueVariants
{
public:
    SortQueueVariants() = default;

    SortQueueVariants(const DataTypes & sort_description_types, const SortDescription & sort_description)
    {
        bool has_collation = false;
        for (const auto & column_description : sort_description)
        {
            if (column_description.collator)
            {
                has_collation = true;
                break;
            }
        }

        if (has_collation)
        {
            initializeQueues<SortCursorWithCollation>();
            return;
        }
        else if (sort_description.size() == 1)
        {
            TypeIndex column_type_index = sort_description_types[0]->getTypeId();

            bool result = callOnIndexAndDataType<void>(
                column_type_index,
                [&](const auto & types)
                {
                    using Types = std::decay_t<decltype(types)>;
                    using ColumnDataType = typename Types::LeftType;
                    using ColumnType = typename ColumnDataType::ColumnType;

                    initializeQueues<SpecializedSingleColumnSortCursor<ColumnType>>();
                    return true;
                });

            if (!result)
                initializeQueues<SimpleSortCursor>();
        }
        else
        {
            initializeQueues<SortCursor>();
        }
    }

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
        return std::holds_alternative<SortingQueue<SimpleSortCursor>>(default_queue_variants)
            || std::holds_alternative<SortingQueue<SortCursor>>(default_queue_variants)
            || std::holds_alternative<SortingQueue<SortCursorWithCollation>>(default_queue_variants);
    }

private:
    template <typename Cursor>
    void initializeQueues()
    {
        default_queue_variants = SortingQueue<Cursor>();
        batch_queue_variants = SortingQueueBatch<Cursor>();
    }

    static DataTypes extractSortDescriptionTypesFromHeader(const Block & header, const SortDescription & sort_description)
    {
        size_t sort_description_size = sort_description.size();
        DataTypes data_types(sort_description_size);

        for (size_t i = 0; i < sort_description_size; ++i)
        {
            const auto & column_sort_description = sort_description[i];
            data_types[i] = header.getByName(column_sort_description.column_name).type;
        }

        return data_types;
    }

    template <SortingQueueStrategy strategy>
    using QueueVariants = std::variant<
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt8>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt16>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt32>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt64>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt128>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UInt256>>, strategy>,

        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int8>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int16>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int32>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int64>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int128>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Int256>>, strategy>,

        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Float32>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<Float64>>, strategy>,

        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal32>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal64>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal128>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnDecimal<Decimal256>>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnDecimal<DateTime64>>, strategy>,

        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnVector<UUID>>, strategy>,

        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnString>, strategy>,
        SortingQueueImpl<SpecializedSingleColumnSortCursor<ColumnFixedString>, strategy>,

        SortingQueueImpl<SimpleSortCursor, strategy>,
        SortingQueueImpl<SortCursor, strategy>,
        SortingQueueImpl<SortCursorWithCollation, strategy>>;

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
        else if (res > 0)
            return false;
    }

    return false;
}

}
