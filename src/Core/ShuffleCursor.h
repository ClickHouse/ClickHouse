#pragma once

#include <cassert>
#include <random>
#include <vector>

#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Common/PODArray_fwd.h>

#include <Core/ColumnNumbers.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

class Block;
using IColumnPermutation = PaddedPODArray<size_t>;

/** Cursor moves inside single block.
  */
struct ShuffleCursor
{
    ColumnRawPtrs all_columns;
    size_t rows = 0;
    /** Cursor number (always?) equals to number of merging part.
      * Therefore this field can be used to determine part number of current row (see ColumnGathererStream).
      */
    size_t order = 0;


    /** We could use ShuffleCursor in case when columns aren't shuffled
      *  but we have their shuffled permutation
      */
    IColumnPermutation * permutation = nullptr;


    ShuffleCursor() = default;

    ShuffleCursor(const Block & block, size_t order_ = 0, IColumnPermutation * perm = nullptr)
    : order(order_)
    { reset(block, perm); }

    ShuffleCursor(const Block & header, const Columns & columns, size_t num_rows, size_t order_ = 0, IColumnPermutation * perm = nullptr)
    : order(order_)
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
        // if (permutation)
        //     return (*permutation)[pos];
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

using ShuffleCursors = std::vector<ShuffleCursor>;
using ShuffleCursorRawPtr = ShuffleCursor *;
using ShuffleCursorRawPtrs = std::vector<ShuffleCursorRawPtr>;


/// Allows to fetch data from multiple shuffle cursors in random order (merging shuffled data streams).
class ShufflingQueue
{
public:
    ShufflingQueue() = default;

    explicit ShufflingQueue(ShuffleCursors & cursors)
    {
        size_t size = cursors.size();
        queue.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            if (cursors[i].empty())
                continue;

            queue.push_back(&cursors[i]);
        }
    }

    bool isValid() const { return !queue.empty(); }

    /// Returns cursor with random index and its index (in order to remove it if needed)
    std::pair<ShuffleCursorRawPtr, size_t> current()
    {
        auto index = generateRandomIndex();
        return {queue[index], index};
    }

    size_t size() { return queue.size(); }

    void incrementCursorPosByIndex(size_t index) {
        queue[index]->next();
    }

    void removeFromIndex(size_t index) {
        std::swap(queue[index], queue.back());
        queue.pop_back();
     }

    void push(ShuffleCursorRawPtr cursor) { 
        queue.push_back(cursor);
    }

private:
    ShuffleCursorRawPtrs queue;
    std::mt19937 rng;

    size_t generateRandomIndex()
    {
        if (queue.empty())
        {
            throw std::out_of_range("Cannot generate index for empty queue");
        }

        std::uniform_int_distribution<size_t> dist(0, queue.size() - 1);
        return dist(rng);
    }
};

}
