#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>

#include <deque>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static const size_t MAX_ROWS_IN_MULTIVERSION_QUEUE = 8192;


/* Deque with fixed memory size. Allows pushing gaps.
 * frontGap() returns the number of gaps were inserted before front.
 *
 * This structure may be implemented via std::deque, but
 *  - Deque uses fixed amount of memory which is allocated in constructor. No more allocations are performed.
 *  - Gaps are not stored as separate values in queue, which is more memory efficient.
 *  - Deque is responsible for gaps invariant: after removing element, moves gaps into neighbor cell.
 *
 * Note: empty deque may have non-zero front gap.
 */
template <typename T>
class FixedSizeDequeWithGaps
{
public:

    struct ValueWithGap
    {
        /// The number of gaps before current element. The number of gaps after last element stores into end cell.
        size_t gap;
        /// Store char[] instead of T in order to make ValueWithGap POD.
        /// Call placement constructors after push and and destructors after pop.
        char value[sizeof(T)];
    };

    explicit FixedSizeDequeWithGaps(size_t size)
    {
        container.resize_fill(size + 1);
    }

    ~FixedSizeDequeWithGaps()
    {
        auto destruct_range = [this](size_t from, size_t to)
        {
            for (size_t i = from; i < to; ++i)
                destructValue(i);
        };

        if (begin <= end)
            destruct_range(begin, end);
        else
        {
            destruct_range(0, end);
            destruct_range(begin, container.size());
        }
    }

    void pushBack(const T & value)
    {
        checkEnoughSpaceToInsert();
        constructValue(end, value);
        moveRight(end);
        container[end].gap = 0;
    }

    void pushGap(size_t count) { container[end].gap += count; }

    void popBack()
    {
        checkHasValuesToRemove();
        size_t curr_gap = container[end].gap;
        moveLeft(end);
        destructValue(end);
        container[end].gap += curr_gap;
    }

    void popFront()
    {
        checkHasValuesToRemove();
        destructValue(begin);
        moveRight(begin);
    }

    T & front()
    {
        checkHasValuesToGet();
        return getValue(begin);
    }
    const T & front() const
    {
        checkHasValuesToGet();
        return getValue(begin);
    }

    const T & back() const
    {
        size_t ps = end;
        moveLeft(ps);
        return getValue(ps);
    }

    size_t & frontGap() { return container[begin].gap; }
    const size_t & frontGap() const { return container[begin].gap; }

    size_t size() const
    {
        if (begin <= end)
            return end - begin;
        return end + (container.size() - begin);
    }

    bool empty() const { return begin == end; }

private:
    PODArray<ValueWithGap> container;

    size_t gap_before_first = 0;
    size_t begin = 0;
    size_t end = 0;

    void constructValue(size_t index, const T & value) { new (container[index].value) T(value); }
    void destructValue(size_t index) { reinterpret_cast<T *>(container[index].value)->~T(); }

    T & getValue(size_t index) { return *reinterpret_cast<T *>(container[index].value); }
    const T & getValue(size_t index) const { return *reinterpret_cast<const T *>(container[index].value); }

    void moveRight(size_t & index) const
    {
        ++index;

        if (index == container.size())
            index = 0;
    }

    void moveLeft(size_t & index) const
    {
        if (index == 0)
            index = container.size();

        --index;
    }

    void checkEnoughSpaceToInsert() const
    {
        if (size() + 1 == container.size())
            throw Exception("Not enough space to insert into FixedSizeDequeWithGaps with capacity "
                            + toString(container.size() - 1), ErrorCodes::LOGICAL_ERROR);
    }

    void checkHasValuesToRemove() const
    {
        if (empty())
            throw Exception("Cannot remove from empty FixedSizeDequeWithGaps", ErrorCodes::LOGICAL_ERROR);
    }

    void checkHasValuesToGet() const
    {
        if (empty())
            throw Exception("Cannot get value from empty FixedSizeDequeWithGaps", ErrorCodes::LOGICAL_ERROR);
    }
};

class VersionedCollapsingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    /// Don't need version column. It's in primary key.
    /// max_rows_in_queue should be about max_block_size_ if we won't store a lot of extra blocks (RowRef holds SharedBlockPtr).
    VersionedCollapsingSortedBlockInputStream(
        const BlockInputStreams & inputs_, const SortDescription & description_,
        const String & sign_column_, size_t max_block_size_,
        WriteBuffer * out_row_sources_buf_ = nullptr);

    String getName() const override { return "VersionedCollapsingSorted"; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    size_t sign_column_number = 0;

    Logger * log = &Logger::get("VersionedCollapsingSortedBlockInputStream");

    /// Read is finished.
    bool finished = false;

    Int8 sign_in_queue = 0;
    const size_t max_rows_in_queue;
    /// Rows with the same primary key and sign.
    FixedSizeDequeWithGaps<RowRef> current_keys;

    size_t blocks_written = 0;

    /// Sources of rows for VERTICAL merge algorithm. Size equals to (size + number of gaps) in current_keys.
    std::queue<RowSourcePart> current_row_sources;

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output to result row for the current primary key.
    void insertRow(size_t skip_rows, const RowRef & row, MutableColumns & merged_columns);

    void insertGap(size_t gap_size);
};

}
