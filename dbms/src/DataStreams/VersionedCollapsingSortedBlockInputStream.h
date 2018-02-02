#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>

#include <deque>

namespace DB
{

static const size_t MAX_ROWS_IN_MULTIVERSION_QUEUE = 8192;

/* Deque with fixed memory size. Allows pushing gaps.
 * frontGap() returns the number of gaps were inserted before front.
 *
 * Note: empty deque may have non-zero front gap.
 */
template <typename T>
class FixedSizeDequeWithGaps
{
public:

    struct ValueWithGap
    {
        size_t gap;
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
        constructValue(end, value);
        moveRight(end);
        container[end].gap = 0;
    }

    void pushGap(size_t count) { container[end].gap += count; }

    void popBack()
    {
        size_t curr_gap = container[end].gap;
        moveLeft(end);
        destructValue(end);
        container[end].gap += curr_gap;
    }

    void popFront()
    {
        destructValue(begin);
        moveRight(begin);
    }

    T & front() { return getValue(begin); }
    const T & front() const { return getValue(begin); }

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
};

class VersionedCollapsingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    /// Don't need version column. It's in primary key.
    /// max_rows_in_queue should be about max_block_size_ if we won't store a lot of extra blocks (RowRef holds SharedBlockPtr).
    VersionedCollapsingSortedBlockInputStream(
            BlockInputStreams inputs_, const SortDescription & description_,
            const String & sign_column_, size_t max_block_size_, bool can_collapse_all_rows_,
            WriteBuffer * out_row_sources_buf_ = nullptr)
            : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_)
            , sign_column(sign_column_)
            , max_rows_in_queue(std::min(std::max<size_t>(3, max_block_size_), MAX_ROWS_IN_MULTIVERSION_QUEUE) - 2)
            , current_keys(max_rows_in_queue + 1), can_collapse_all_rows(can_collapse_all_rows_)
    {
    }

    String getName() const override { return "VersionedCollapsingSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "VersionedCollapsingSortedBlockInputStream(inputs";

        for (const auto & child : children)
            res << ", " << child->getID();

        res << ", description";

        for (const auto & descr : description)
            res << ", " << descr.getID();

        res << ", sign_column, " << sign_column;
        res << ", version_column, " << sign_column << ")";
        return res.str();
    }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String sign_column;

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

    const bool can_collapse_all_rows;

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output to result row for the current primary key.
    void insertRow(size_t skip_rows, const RowRef & row, MutableColumns & merged_columns);

    void insertGap(size_t gap_size);
};

}
