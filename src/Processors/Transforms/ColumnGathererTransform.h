#pragma once

#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>


namespace Poco { class Logger; }


namespace DB
{


/// Tiny struct, stores number of a Part from which current row was fetched, and insertion flag.
struct RowSourcePart
{
    UInt8 data = 0;

    RowSourcePart() = default;

    explicit RowSourcePart(size_t source_num, bool skip_flag = false)
    {
        static_assert(sizeof(*this) == 1, "Size of RowSourcePart is too big due to compiler settings");
        setSourceNum(source_num);
        setSkipFlag(skip_flag);
    }

    size_t getSourceNum() const { return data & MASK_NUMBER; }

    /// In CollapsingMergeTree case flag means "skip this rows"
    bool getSkipFlag() const { return (data & MASK_FLAG) != 0; }

    void setSourceNum(size_t source_num)
    {
        data = (data & MASK_FLAG) | (static_cast<UInt8>(source_num) & MASK_NUMBER);
    }

    void setSkipFlag(bool flag)
    {
        data = flag ? data | MASK_FLAG : data & ~MASK_FLAG;
    }

    static constexpr size_t MAX_PARTS = 0x7F;
    static constexpr UInt8 MASK_NUMBER = 0x7F;
    static constexpr UInt8 MASK_FLAG = 0x80;
};

using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should contain exactly one column.
  */
class ColumnGathererStream final : public IMergingAlgorithm
{
public:
    ColumnGathererStream(size_t num_inputs, ReadBuffer & row_sources_buf_, size_t block_preferred_size_ = DEFAULT_BLOCK_SIZE);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    /// for use in implementations of IColumn::gather()
    template <typename Column>
    void gather(Column & column_res);

    UInt64 getMergedRows() const { return merged_rows; }
    UInt64 getMergedBytes() const { return merged_bytes; }

private:
    /// Cache required fields
    struct Source
    {
        ColumnPtr column;
        size_t pos = 0;
        size_t size = 0;

        void update(ColumnPtr column_)
        {
            column = std::move(column_);
            size = column->size();
            pos = 0;
        }
    };

    MutableColumnPtr result_column;

    std::vector<Source> sources;
    ReadBuffer & row_sources_buf;

    const size_t block_preferred_size;

    Source * source_to_fully_copy = nullptr;

    ssize_t next_required_source = -1;
    size_t cur_block_preferred_size = 0;

    UInt64 merged_rows = 0;
    UInt64 merged_bytes = 0;
};

class ColumnGathererTransform final : public IMergingTransform<ColumnGathererStream>
{
public:
    ColumnGathererTransform(
        const Block & header,
        size_t num_inputs,
        ReadBuffer & row_sources_buf_,
        size_t block_preferred_size_ = DEFAULT_BLOCK_SIZE);

    String getName() const override { return "ColumnGathererTransform"; }

    void work() override;

protected:
    void onFinish() override;
    UInt64 elapsed_ns = 0;

    Poco::Logger * log;
};


template <typename Column>
void ColumnGathererStream::gather(Column & column_res)
{
    row_sources_buf.nextIfAtEnd();
    RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(row_sources_buf.position());
    RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(row_sources_buf.buffer().end());

    if (next_required_source == -1)
    {
        /// Start new column.
        cur_block_preferred_size = std::min(static_cast<size_t>(row_sources_end - row_source_pos), block_preferred_size);
        column_res.reserve(cur_block_preferred_size);
    }

    size_t cur_size = column_res.size();
    next_required_source = -1;

    while (row_source_pos < row_sources_end && cur_size < cur_block_preferred_size)
    {
        RowSourcePart row_source = *row_source_pos;
        size_t source_num = row_source.getSourceNum();
        Source & source = sources[source_num];
        bool source_skip = row_source.getSkipFlag();

        if (source.pos >= source.size) /// Fetch new block from source_num part
        {
            next_required_source = source_num;
            return;
        }

        ++row_source_pos;

        /// Consecutive optimization. TODO: precompute lengths
        size_t len = 1;
        size_t max_len = std::min(static_cast<size_t>(row_sources_end - row_source_pos), source.size - source.pos); // interval should be in the same block
        while (len < max_len && row_source_pos->data == row_source.data)
        {
            ++len;
            ++row_source_pos;
        }

        row_sources_buf.position() = reinterpret_cast<char *>(row_source_pos);

        if (!source_skip)
        {
            /// Whole block could be produced via copying pointer from current block
            if (source.pos == 0 && source.size == len)
            {
                /// If current block already contains data, return it.
                /// Whole column from current source will be returned on next read() iteration.
                source_to_fully_copy = &source;
                return;
            }
            else if (len == 1)
                column_res.insertFrom(*source.column, source.pos);
            else
                column_res.insertRangeFrom(*source.column, source.pos, len);

            cur_size += len;
        }

        source.pos += len;
    }
}

}
