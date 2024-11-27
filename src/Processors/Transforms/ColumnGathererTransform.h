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
    ColumnGathererStream(
        size_t num_inputs,
        ReadBuffer & row_sources_buf_,
        size_t block_preferred_size_rows_,
        size_t block_preferred_size_bytes_,
        bool is_result_sparse_);

    const char * getName() const override { return "ColumnGathererStream"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    /// for use in implementations of IColumn::gather()
    template <typename Column>
    void gather(Column & column_res);

    MergedStats getMergedStats() const override { return {.bytes = merged_bytes, .rows = merged_rows, .blocks = merged_blocks}; }

private:
    void updateStats(const IColumn & column);

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

    const size_t block_preferred_size_rows;
    const size_t block_preferred_size_bytes;
    const bool is_result_sparse;

    Source * source_to_fully_copy = nullptr;

    ssize_t next_required_source = -1;
    UInt64 merged_rows = 0;
    UInt64 merged_bytes = 0;
    UInt64 merged_blocks = 0;
};

class ColumnGathererTransform final : public IMergingTransform<ColumnGathererStream>
{
public:
    ColumnGathererTransform(
        const Block & header,
        size_t num_inputs,
        std::unique_ptr<ReadBuffer> row_sources_buf_,
        size_t block_preferred_size_rows_,
        size_t block_preferred_size_bytes_,
        bool is_result_sparse_);

    String getName() const override { return "ColumnGathererTransform"; }

protected:
    void onFinish() override;

    std::unique_ptr<ReadBuffer> row_sources_buf_holder; /// Keep ownership of row_sources_buf while it's in use by ColumnGathererStream.
    LoggerPtr log;
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
        /// Actually reserve works only for fixed size columns.
        /// So it's safe to ignore preferred size in bytes and call reserve for number of rows.
        size_t size_to_reserve = std::min(static_cast<size_t>(row_sources_end - row_source_pos), block_preferred_size_rows);
        column_res.reserve(size_to_reserve);
    }

    next_required_source = -1;

    /// We use do ... while here to ensure there will be at least one iteration of this loop.
    /// Because the column_res.byteSize() could be bigger than block_preferred_size_bytes already at this point.
    do
    {
        if (row_source_pos >= row_sources_end)
            break;

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
            if (len == 1)
                column_res.insertFrom(*source.column, source.pos);
            else
                column_res.insertRangeFrom(*source.column, source.pos, len);
        }

        source.pos += len;
    } while (column_res.size() < block_preferred_size_rows && column_res.byteSize() < block_preferred_size_bytes);
}

}
