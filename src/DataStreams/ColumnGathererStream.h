#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>


namespace Poco { class Logger; }


namespace DB
{


/// Tiny struct, stores number of a Part from which current row was fetched, and insertion flag.
struct RowSourcePart
{
    UInt8 data = 0;

    RowSourcePart() = default;

    RowSourcePart(size_t source_num, bool skip_flag = false)
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
class ColumnGathererStream : public IBlockInputStream
{
public:
    ColumnGathererStream(
        const String & column_name_, const BlockInputStreams & source_streams, ReadBuffer & row_sources_buf_,
        size_t block_preferred_size_ = DEFAULT_BLOCK_SIZE);

    String getName() const override { return "ColumnGatherer"; }

    Block readImpl() override;

    void readSuffixImpl() override;

    Block getHeader() const override { return children.at(0)->getHeader(); }

    /// for use in implementations of IColumn::gather()
    template <typename Column>
    void gather(Column & column_res);

private:
    /// Cache required fields
    struct Source
    {
        const IColumn * column = nullptr;
        size_t pos = 0;
        size_t size = 0;
        Block block;

        void update(const String & name)
        {
            column = block.getByName(name).column.get();
            size = block.rows();
            pos = 0;
        }
    };

    void fetchNewBlock(Source & source, size_t source_num);

    String column_name;
    ColumnWithTypeAndName column;

    std::vector<Source> sources;
    ReadBuffer & row_sources_buf;

    size_t block_preferred_size;

    Source * source_to_fully_copy = nullptr;
    Block output_block;

    Poco::Logger * log;
};

template <typename Column>
void ColumnGathererStream::gather(Column & column_res)
{
    if (source_to_fully_copy) /// Was set on a previous iteration
    {
        output_block.getByPosition(0).column = source_to_fully_copy->block.getByName(column_name).column;
        source_to_fully_copy->pos = source_to_fully_copy->size;
        source_to_fully_copy = nullptr;
        return;
    }

    row_sources_buf.nextIfAtEnd();
    RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(row_sources_buf.position());
    RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(row_sources_buf.buffer().end());

    size_t cur_block_preferred_size = std::min(static_cast<size_t>(row_sources_end - row_source_pos), block_preferred_size);
    column_res.reserve(cur_block_preferred_size);

    size_t cur_size = 0;

    while (row_source_pos < row_sources_end && cur_size < cur_block_preferred_size)
    {
        RowSourcePart row_source = *row_source_pos;
        size_t source_num = row_source.getSourceNum();
        Source & source = sources[source_num];
        bool source_skip = row_source.getSkipFlag();
        ++row_source_pos;

        if (source.pos >= source.size) /// Fetch new block from source_num part
        {
            fetchNewBlock(source, source_num);
        }

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
                if (cur_size > 0)
                {
                    source_to_fully_copy = &source;
                    return;
                }

                output_block.getByPosition(0).column = source.block.getByName(column_name).column;
                source.pos += len;
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
