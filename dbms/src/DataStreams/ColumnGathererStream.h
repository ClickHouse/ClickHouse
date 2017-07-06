#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/PODArray.h>


namespace Poco { class Logger; }


namespace DB
{


/// Tiny struct, stores number of a Part from which current row was fetched, and insertion flag.
struct RowSourcePart
{
    RowSourcePart() = default;

    RowSourcePart(size_t source_num, bool flag = false)
    {
        static_assert(sizeof(*this) == 1, "Size of RowSourcePart is too big due to compiler settings");
        setSourceNum(source_num);
        setSkipFlag(flag);
    }

    /// Data is equal to getSourceNum() if flag is false
    UInt8 getData() const        { return data; }

    size_t getSourceNum() const { return data & MASK_NUMBER; }

    /// In CollapsingMergeTree case flag means "skip this rows"
    bool getSkipFlag() const     { return (data & MASK_FLAG) != 0; }

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

private:
    UInt8 data;
};

using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should conatin exactly one column.
  */
class ColumnGathererStream : public IProfilingBlockInputStream
{
public:
    ColumnGathererStream(const BlockInputStreams & source_streams, const String & column_name_,
                         const MergedRowSources & row_source_, size_t block_preferred_size_ = DEFAULT_MERGE_BLOCK_SIZE);

    String getName() const override { return "ColumnGatherer"; }

    String getID() const override;

    Block readImpl() override;

    void readSuffixImpl() override;

    /// for use in implementations of IColumn::gather()
    template<typename Column>
    void gather(Column & column_res);

private:
    String name;
    ColumnWithTypeAndName column;
    const MergedRowSources & row_source;

    /// Cache required fileds
    struct Source
    {
        const IColumn * column;
        size_t pos;
        size_t size;
        Block block;

        Source(Block && block_, const String & name) : block(std::move(block_))
        {
            update(name);
        }

        void update(const String & name)
        {
            column = block.getByName(name).column.get();
            size = block.rows();
            pos = 0;
        }
    };

    void init();
    void fetchNewBlock(Source & source, size_t source_num);

    std::vector<Source> sources;

    size_t pos_global_start = 0;
    size_t block_preferred_size;

    Block block_res;

    Poco::Logger * log;
};

template<typename Column>
void ColumnGathererStream::gather(Column & column_res)
{
    size_t global_size = row_source.size();
    size_t curr_block_preferred_size = std::min(global_size - pos_global_start,  block_preferred_size);
    column_res.reserve(curr_block_preferred_size);

    size_t pos_global = pos_global_start;
    while (pos_global < global_size && column_res.size() < curr_block_preferred_size)
    {
        auto source_data = row_source[pos_global].getData();
        bool source_skip = row_source[pos_global].getSkipFlag();
        auto source_num = row_source[pos_global].getSourceNum();
        Source & source = sources[source_num];

        if (source.pos >= source.size) /// Fetch new block from source_num part
        {
            fetchNewBlock(source, source_num);
        }

        /// Consecutive optimization. TODO: precompute lens
        size_t len = 1;
        size_t max_len = std::min(global_size - pos_global, source.size - source.pos); // interval should be in the same block
        for (; len < max_len && source_data == row_source[pos_global + len].getData(); ++len);

        if (!source_skip)
        {
            /// Whole block could be produced via copying pointer from current block
            if (source.pos == 0 && source.size == len)
            {
                /// If current block already contains data, return it. We will be here again on next read() iteration.
                if (column_res.size() != 0)
                    break;

                block_res.getByPosition(0).column = source.block.getByName(name).column;
                source.pos += len;
                pos_global += len;
                break;
            }
            else if (len == 1)
            {
                column_res.insertFrom(*source.column, source.pos);
            }
            else
            {
                column_res.insertRangeFrom(*source.column, source.pos, len);
            }
        }

        source.pos += len;
        pos_global += len;
    }
    pos_global_start = pos_global;
}

}
