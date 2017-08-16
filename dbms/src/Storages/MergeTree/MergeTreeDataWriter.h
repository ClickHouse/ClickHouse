#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>
#include <Interpreters/Context.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

struct BlockWithDateInterval
{
    Block block;
    UInt16 min_date = std::numeric_limits<UInt16>::max(); /// For further updating, see updateDates method.
    UInt16 max_date = std::numeric_limits<UInt16>::min();

    BlockWithDateInterval() = default;
    BlockWithDateInterval(const Block & block_, UInt16 min_date_, UInt16 max_date_)
        : block(block_), min_date(min_date_), max_date(max_date_) {}

    void updateDates(UInt16 date)
    {
        if (date < min_date)
            min_date = date;

        if (date > max_date)
            max_date = date;
    }
};

using BlocksWithDateIntervals = std::list<BlockWithDateInterval>;

 /** Writes new parts of data to the merge tree.
  */
class MergeTreeDataWriter
{
public:
    MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Logger::get(data.getLogName() + " (Writer)")) {}

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      */
    BlocksWithDateIntervals splitBlockIntoParts(const Block & block);

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithDateInterval & block);

private:
    MergeTreeData & data;

    Logger * log;
};

}
