#include <DataStreams/BlockStreamProfileInfo.h>
#include <DataStreams/IBlockInputStream.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/Block.h>

namespace DB
{

void BlockStreamProfileInfo::read(ReadBuffer & in)
{
    readVarUInt(rows, in);
    readVarUInt(blocks, in);
    readVarUInt(bytes, in);
    readBinary(applied_limit, in);
    readVarUInt(rows_before_limit, in);
    readBinary(calculated_rows_before_limit, in);
}


void BlockStreamProfileInfo::write(WriteBuffer & out) const
{
    writeVarUInt(rows, out);
    writeVarUInt(blocks, out);
    writeVarUInt(bytes, out);
    writeBinary(hasAppliedLimit(), out);
    writeVarUInt(getRowsBeforeLimit(), out);
    writeBinary(calculated_rows_before_limit, out);
}


void BlockStreamProfileInfo::setFrom(const BlockStreamProfileInfo & rhs, bool skip_block_size_info)
{
    if (!skip_block_size_info)
    {
        rows = rhs.rows;
        blocks = rhs.blocks;
        bytes = rhs.bytes;
    }
    applied_limit = rhs.applied_limit;
    rows_before_limit = rhs.rows_before_limit;
    calculated_rows_before_limit = rhs.calculated_rows_before_limit;
}


size_t BlockStreamProfileInfo::getRowsBeforeLimit() const
{
    if (!calculated_rows_before_limit)
        calculateRowsBeforeLimit();
    return rows_before_limit;
}


bool BlockStreamProfileInfo::hasAppliedLimit() const
{
    if (!calculated_rows_before_limit)
        calculateRowsBeforeLimit();
    return applied_limit;
}


void BlockStreamProfileInfo::update(Block & block)
{
    update(block.rows(), block.bytes());
}

void BlockStreamProfileInfo::update(size_t num_rows, size_t num_bytes)
{
    ++blocks;
    rows += num_rows;
    bytes += num_bytes;
}


void BlockStreamProfileInfo::collectInfosForStreamsWithName(const char * name, BlockStreamProfileInfos & res) const
{
    if (!parent)
        return;

    if (parent->getName() == name)
    {
        res.push_back(this);
        return;
    }

    parent->forEachChild([&] (IBlockInputStream & child)
    {
        child.getProfileInfo().collectInfosForStreamsWithName(name, res);
        return false;
    });
}


void BlockStreamProfileInfo::calculateRowsBeforeLimit() const
{
    calculated_rows_before_limit = true;

    /// is there a Limit?
    BlockStreamProfileInfos limits;
    collectInfosForStreamsWithName("Limit", limits);

    if (!limits.empty())
    {
        applied_limit = true;

        /** Take the number of lines read below `PartialSorting`, if any, or below `Limit`.
          * This is necessary, because sorting can return only part of the rows.
          */
        BlockStreamProfileInfos partial_sortings;
        collectInfosForStreamsWithName("PartialSorting", partial_sortings);

        BlockStreamProfileInfos & limits_or_sortings = partial_sortings.empty() ? limits : partial_sortings;

        for (const BlockStreamProfileInfo * info_limit_or_sort : limits_or_sortings)
        {
            info_limit_or_sort->parent->forEachChild([&] (IBlockInputStream & child)
            {
                rows_before_limit += child.getProfileInfo().rows;
                return false;
            });
        }
    }
    else
    {
        /// Then the data about `rows_before_limit` can be in `RemoteBlockInputStream` (come from a remote server).
        BlockStreamProfileInfos remotes;
        collectInfosForStreamsWithName("Remote", remotes);
        collectInfosForStreamsWithName("TreeExecutor", remotes);

        if (remotes.empty())
            return;

        for (const auto & info : remotes)
        {
            if (info->applied_limit)
            {
                applied_limit = true;
                rows_before_limit += info->rows_before_limit;
            }
        }
    }
}

}
