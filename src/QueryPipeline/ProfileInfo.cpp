#include <QueryPipeline/ProfileInfo.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/Block.h>

namespace DB
{

void ProfileInfo::read(ReadBuffer & in)
{
    readVarUInt(rows, in);
    readVarUInt(blocks, in);
    readVarUInt(bytes, in);
    readBinary(applied_limit, in);
    readVarUInt(rows_before_limit, in);
    readBinary(calculated_rows_before_limit, in);
}


void ProfileInfo::write(WriteBuffer & out) const
{
    writeVarUInt(rows, out);
    writeVarUInt(blocks, out);
    writeVarUInt(bytes, out);
    writeBinary(hasAppliedLimit(), out);
    writeVarUInt(getRowsBeforeLimit(), out);
    writeBinary(calculated_rows_before_limit, out);
}


void ProfileInfo::setFrom(const ProfileInfo & rhs, bool skip_block_size_info)
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


size_t ProfileInfo::getRowsBeforeLimit() const
{
    calculated_rows_before_limit = true;
    return rows_before_limit;
}


bool ProfileInfo::hasAppliedLimit() const
{
    calculated_rows_before_limit = true;
    return applied_limit;
}


void ProfileInfo::update(Block & block)
{
    update(block.rows(), block.bytes());
}

void ProfileInfo::update(size_t num_rows, size_t num_bytes)
{
    ++blocks;
    rows += num_rows;
    bytes += num_bytes;
}

}
