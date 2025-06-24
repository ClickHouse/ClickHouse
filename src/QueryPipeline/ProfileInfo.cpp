#include <QueryPipeline/ProfileInfo.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void ProfileInfo::read(ReadBuffer & in, UInt64 server_revision)
{
    readVarUInt(rows, in);
    readVarUInt(blocks, in);
    readVarUInt(bytes, in);
    readBinary(applied_limit, in);
    readVarUInt(rows_before_limit, in);
    readBinary(calculated_rows_before_limit, in);
    if (server_revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION)
    {
        readBinary(applied_aggregation, in);
        readVarUInt(rows_before_aggregation, in);
    }
}


void ProfileInfo::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(rows, out);
    writeVarUInt(blocks, out);
    writeVarUInt(bytes, out);
    writeBinary(hasAppliedLimit(), out);
    writeVarUInt(getRowsBeforeLimit(), out);
    writeBinary(calculated_rows_before_limit, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION)
    {
        writeBinary(hasAppliedAggregation(), out);
        writeVarUInt(getRowsBeforeAggregation(), out);
    }
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
    applied_aggregation = rhs.applied_aggregation;
    rows_before_aggregation = rhs.rows_before_aggregation;
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

size_t ProfileInfo::getRowsBeforeAggregation() const
{
    return rows_before_aggregation;
}


bool ProfileInfo::hasAppliedAggregation() const
{
    return applied_aggregation;
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
