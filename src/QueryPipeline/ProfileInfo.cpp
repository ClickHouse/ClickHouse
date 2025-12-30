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
    bool unused_obsolete_field = false;
    readBinary(unused_obsolete_field, in);
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
    bool unused_obsolete_field = true;
    writeBinary(unused_obsolete_field, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION)
    {
        writeBinary(hasAppliedAggregation(), out);
        writeVarUInt(getRowsBeforeAggregation(), out);
    }
}


size_t ProfileInfo::getRowsBeforeLimit() const
{
    return rows_before_limit;
}


bool ProfileInfo::hasAppliedLimit() const
{
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
