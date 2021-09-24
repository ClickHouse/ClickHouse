#include <Storages/MergeTree/RequestResponse.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{


void PartitionReadRequest::serialize(WriteBuffer & out) const
{
    writeStringBinary(partition_id, out);
    writeStringBinary(part_name, out);
    writeStringBinary(projection_name, out);

    writeVarInt(block_range.begin, out);
    writeVarInt(block_range.end, out);

    writeDequeBinary(mark_ranges, out);
}


void PartitionReadRequest::deserialize(ReadBuffer & in)
{
    readStringBinary(partition_id, in);
    readStringBinary(part_name, in);
    readStringBinary(projection_name, in);

    readVarInt(block_range.begin, in);
    readVarInt(block_range.end, in);

    readDequeBinary(mark_ranges, in);
}


void PartitionReadResponce::serialize(WriteBuffer & out) const
{
    writeVarUInt(static_cast<UInt64>(denied), out);
    writeDequeBinary(mark_ranges, out);
}


void PartitionReadResponce::deserialize(ReadBuffer & in)
{
    UInt64 value;
    readVarUInt(value, in);
    denied = static_cast<bool>(value);
    readDequeBinary(mark_ranges, in);
}

}
