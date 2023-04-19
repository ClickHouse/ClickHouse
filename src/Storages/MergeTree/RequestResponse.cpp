#include <Storages/MergeTree/RequestResponse.h>

#include <Core/ProtocolDefines.h>
#include <Common/SipHash.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <consistent_hashing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int BAD_ARGUMENTS;
}

static void readMarkRangesBinary(MarkRanges & ranges, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too large ranges size: {}.", size);

    ranges.resize(size);
    for (size_t i = 0; i < size; ++i)
    {
        readBinary(ranges[i].begin, buf);
        readBinary(ranges[i].end, buf);
    }
}


static void writeMarkRangesBinary(const MarkRanges & ranges, WriteBuffer & buf)
{
    writeVarUInt(ranges.size(), buf);

    for (const auto & [begin, end] : ranges)
    {
        writeBinary(begin, buf);
        writeBinary(end, buf);
    }
}


void PartitionReadRequest::serialize(WriteBuffer & out) const
{
    /// Must be the first
    writeVarUInt(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION, out);

    writeStringBinary(partition_id, out);
    writeStringBinary(part_name, out);
    writeStringBinary(projection_name, out);

    writeVarInt(block_range.begin, out);
    writeVarInt(block_range.end, out);

    writeMarkRangesBinary(mark_ranges, out);
}


String PartitionReadRequest::toString() const
{
    WriteBufferFromOwnString out;
    out << "partition: " << partition_id << ", part: " << part_name;
    if (!projection_name.empty())
        out << ", projection: " << projection_name;
    out << ", block range: [" << block_range.begin << ", " << block_range.end << "]";
    out << ", mark ranges: ";

    bool is_first = true;
    for (const auto & [begin, end] : mark_ranges)
    {
        if (!is_first)
            out << ", ";
        out << "[" << begin << ", " << end << ")";
        is_first = false;
    }

    return out.str();
}


void PartitionReadRequest::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readVarUInt(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading \
            from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    readStringBinary(partition_id, in);
    readStringBinary(part_name, in);
    readStringBinary(projection_name, in);

    readVarInt(block_range.begin, in);
    readVarInt(block_range.end, in);

    readMarkRangesBinary(mark_ranges, in);
}

UInt64 PartitionReadRequest::getConsistentHash(size_t buckets) const
{
    SipHash hash;

    hash.update(partition_id.size());
    hash.update(partition_id);

    hash.update(part_name.size());
    hash.update(part_name);

    hash.update(projection_name.size());
    hash.update(projection_name);

    hash.update(block_range.begin);
    hash.update(block_range.end);

    hash.update(mark_ranges.size());
    for (const auto & range : mark_ranges)
    {
        hash.update(range.begin);
        hash.update(range.end);
    }

    return ConsistentHashing(hash.get64(), buckets);
}


void PartitionReadResponse::serialize(WriteBuffer & out) const
{
    /// Must be the first
    writeVarUInt(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION, out);

    writeBinary(denied, out);
    writeMarkRangesBinary(mark_ranges, out);
}


void PartitionReadResponse::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readVarUInt(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading \
            from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    UInt64 value;
    readVarUInt(value, in);
    denied = static_cast<bool>(value);
    readMarkRangesBinary(mark_ranges, in);
}

}
