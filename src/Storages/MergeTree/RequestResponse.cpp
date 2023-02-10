#include <chrono>
#include <Storages/MergeTree/RequestResponse.h>

#include <Core/ProtocolDefines.h>
#include <Common/SipHash.h>
#include "IO/VarInt.h"
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <consistent_hashing.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
}

void ParallelReadRequest::serialize(WriteBuffer & out) const
{
    UInt64 version = DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    /// Must be the first
    writeIntBinary(version, out);

    writeIntBinary(mode, out);
    writeIntBinary(replica_num, out);
    writeIntBinary(min_number_of_marks, out);
    description.serialize(out);
}


String ParallelReadRequest::describe() const
{
    String result;
    result += fmt::format("replica_num: {} \n", replica_num);
    result += fmt::format("min_num_of_marks: {} \n", min_number_of_marks);
    result += description.describe();
    return result;
}

void ParallelReadRequest::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading "\
            "from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    readIntBinary(mode, in);
    readIntBinary(replica_num, in);
    readIntBinary(min_number_of_marks, in);
    description.deserialize(in);
}

void ParallelReadRequest::merge(ParallelReadRequest & other)
{
    assert(mode == other.mode);
    assert(replica_num == other.replica_num);
    assert(min_number_of_marks == other.min_number_of_marks);
    description.merge(other.description);
}

void ParallelReadResponse::serialize(WriteBuffer & out) const
{
    UInt64 version = DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    /// Must be the first
    writeIntBinary(version, out);

    writeBoolText(finish, out);
    description.serialize(out);
}

String ParallelReadResponse::describe() const
{
    String result;
    result += fmt::format("finish: {} \n", finish);
    result += description.describe();
    return result;
}

void ParallelReadResponse::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading " \
            "from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    readBoolText(finish, in);
    description.deserialize(in);
}


void InitialAllRangesAnnouncement::serialize(WriteBuffer & out) const
{
    UInt64 version = DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    /// Must be the first
    writeIntBinary(version, out);

    description.serialize(out);
    writeIntBinary(replica_num, out);
}


String InitialAllRangesAnnouncement::describe()
{
    String result;
    result += description.describe();
    result += fmt::format("----------\nReceived from {} replica\n", replica_num);
    return result;
}

void InitialAllRangesAnnouncement::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading " \
            "from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    description.deserialize(in);
    readIntBinary(replica_num, in);
}

}
