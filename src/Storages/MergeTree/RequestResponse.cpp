#include <chrono>
#include <Storages/MergeTree/RequestResponse.h>

#include <Core/ProtocolDefines.h>
#include <Common/SipHash.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <consistent_hashing.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
}

namespace
{
     CoordinationMode validateAndGet(uint8_t candidate)
    {
        if (candidate <= static_cast<uint8_t>(CoordinationMode::MAX))
            return static_cast<CoordinationMode>(candidate);

        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown reading mode: {}", candidate);
    }
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

ParallelReadRequest ParallelReadRequest::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading "\
            "from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    CoordinationMode mode;
    size_t replica_num;
    size_t min_number_of_marks;
    RangesInDataPartsDescription description;

    uint8_t mode_candidate;
    readIntBinary(mode_candidate, in);
    mode = validateAndGet(mode_candidate);
    readIntBinary(replica_num, in);
    readIntBinary(min_number_of_marks, in);
    description.deserialize(in);

    return ParallelReadRequest(
        mode,
        replica_num,
        min_number_of_marks,
        std::move(description)
    );
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
    return fmt::format("{}. Finish: {}", description.describe(), finish);
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

    writeIntBinary(mode, out);
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

InitialAllRangesAnnouncement InitialAllRangesAnnouncement::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol versions for parallel reading " \
            "from replicas differ. Got: {}, supported version: {}",
            version, DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    CoordinationMode mode;
    RangesInDataPartsDescription description;
    size_t replica_num;

    uint8_t mode_candidate;
    readIntBinary(mode_candidate, in);
    mode = validateAndGet(mode_candidate);
    description.deserialize(in);
    readIntBinary(replica_num, in);

    return InitialAllRangesAnnouncement {
        mode,
        description,
        replica_num
    };
}

}
