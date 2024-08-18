#include <chrono>
#include <Storages/MergeTree/RequestResponse.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>

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
/// Previously we had a separate protocol version number for parallel replicas.
/// But we didn't maintain backward compatibility and every protocol change was breaking.
/// Now we have to support at least minimal tail of the previous versions and the implementation
/// is based on the common tcp protocol version as in all other places.
constexpr UInt64 DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION = 3;

CoordinationMode validateAndGet(uint8_t candidate)
{
    if (candidate <= static_cast<uint8_t>(CoordinationMode::MAX))
        return static_cast<CoordinationMode>(candidate);

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown reading mode: {}", candidate);
}
}

void ParallelReadRequest::serialize(WriteBuffer & out) const
{
    /// Must be the first
    writeIntBinary(DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION, out);

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
    if (version != DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Protocol versions for parallel reading "
            "from replicas differ. Got: {}, supported version: {}",
            version,
            DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION);

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
    /// Must be the first
    writeIntBinary(DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION, out);

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
    if (version != DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Protocol versions for parallel reading "
            "from replicas differ. Got: {}, supported version: {}",
            version,
            DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    readBoolText(finish, in);
    description.deserialize(in);
}


void InitialAllRangesAnnouncement::serialize(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    /// Must be the first
    writeIntBinary(DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION, out);

    writeIntBinary(mode, out);
    description.serialize(out);
    writeIntBinary(replica_num, out);
    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_ADAPTIVE_MARK_SEGMENT_FOR_PARALLEL_REPLICAS)
        writeIntBinary(mark_segment_size, out);
}


String InitialAllRangesAnnouncement::describe()
{
    String result;
    result += description.describe();
    result += fmt::format("----------\nReceived from {} replica\n", replica_num);
    return result;
}

InitialAllRangesAnnouncement InitialAllRangesAnnouncement::deserialize(ReadBuffer & in, UInt64 client_protocol_revision)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Protocol versions for parallel reading "
            "from replicas differ. Got: {}, supported version: {}",
            version,
            DEPRECATED_FIELD_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    CoordinationMode mode;
    RangesInDataPartsDescription description;
    size_t replica_num;

    uint8_t mode_candidate;
    readIntBinary(mode_candidate, in);
    mode = validateAndGet(mode_candidate);
    description.deserialize(in);
    readIntBinary(replica_num, in);

    size_t mark_segment_size = 128;
    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_ADAPTIVE_MARK_SEGMENT_FOR_PARALLEL_REPLICAS)
        readIntBinary(mark_segment_size, in);

    return InitialAllRangesAnnouncement{
        mode,
        description,
        replica_num,
        mark_segment_size,
    };
}

}
