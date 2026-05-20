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
CoordinationMode validateAndGet(uint8_t candidate)
{
    if (candidate <= static_cast<uint8_t>(CoordinationMode::MAX))
        return static_cast<CoordinationMode>(candidate);

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown reading mode: {}", candidate);
}
}

void ParallelReadRequest::serialize(WriteBuffer & out, UInt64 initiator_pr_protocol_version, UInt64 initiator_tcp_protocol_version) const
{
    /// Previously we didn't maintain backward compatibility and every change was breaking.
    /// Particularly, we had an equality check for the version. To work around that code
    /// in previous server versions we now have to lie to them about the version.
    const UInt64 version = initiator_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        ? DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
        : DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    writeIntBinary(version, out);

    writeIntBinary(mode, out);
    writeIntBinary(replica_num, out);
    writeIntBinary(min_marks_per_request, out);
    description.serialize(out, initiator_pr_protocol_version);
    if (initiator_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        writeStringBinary(stream_id, out);
}

String ParallelReadRequest::describe() const
{
    String result
        = fmt::format("replica_num {}, stream {}, min_num_of_marks {}, ", replica_num, stream_id, min_marks_per_request);
    result += description.describe();
    return result;
}

ParallelReadRequest ParallelReadRequest::deserialize(ReadBuffer & in, UInt64 replica_pr_protocol_version)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version < DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Parallel replicas protocol version is too old. Got: {}, min supported version: {}",
            version,
            DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    CoordinationMode mode;
    size_t replica_num;
    size_t min_marks_per_request;
    RangesInDataPartsDescription description;

    uint8_t mode_candidate;
    readIntBinary(mode_candidate, in);
    mode = validateAndGet(mode_candidate);
    readIntBinary(replica_num, in);
    readIntBinary(min_marks_per_request, in);
    description.deserialize(in, replica_pr_protocol_version);

    String stream_id;
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        readStringBinary(stream_id, in);
    return ParallelReadRequest(mode, replica_num, min_marks_per_request, std::move(description), std::move(stream_id));
}

void ParallelReadResponse::serialize(WriteBuffer & out, UInt64 replica_pr_protocol_version, UInt64 replica_tcp_protocol_version) const
{
    /// Previously we didn't maintain backward compatibility and every change was breaking.
    /// Particularly, we had an equality check for the version. To work around that code
    /// in previous server versions we now have to lie to them about the version.
    UInt64 version = replica_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        ? DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
        : DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    /// Must be the first
    writeIntBinary(version, out);

    writeBoolText(finish, out);
    description.serialize(out, replica_pr_protocol_version);
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        writeStringBinary(stream_id, out);
}

String ParallelReadResponse::describe() const
{
    return fmt::format("{}. Finish: {}", description.describe(), finish);
}

void ParallelReadResponse::deserialize(ReadBuffer & in, UInt64 replica_pr_protocol_version)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version < DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Parallel replicas protocol version is too old. Got: {}, min supported version: {}",
            version,
            DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    readBoolText(finish, in);
    description.deserialize(in, replica_pr_protocol_version);
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        readStringBinary(stream_id, in);
}


void InitialAllRangesAnnouncement::serialize(
    WriteBuffer & out, UInt64 initiator_pr_protocol_version, UInt64 initiator_tcp_protocol_version) const
{
    /// Previously we didn't maintain backward compatibility and every change was breaking.
    /// Particularly, we had an equality check for the version. To work around that code
    /// in previous server versions we now have to lie to them about the version.
    UInt64 version = initiator_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        ? DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
        : DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION;
    writeIntBinary(version, out);

    writeIntBinary(mode, out);
    description.serialize(out, initiator_pr_protocol_version);
    writeIntBinary(replica_num, out);
    if (initiator_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MARK_SEGMENT_SIZE_FIELD)
        writeIntBinary(mark_segment_size, out);
    if (initiator_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        writeIntBinary(min_marks_per_request, out);
    if (initiator_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        writeStringBinary(stream_id, out);
}


String InitialAllRangesAnnouncement::describe()
{
    return fmt::format("replica {}, mode {}, {}", replica_num, mode, description.describe());
}

InitialAllRangesAnnouncement InitialAllRangesAnnouncement::deserialize(ReadBuffer & in, UInt64 replica_pr_protocol_version)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version < DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Parallel replicas protocol version is too old. Got: {}, min supported version: {}",
            version,
            DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION);

    CoordinationMode mode;
    RangesInDataPartsDescription description;
    size_t replica_num;

    uint8_t mode_candidate;
    readIntBinary(mode_candidate, in);
    mode = validateAndGet(mode_candidate);
    description.deserialize(in, replica_pr_protocol_version);
    readIntBinary(replica_num, in);

    size_t mark_segment_size = 128;
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MARK_SEGMENT_SIZE_FIELD)
        readIntBinary(mark_segment_size, in);

    size_t min_marks_per_request = 0;
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        readIntBinary(min_marks_per_request, in);

    String stream_id;
    if (replica_pr_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID)
        readStringBinary(stream_id, in);
    return InitialAllRangesAnnouncement{mode, description, replica_num, mark_segment_size, min_marks_per_request, std::move(stream_id)};
}

}
