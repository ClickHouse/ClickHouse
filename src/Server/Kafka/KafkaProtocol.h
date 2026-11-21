#pragma once

#include <base/types.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>


/// Primitives of the Kafka wire protocol: https://kafka.apache.org/protocol
/// The message layouts are described by the JSON schemas in `clients/src/main/resources/common/message` of the Kafka repository.
namespace DB::Kafka
{

enum class ApiKey : Int16
{
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    InitProducerId = 22,
    SaslAuthenticate = 36,
};

enum class ErrorCode : Int16
{
    UNKNOWN_SERVER_ERROR = -1,
    NONE = 0,
    OFFSET_OUT_OF_RANGE = 1,
    CORRUPT_MESSAGE = 2,
    UNKNOWN_TOPIC_OR_PARTITION = 3,
    REQUEST_TIMED_OUT = 7,
    MESSAGE_TOO_LARGE = 10,
    COORDINATOR_NOT_AVAILABLE = 15,
    NOT_COORDINATOR = 16,
    INVALID_TOPIC_EXCEPTION = 17,
    ILLEGAL_GENERATION = 22,
    INCONSISTENT_GROUP_PROTOCOL = 23,
    INVALID_GROUP_ID = 24,
    UNKNOWN_MEMBER_ID = 25,
    INVALID_SESSION_TIMEOUT = 26,
    REBALANCE_IN_PROGRESS = 27,
    TOPIC_AUTHORIZATION_FAILED = 29,
    GROUP_AUTHORIZATION_FAILED = 30,
    UNSUPPORTED_SASL_MECHANISM = 33,
    ILLEGAL_SASL_STATE = 34,
    UNSUPPORTED_VERSION = 35,
    TOPIC_ALREADY_EXISTS = 36,
    INVALID_PARTITIONS = 37,
    INVALID_REPLICATION_FACTOR = 38,
    INVALID_CONFIG = 40,
    INVALID_REQUEST = 42,
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
    SASL_AUTHENTICATION_FAILED = 58,
    NON_EMPTY_GROUP = 68,
    GROUP_ID_NOT_FOUND = 69,
    UNSUPPORTED_COMPRESSION_TYPE = 76,
    INVALID_RECORD = 87,
};

/// Reads the fields of a request. `flexible` selects the compact encodings of the "flexible versions" (KIP-482).
class Reader
{
public:
    Reader(const char * begin_, const char * end_, bool flexible_ = false)
        : pos(begin_), end(end_), flexible(flexible_)
    {
    }

    void setFlexible(bool value) { flexible = value; }
    [[nodiscard]] bool isFlexible() const { return flexible; }

    Int8 int8();
    Int16 int16();
    Int32 int32();
    Int64 int64();
    UInt32 uint32();
    bool boolean() { return int8() != 0; }
    void uuid() { skip(16); }

    UInt64 unsignedVarInt();
    Int64 varLong();
    Int32 varInt() { return static_cast<Int32>(varLong()); }

    String string();
    std::optional<String> nullableString();
    String bytes() { return nullableBytes().value_or(String{}); }
    std::optional<String> nullableBytes();

    /// The length of an array, -1 for a null array.
    Int32 arrayLength();

    /// The tagged fields at the end of a structure, ignored.
    void taggedFields();

    std::string_view raw(size_t size);
    void skip(size_t size) { raw(size); }

    [[nodiscard]] bool eof() const { return pos == end; }
    [[nodiscard]] size_t remaining() const { return end - pos; }

private:
    const char * pos;
    const char * end;
    bool flexible;
};

/// Writes the fields of a response.
class Writer
{
public:
    explicit Writer(String & out_, bool flexible_ = false) : out(out_), flexible(flexible_) {}

    void setFlexible(bool value) { flexible = value; }
    [[nodiscard]] bool isFlexible() const { return flexible; }

    void int8(Int8 x) { out.push_back(static_cast<char>(x)); }
    void int16(Int16 x);
    void int32(Int32 x);
    void int64(Int64 x);
    void uint32(UInt32 x);
    void boolean(bool x) { int8(x ? 1 : 0); }
    void uuid() { out.append(16, '\0'); }
    void errorCode(ErrorCode code) { int16(static_cast<Int16>(code)); }

    void unsignedVarInt(UInt64 x);
    void varLong(Int64 x);
    void varInt(Int32 x) { varLong(x); }

    void string(std::string_view s);
    void nullableString(const std::optional<String> & s);
    void nullString();
    void bytes(std::string_view s);
    void nullableBytes(const std::optional<String> & s);

    void arrayLength(size_t size);
    void nullArray();

    /// An empty set of tagged fields, for flexible versions.
    void taggedFields();

    void raw(std::string_view s) { out.append(s); }

    [[nodiscard]] size_t size() const { return out.size(); }
    String & buffer() { return out; }

private:
    String & out;
    bool flexible;
};

/// One record of a record batch (the message format v2).
struct Record
{
    Int64 offset = 0;
    Int64 timestamp = -1;
    std::optional<String> key;
    std::optional<String> value;
    std::vector<std::pair<String, std::optional<String>>> headers;
};

struct RecordBatch
{
    Int64 base_offset = 0;
    Int16 attributes = 0;
    Int64 base_timestamp = -1;
    Int64 producer_id = -1;
    Int16 producer_epoch = -1;
    Int32 base_sequence = -1;
    std::vector<Record> records;

    [[nodiscard]] bool isTransactional() const { return attributes & 0x10; }
    [[nodiscard]] bool isControl() const { return attributes & 0x20; }
};

/// Parses a sequence of record batches, verifying the checksums and decompressing the records.
/// Only the message format v2 (magic 2) is supported, as in the Produce request since version 3.
std::vector<RecordBatch> parseRecordBatches(std::string_view data);

/// Appends one uncompressed record batch of the message format v2.
/// The offsets of the records must be ascending and fit into 32 bits relative to the first one.
void writeRecordBatch(String & out, const std::vector<Record> & records);

/// Appends a record batch without records, which moves the position of a consumer to the offset after `last_offset`.
void writeEmptyRecordBatch(String & out, Int64 last_offset);

/// A Kafka error code for the current exception.
ErrorCode getCurrentExceptionKafkaErrorCode();

}
