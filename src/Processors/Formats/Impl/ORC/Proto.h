#pragma once

#include <base/types.h>
#include <base/extended_types.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

/// Structures of the ORC file metadata, which is serialized with Protocol Buffers.
/// See `orc_proto.proto` in the ORC specification: https://orc.apache.org/specification/ORCv1/
///
/// Only the messages and the fields that are used by the reader and the writer are represented.
/// The parser skips the unknown fields, as required by the Protocol Buffers rules, so the files
/// written by newer writers can be read.

namespace DB::ORC::Proto
{

/// Low-level reader of the Protocol Buffers wire format over a contiguous buffer.
/// Throws `INCORRECT_DATA` on a malformed or truncated message.
class WireReader
{
public:
    enum WireType : UInt8
    {
        VARINT = 0,
        FIXED64 = 1,
        LENGTH_DELIMITED = 2,
        START_GROUP = 3,
        END_GROUP = 4,
        FIXED32 = 5,
    };

    WireReader(const char * begin_, const char * end_) : pos(begin_), end(end_) {}
    explicit WireReader(std::string_view data) : pos(data.data()), end(data.data() + data.size()) {}

    /// Reads the next field key. Returns false at the end of the message.
    bool nextField(UInt32 & field_number, WireType & wire_type);

    UInt64 readVarint();
    Int64 readSignedVarint() { UInt64 v = readVarint(); return static_cast<Int64>((v >> 1) ^ (~(v & 1) + 1)); }
    UInt64 readFixed64();
    UInt32 readFixed32();
    double readDouble();
    std::string_view readBytes();

    /// Skips a field of the given wire type.
    void skip(WireType wire_type);

    /// Reads a scalar field that can be either packed or not packed.
    /// Returns false (and reads nothing) if the wire type is not suitable.
    [[nodiscard]] bool readRepeatedVarint(WireType wire_type, std::vector<UInt64> & out);

private:
    void check(size_t n) const;

    const char * pos;
    const char * end;
};

/// Low-level writer of the Protocol Buffers wire format.
class WireWriter
{
public:
    explicit WireWriter(std::string & out_) : out(out_) {}

    void writeVarint(UInt32 field, UInt64 value);
    void writeSignedVarint(UInt32 field, Int64 value) { writeVarint(field, (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63)); }
    void writeDouble(UInt32 field, double value);
    void writeBytes(UInt32 field, std::string_view value);
    void writePackedVarint(UInt32 field, const std::vector<UInt64> & values);
    void writePackedFixed64(UInt32 field, const std::vector<UInt64> & values);

    static void appendRawVarint(std::string & s, UInt64 value);

private:
    void writeKey(UInt32 field, UInt8 wire_type) { appendRawVarint(out, (static_cast<UInt64>(field) << 3) | wire_type); }

    std::string & out;
};


enum class CompressionKind : UInt8
{
    NONE = 0,
    ZLIB = 1,
    SNAPPY = 2,
    LZO = 3,
    LZ4 = 4,
    ZSTD = 5,
    BROTLI = 6,
};

struct PostScript
{
    UInt64 footer_length = 0;
    CompressionKind compression = CompressionKind::NONE;
    /// The default value in the specification.
    UInt64 compression_block_size = 256 * 1024;
    std::vector<UInt64> version;
    UInt64 metadata_length = 0;
    UInt32 writer_version = 0;
    std::string magic;

    void parse(std::string_view data);
    std::string serialize() const;
};

struct IntegerStatistics
{
    std::optional<Int64> minimum;
    std::optional<Int64> maximum;
    std::optional<Int64> sum;
};

struct DoubleStatistics
{
    std::optional<double> minimum;
    std::optional<double> maximum;
    std::optional<double> sum;
};

struct StringStatistics
{
    std::optional<std::string> minimum;
    std::optional<std::string> maximum;
    std::optional<Int64> sum;
    std::optional<std::string> lower_bound;
    std::optional<std::string> upper_bound;
};

struct BucketStatistics
{
    std::vector<UInt64> count;
};

struct DecimalStatistics
{
    std::optional<std::string> minimum;
    std::optional<std::string> maximum;
    std::optional<std::string> sum;
};

struct DateStatistics
{
    std::optional<Int32> minimum;
    std::optional<Int32> maximum;
};

struct TimestampStatistics
{
    /// In milliseconds, in the local time zone of the writer (deprecated).
    std::optional<Int64> minimum;
    std::optional<Int64> maximum;
    /// In milliseconds since the epoch in UTC.
    std::optional<Int64> minimum_utc;
    std::optional<Int64> maximum_utc;
    /// The sub-millisecond part of the nanoseconds plus one, if not zero.
    std::optional<Int32> minimum_nanos;
    std::optional<Int32> maximum_nanos;
};

struct BinaryStatistics
{
    std::optional<Int64> sum;
};

struct CollectionStatistics
{
    std::optional<UInt64> min_children;
    std::optional<UInt64> max_children;
    std::optional<UInt64> total_children;
};

struct ColumnStatistics
{
    std::optional<UInt64> number_of_values;
    std::optional<IntegerStatistics> int_statistics;
    std::optional<DoubleStatistics> double_statistics;
    std::optional<StringStatistics> string_statistics;
    std::optional<BucketStatistics> bucket_statistics;
    std::optional<DecimalStatistics> decimal_statistics;
    std::optional<DateStatistics> date_statistics;
    std::optional<BinaryStatistics> binary_statistics;
    std::optional<TimestampStatistics> timestamp_statistics;
    std::optional<bool> has_null;
    std::optional<UInt64> bytes_on_disk;
    std::optional<CollectionStatistics> collection_statistics;

    void parse(std::string_view data);
    std::string serialize() const;
};

struct RowIndexEntry
{
    std::vector<UInt64> positions;
    std::optional<ColumnStatistics> statistics;
};

struct RowIndex
{
    std::vector<RowIndexEntry> entry;

    void parse(std::string_view data);
    std::string serialize() const;
};

struct BloomFilter
{
    UInt32 num_hash_functions = 0;
    std::vector<UInt64> bitset;
    std::optional<std::string> utf8bitset;
};

struct BloomFilterIndex
{
    std::vector<BloomFilter> bloom_filter;

    void parse(std::string_view data);
};

enum class StreamKind : UInt32
{
    PRESENT = 0,
    DATA = 1,
    LENGTH = 2,
    DICTIONARY_DATA = 3,
    DICTIONARY_COUNT = 4,
    SECONDARY = 5,
    ROW_INDEX = 6,
    BLOOM_FILTER = 7,
    BLOOM_FILTER_UTF8 = 8,
    ENCRYPTED_INDEX = 9,
    ENCRYPTED_DATA = 10,
    STRIPE_STATISTICS = 100,
    FILE_STATISTICS = 101,
};

struct Stream
{
    std::optional<UInt32> kind;
    UInt32 column = 0;
    UInt64 length = 0;
};

enum class ColumnEncodingKind : UInt32
{
    DIRECT = 0,
    DICTIONARY = 1,
    DIRECT_V2 = 2,
    DICTIONARY_V2 = 3,
};

struct ColumnEncoding
{
    ColumnEncodingKind kind = ColumnEncodingKind::DIRECT;
    UInt32 dictionary_size = 0;
    /// 1 means the bloom filters use UTF-8 for the strings and UTC for the timestamps.
    std::optional<UInt32> bloom_encoding;
};

struct StripeFooter
{
    std::vector<Stream> streams;
    std::vector<ColumnEncoding> columns;
    std::optional<std::string> writer_timezone;
    /// Non-zero if the stripe has encrypted columns, which are not supported.
    size_t encryption_variants = 0;

    void parse(std::string_view data);
    std::string serialize() const;
};

struct StringPair
{
    std::string key;
    std::string value;
};

struct Type
{
    UInt32 kind = 0;
    std::vector<UInt64> subtypes;
    std::vector<std::string> field_names;
    std::optional<UInt32> maximum_length;
    std::optional<UInt32> precision;
    std::optional<UInt32> scale;
    std::vector<StringPair> attributes;
    /// Only for GEOMETRY and GEOGRAPHY.
    std::optional<std::string> crs;
    std::optional<UInt32> algorithm;
};

struct StripeInformation
{
    UInt64 offset = 0;
    UInt64 index_length = 0;
    UInt64 data_length = 0;
    UInt64 footer_length = 0;
    UInt64 number_of_rows = 0;
};

struct UserMetadataItem
{
    std::string name;
    std::string value;
};

struct StripeStatistics
{
    std::vector<ColumnStatistics> col_stats;
};

struct Metadata
{
    std::vector<StripeStatistics> stripe_stats;

    void parse(std::string_view data);
    std::string serialize() const;
};

struct Footer
{
    UInt64 header_length = 0;
    UInt64 content_length = 0;
    std::vector<StripeInformation> stripes;
    std::vector<Type> types;
    std::vector<UserMetadataItem> metadata;
    UInt64 number_of_rows = 0;
    std::vector<ColumnStatistics> statistics;
    UInt32 row_index_stride = 0;
    std::optional<UInt32> writer;
    bool has_encryption = false;
    std::optional<std::string> software_version;

    void parse(std::string_view data);
    std::string serialize() const;
};

}
