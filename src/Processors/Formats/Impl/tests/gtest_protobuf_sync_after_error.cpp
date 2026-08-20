#include "config.h"

#if USE_PROTOBUF

#include <gtest/gtest.h>

#include <Processors/Formats/Impl/ProtobufRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufSerializer.h>
#include <IO/ReadBufferFromString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>

using namespace DB;

namespace
{

struct ProtobufFormatHelper
{
    static std::shared_ptr<ProtobufRowInputFormat> createFormat(
        ReadBuffer & read_buf,
        const Block & header,
        const String & proto_schema,
        UInt64 allow_errors_num = 10,
        bool with_length_delimiter = true)
    {
        RowInputFormatParams params;
        params.max_block_size_rows = 100;
        params.allow_errors_num = allow_errors_num;

        FormatSettings format_settings;
        format_settings.schema.format_schema = proto_schema;
        format_settings.schema.format_schema_message_name = "Row";
        format_settings.schema.format_schema_source = "string";
        format_settings.schema.format_schema_path = "/tmp";

        ProtobufSchemaInfo schema_info(format_settings, "Protobuf", header, false);

        return std::make_shared<ProtobufRowInputFormat>(
            read_buf,
            std::make_shared<const Block>(header),
            params,
            schema_info,
            with_length_delimiter,
            /* flatten_google_wrappers = */ false,
            /* oneof_presence = */ false,
            /* google_protos_path = */ "");
    }

    static Block uint32Header()
    {
        Block header;
        header.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "number"});
        return header;
    }

    static Block dateHeader()
    {
        Block header;
        header.insert({ColumnUInt16::create(), std::make_shared<DataTypeDate>(), "d"});
        return header;
    }
};

}

/// Truncated stream must not crash. Previously syncAfterError() dereferenced
/// a null reader after destroyReaderAndSerializer() in readRow's catch.
/// Now it throws an exception instead of crashing the server.
TEST(ProtobufRowInputFormat, TruncatedMessageDoesNotCrash)
{
    std::string truncated_data;
    truncated_data += '\x06'; // message length = 6
    truncated_data += '\x0d'; // field 1, wire type BITS32: tag = (1 << 3) | 5
    truncated_data += '\x01'; // 1 byte instead of the 4 expected

    ReadBufferFromString read_buf(truncated_data);
    auto header = ProtobufFormatHelper::uint32Header();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { fixed32 number = 1; }");

    EXPECT_THROW(format->read(), Exception);
}

/// Invalid date string ("bad!") followed by a valid date ("2024-01-15").
/// The bad row triggers CANNOT_PARSE_DATE (in isParseError). After the error,
/// destroyReaderAndSerializer nulls both. On the next iteration readRow recreates
/// them fresh, reads the valid message, and returns it.
TEST(ProtobufRowInputFormat, BadDateRecovery)
{
    std::string data;

    /// Bad message: field 1 (string) = "bad!" — not a valid date
    data += '\x06'; // message length = 6
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x04'; // string length = 4
    data += "bad!";

    /// Valid message: field 1 (string) = "2024-01-15"
    data += '\x0c'; // message length = 12
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x0a'; // string length = 10
    data += "2024-01-15";

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::dateHeader();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { string d = 1; }");

    Chunk chunk;
    ASSERT_NO_THROW(chunk = format->read());
    ASSERT_EQ(chunk.getNumRows(), 1);
    const auto & col = chunk.getColumns()[0];
    /// 2024-01-15 = day 19737 since epoch
    EXPECT_EQ(col->getUInt(0), 19737);
}

/// Bad date field followed by another field in the SAME message, then a valid message.
/// The bad message has: field 1 (string "bad!") + field 2 (fixed32 = 99).
/// The valid message has: field 1 (string "2024-01-15") + field 2 (fixed32 = 7).
/// After skipping the bad row, the valid row must still be returned.
TEST(ProtobufRowInputFormat, BadDateWithTrailingFieldRecovery)
{
    std::string data;

    /// Bad message: length = 11, field 1 = "bad!", field 2 = 99
    data += '\x0b'; // message length = 11
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x04'; // string length = 4
    data += "bad!";
    data += '\x15'; // field 2, wire type BITS32: tag = (2 << 3) | 5
    data += '\x63'; // 99 in little-endian
    data += '\x00';
    data += '\x00';
    data += '\x00';

    /// Valid message: length = 17, field 1 = "2024-01-15", field 2 = 7
    data += '\x11'; // message length = 17
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x0a'; // string length = 10
    data += "2024-01-15";
    data += '\x15'; // field 2, wire type BITS32
    data += '\x07';
    data += '\x00';
    data += '\x00';
    data += '\x00';

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::dateHeader();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { string d = 1; fixed32 n = 2; }");

    Chunk chunk;
    ASSERT_NO_THROW(chunk = format->read());
    ASSERT_EQ(chunk.getNumRows(), 1);
    const auto & col = chunk.getColumns()[0];
    EXPECT_EQ(col->getUInt(0), 19737);
}

/// Two valid messages read correctly.
TEST(ProtobufRowInputFormat, ValidMultipleMessages)
{
    std::string data;

    data += '\x05';
    data += '\x0d';
    data += '\x07';
    data += '\x00';
    data += '\x00';
    data += '\x00';

    data += '\x05';
    data += '\x0d';
    data += '\x2a';
    data += '\x00';
    data += '\x00';
    data += '\x00';

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::uint32Header();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { fixed32 number = 1; }");

    Chunk chunk;
    ASSERT_NO_THROW(chunk = format->read());
    ASSERT_EQ(chunk.getNumRows(), 2);
    const auto & col = chunk.getColumns()[0];
    EXPECT_EQ(col->getUInt(0), 7);
    EXPECT_EQ(col->getUInt(1), 42);
}

/// Multiple consecutive bad rows followed by a valid row.
/// All bad rows are skipped, the valid row at the end is returned.
TEST(ProtobufRowInputFormat, MultipleErrorsThenValidRow)
{
    std::string data;

    /// Bad message 1: field 1 (string) = "xxx"
    data += '\x05'; // message length = 5
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x03'; // string length = 3
    data += "xxx";

    /// Bad message 2: field 1 (string) = "yyy!"
    data += '\x06'; // message length = 6
    data += '\x0a';
    data += '\x04'; // string length = 4
    data += "yyy!";

    /// Bad message 3: field 1 (string) = "zz"
    data += '\x04'; // message length = 4
    data += '\x0a';
    data += '\x02'; // string length = 2
    data += "zz";

    /// Valid message: field 1 (string) = "2024-01-15"
    data += '\x0c'; // message length = 12
    data += '\x0a';
    data += '\x0a'; // string length = 10
    data += "2024-01-15";

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::dateHeader();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { string d = 1; }");

    Chunk chunk;
    ASSERT_NO_THROW(chunk = format->read());
    ASSERT_EQ(chunk.getNumRows(), 1);
    const auto & col = chunk.getColumns()[0];
    EXPECT_EQ(col->getUInt(0), 19737);
}

/// Non-parse error leaves reader and serializer alive mid-message.
/// Calling read() again should not crash or produce garbage — it should
/// either throw again or return nothing.
TEST(ProtobufRowInputFormat, NonParseErrorReaderLeftAlive)
{
    std::string data;

    /// Bad message: invalid wire type (7) → UNKNOWN_PROTOBUF_FORMAT (not in isParseError)
    data += '\x03'; // message length = 3
    data += '\x0f'; // field 1, wire type 7 (invalid): tag = (1 << 3) | 7
    data += '\x00';
    data += '\x00';

    /// Valid message after it
    data += '\x05'; // message length = 5
    data += '\x0d'; // field 1, wire type BITS32
    data += '\x07';
    data += '\x00';
    data += '\x00';
    data += '\x00';

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::uint32Header();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { fixed32 number = 1; }",
        /* allow_errors_num = */ 0);

    /// First read throws (non-parse error, not skippable)
    EXPECT_THROW(format->read(), Exception);

    /// Second read: reader/serializer were left alive mid-message from the first call.
    /// The valid message follows in the stream. Verify no crash and check what happens.
    Chunk chunk;
    ASSERT_NO_THROW(chunk = format->read());
    /// The valid message (value=7) should be readable since the serializer's internal
    /// catch already called endMessage on the bad message before re-throwing.
    EXPECT_EQ(chunk.getNumRows(), 1);
    if (chunk.getNumRows() > 0)
        EXPECT_EQ(chunk.getColumns()[0]->getUInt(0), 7);
}

/// A message whose length prefix claims more bytes than exist, with a bad date field.
/// The parse error (CANNOT_PARSE_DATE) fires, then endMessage(true) tries to skip to
/// the declared boundary but the stream is truncated → endMessage throws.
/// The error must NOT be silently skipped — it should propagate as a query failure.
TEST(ProtobufRowInputFormat, ResyncFailureMustNotSkip)
{
    std::string data;

    /// Bad message: length prefix claims 20 bytes but only 6 bytes of body follow.
    /// Contains field 1 (string "bad!") which triggers CANNOT_PARSE_DATE.
    /// After that, endMessage(true) tries to ignore(20 - 6 = 14 bytes) → EOF → throws.
    data += '\x14'; // message length = 20 (but only 6 body bytes provided)
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED
    data += '\x04'; // string length = 4
    data += "bad!";

    /// A "valid" message after it — but we should never reach it because the
    /// bad message's boundary extends past EOF.
    data += '\x05';
    data += '\x0d';
    data += '\x07';
    data += '\x00';
    data += '\x00';
    data += '\x00';

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::dateHeader();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header, "syntax = \"proto3\";\nmessage Row { string d = 1; }");

    /// Must throw — resync to the declared message boundary failed (stream truncated),
    /// so the error cannot be safely skipped.
    EXPECT_THROW(format->read(), Exception);
}

/// ProtobufSingle (with_length_delimiter = false): one message = one row = entire input.
/// If a field parse error occurs mid-message, there's no next row to skip to.
/// allowSyncAfterError must return false so no partial row is produced from the
/// remaining fields of the same message.
TEST(ProtobufRowInputFormat, ProtobufSingleNoPartialRow)
{
    /// Single message with two fields:
    ///   field 1 (string) = "bad!" — invalid date, triggers CANNOT_PARSE_DATE
    ///   field 2 (fixed32) = 42
    /// Without the fix, after the bad date error the reader is recreated and
    /// tries to parse field 2's bytes as a new root message, producing garbage.
    std::string data;
    data += '\x0a'; // field 1, wire type LENGTH_DELIMITED: tag = (1 << 3) | 2
    data += '\x04'; // string length = 4
    data += "bad!";
    data += '\x15'; // field 2, wire type BITS32: tag = (2 << 3) | 5
    data += '\x2a'; // 42 in little-endian
    data += '\x00';
    data += '\x00';
    data += '\x00';

    ReadBufferFromString read_buf(data);
    auto header = ProtobufFormatHelper::dateHeader();
    auto format = ProtobufFormatHelper::createFormat(
        read_buf, header,
        "syntax = \"proto3\";\nmessage Row { string d = 1; fixed32 n = 2; }",
        /* allow_errors_num = */ 10,
        /* with_length_delimiter = */ false);

    /// Must throw — ProtobufSingle cannot skip errors because there's no next row.
    /// Without the fix, the error is silently swallowed and an empty result is returned.
    EXPECT_THROW(format->read(), Exception);
}

#endif

