#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/PostgreSQLProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ParserPreparedStatement.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>

#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

using namespace DB;
namespace Messaging = DB::PostgreSQLProtocol::Messaging;
namespace PreparedStatements = DB::PostgreSQLProtocol::PostgresPreparedStatements;

namespace
{

void putUInt8(std::string & s, UInt8 v)
{
    s.push_back(static_cast<char>(v));
}

void putInt16(std::string & s, Int16 v)
{
    s.push_back(static_cast<char>((v >> 8) & 0xFF));
    s.push_back(static_cast<char>(v & 0xFF));
}

void putInt32(std::string & s, Int32 v)
{
    for (int i = 3; i >= 0; --i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

std::string framePayload(std::string payload)
{
    std::string bytes;
    putInt32(bytes, static_cast<Int32>(4 + payload.size()));
    bytes += payload;
    return bytes;
}

/// Run `body` over the bytes and report whether it threw UNKNOWN_PACKET_FROM_CLIENT.
template <typename F>
bool throwsUnknownPacket(const std::string & bytes, F && body)
{
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    try
    {
        body(in);
        return false;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        return e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT;
    }
}

template <typename TMessage>
void expectUnknownPacketAndAligned(std::string payload)
{
    /// These bytes look like a `Sync` frame, but they are part of the rejected message payload.
    payload.append("S\0\0\0\4", 5);
    std::string bytes = framePayload(std::move(payload));
    bytes.push_back('X');

    ReadBufferFromMemory in(bytes.data(), bytes.size());
    TMessage msg;
    try
    {
        msg.deserialize(in);
        FAIL() << "Expected UNKNOWN_PACKET_FROM_CLIENT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }

    char marker = 0;
    in.readStrict(marker);
    EXPECT_EQ(marker, 'X');
}

template <typename TMessage>
void expectIncompletePayloadAndAligned(std::string payload = {})
{
    /// The following `Sync` must remain unread if the current payload is incomplete.
    std::string bytes = framePayload(std::move(payload));
    bytes.append("S\0\0\0\4", 5);

    ReadBufferFromMemory in(bytes.data(), bytes.size());
    TMessage msg;
    EXPECT_THROW(msg.deserialize(in), Exception);

    char message_type = 0;
    in.readStrict(message_type);
    EXPECT_EQ(message_type, 'S');
}

template <typename TMessage>
void expectTrailingPayloadIsRejectedAndAligned(std::string payload)
{
    /// These bytes look like a `Sync` frame, but they are trailing bytes in the current message.
    payload.append("S\0\0\0\4", 5);
    std::string bytes = framePayload(std::move(payload));
    bytes.push_back('X');

    ReadBufferFromMemory in(bytes.data(), bytes.size());
    TMessage msg;
    try
    {
        msg.deserialize(in);
        FAIL() << "Expected UNKNOWN_PACKET_FROM_CLIENT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }

    char marker = 0;
    in.readStrict(marker);
    EXPECT_EQ(marker, 'X');
}

}

TEST(PostgreSQLProtocol, DropMessageRejectsLengthBelowFour)
{
    /// The message-length field includes its own four bytes, so anything below 4 underflows `size - 4`.
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        WriteBufferFromOwnString out;
        EXPECT_TRUE(throwsUnknownPacket(bytes, [&](ReadBuffer & in)
        {
            Messaging::MessageTransport mt(&in, &out);
            mt.dropMessage();
        })) << "size = " << size;
    }

    /// A well-formed length skips the declared number of trailing bytes.
    std::string bytes;
    putInt32(bytes, 8);
    bytes += "abcd";
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    EXPECT_NO_THROW(mt.dropMessage());
}

TEST(PostgreSQLProtocol, SASLResponseRejectsLengthBelowFour)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putUInt8(bytes, 'p');
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::SASLResponse msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }

    /// size == 4 means an empty SASL payload.
    std::string bytes;
    putUInt8(bytes, 'p');
    putInt32(bytes, 4);
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    Messaging::SASLResponse msg;
    EXPECT_NO_THROW(msg.deserialize(in));
    EXPECT_TRUE(msg.sasl_mechanism.empty());
}

TEST(PostgreSQLProtocol, SASLInitialResponseHandlesMechanismLength)
{
    auto build = [](Int32 size_sasl_mechanism, const std::string & data)
    {
        std::string bytes;
        putUInt8(bytes, 'p');
        putInt32(bytes, 0); /// the outer size field is not used for bounds here
        bytes += "SCRAM-SHA-256";
        bytes.push_back('\0');
        putInt32(bytes, size_sasl_mechanism);
        bytes += data;
        return bytes;
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](ReadBuffer & in)
    {
        Messaging::SASLInitialResponse msg;
        msg.deserialize(in);
    }));

    /// -1 is the protocol sentinel for "no initial response".
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::SASLInitialResponse msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_TRUE(msg.sasl_mechanism.empty());
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(3, "abc");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::SASLInitialResponse msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_EQ(msg.sasl_mechanism, "abc");
    }
}

TEST(PostgreSQLProtocol, BindHandlesParameterLength)
{
    auto build = [](Int32 sz_param, const std::string & data)
    {
        std::string payload;
        payload.push_back('\0'); /// empty portal name
        payload.push_back('\0'); /// empty statement name
        putInt16(payload, 0); /// no parameter format codes
        putInt16(payload, 1); /// one parameter
        putInt32(payload, sz_param);
        payload += data;
        putInt16(payload, 0); /// no result format codes
        return framePayload(std::move(payload));
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](ReadBuffer & in)
    {
        Messaging::BindQuery msg;
        msg.deserialize(in);
    }));

    /// -1 represents protocol `NULL` without payload bytes.
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], std::nullopt);
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(2, "hi");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
    }
}

TEST(PostgreSQLProtocol, BindRejectsNegativeCounts)
{
    /// Reject negative counts after consuming the full declared payload.
    {
        std::string payload;
        payload.push_back('\0'); /// empty portal name
        payload.push_back('\0'); /// empty statement name
        putInt16(payload, 0); /// no parameter format codes
        putInt16(payload, -1); /// negative parameter count
        expectUnknownPacketAndAligned<Messaging::BindQuery>(std::move(payload));
    }

    /// A negative result-format-code count is also malformed.
    {
        std::string payload;
        payload.push_back('\0');
        payload.push_back('\0');
        putInt16(payload, 0); /// no parameter format codes
        putInt16(payload, 0); /// no parameters
        putInt16(payload, -1); /// negative result-format-code count
        expectUnknownPacketAndAligned<Messaging::BindQuery>(std::move(payload));
    }
}

TEST(PostgreSQLProtocol, ParseRejectsNegativeCountAndKeepsStreamAligned)
{
    std::string payload;
    payload.push_back('\0'); /// empty statement name
    payload += "SELECT 1";
    payload.push_back('\0');
    putInt16(payload, -1); /// negative parameter count
    expectUnknownPacketAndAligned<Messaging::ParseQuery>(std::move(payload));
}

TEST(PostgreSQLProtocol, ExtendedQueryMessagesKeepIncompletePayloadsAligned)
{
    expectIncompletePayloadAndAligned<Messaging::DescribeQuery>();
    expectIncompletePayloadAndAligned<Messaging::ExecuteQuery>();
    expectIncompletePayloadAndAligned<Messaging::CloseQuery>();
}

TEST(PostgreSQLProtocol, QueryKeepsIncompletePayloadAligned)
{
    expectIncompletePayloadAndAligned<Messaging::Query>();
}

TEST(PostgreSQLProtocol, LengthDelimitedMessagesRejectTrailingPayload)
{
    std::string query_payload = "SELECT 1";
    query_payload.push_back('\0');
    expectTrailingPayloadIsRejectedAndAligned<Messaging::Query>(std::move(query_payload));

    std::string parse_payload;
    parse_payload.push_back('\0');
    parse_payload += "SELECT 1";
    parse_payload.push_back('\0');
    putInt16(parse_payload, 0);
    expectTrailingPayloadIsRejectedAndAligned<Messaging::ParseQuery>(std::move(parse_payload));

    std::string bind_payload;
    bind_payload.append("\0\0", 2);
    putInt16(bind_payload, 0); /// no parameter format codes
    putInt16(bind_payload, 0); /// no parameters
    putInt16(bind_payload, 0); /// no result format codes
    expectTrailingPayloadIsRejectedAndAligned<Messaging::BindQuery>(std::move(bind_payload));

    std::string describe_payload = "S";
    describe_payload.push_back('\0');
    expectTrailingPayloadIsRejectedAndAligned<Messaging::DescribeQuery>(std::move(describe_payload));

    std::string execute_payload(1, '\0');
    putInt32(execute_payload, 0);
    expectTrailingPayloadIsRejectedAndAligned<Messaging::ExecuteQuery>(std::move(execute_payload));

    std::string close_payload = "P";
    close_payload.push_back('\0');
    expectTrailingPayloadIsRejectedAndAligned<Messaging::CloseQuery>(std::move(close_payload));
}

TEST(PostgreSQLProtocol, SyncRejectsNonEmptyPayload)
{
    for (Int32 size : {0, 1, 2, 3, 5})
    {
        std::string bytes;
        putInt32(bytes, size);
        if (size > 4)
            bytes.append(static_cast<size_t>(size - 4), '\0');

        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::SyncQuery msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }
}

TEST(PostgreSQLProtocol, CopyDoneRejectsNonEmptyPayload)
{
    for (Int32 size : {0, 1, 2, 3, 5})
    {
        std::string bytes;
        putInt32(bytes, size);
        if (size > 4)
            bytes.append(static_cast<size_t>(size - 4), '\0');

        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::CopyDone msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }
}

TEST(PostgreSQLProtocol, BindRejectsBinaryFormatParameters)
{
    /// Build a `Bind` with explicit parameter format codes and one value.
    auto build = [](const std::vector<Int16> & format_codes)
    {
        std::string payload;
        payload.push_back('\0'); /// empty portal name
        payload.push_back('\0'); /// empty statement name
        putInt16(payload, static_cast<Int16>(format_codes.size()));
        for (Int16 code : format_codes)
            putInt16(payload, code);
        putInt16(payload, 1); /// one parameter
        putInt32(payload, 2);
        payload += "hi";
        putInt16(payload, 0); /// no result format codes
        return framePayload(std::move(payload));
    };

    /// Deserialization records binary format only after consuming the message.
    auto deserializeThenAttach = [](const std::string & bytes) -> bool
    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        auto msg = std::make_unique<Messaging::BindQuery>();
        msg->deserialize(in);
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        try
        {
            manager.attachBindQuery(std::move(msg));
            return false;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
            return e.code() == ErrorCodes::NOT_IMPLEMENTED;
        }
    };

    auto deserializeThrows = [](const std::string & bytes, int expected_code)
    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        try
        {
            msg.deserialize(in);
            return false;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), expected_code);
            return e.code() == expected_code;
        }
    };

    /// No format codes: all parameters are text (accepted, flag not set).
    {
        std::string bytes = build({});
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
        EXPECT_FALSE(msg.has_binary_format_param);
    }

    /// Explicit text format code (accepted, flag not set).
    {
        std::string bytes = build({0});
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
        EXPECT_FALSE(msg.has_binary_format_param);
    }

    /// Consume binary codes before `attachBindQuery` rejects them.
    {
        std::string bytes = build({1});
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_TRUE(msg.has_binary_format_param);
    }
    EXPECT_TRUE(deserializeThenAttach(build({1})));

    /// A binary code anywhere in the array is rejected.
    EXPECT_TRUE(deserializeThenAttach(build({0, 1})));

    /// A negative format-code count is malformed and rejected during deserialize.
    {
        std::string payload;
        payload.push_back('\0');
        payload.push_back('\0');
        putInt16(payload, -1); /// negative format-code count
        EXPECT_TRUE(deserializeThrows(framePayload(std::move(payload)), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT));
    }
}

TEST(PostgreSQLProtocol, BindConsumesResultFormatCodesAndKeepsStreamAligned)
{
    /// Build a `Bind` with explicit result formats and a trailing alignment marker.
    auto build = [](const std::vector<Int16> & result_codes)
    {
        std::string payload;
        payload.push_back('\0'); /// empty portal name
        payload.push_back('\0'); /// empty statement name
        putInt16(payload, 0); /// no parameter format codes (all text)
        putInt16(payload, 1); /// one parameter
        putInt32(payload, 2);
        payload += "hi";
        putInt16(payload, static_cast<Int16>(result_codes.size()));
        for (Int16 code : result_codes)
            putInt16(payload, code);
        std::string bytes = framePayload(std::move(payload));
        bytes.push_back('X'); /// trailing marker: must remain unread after deserialize
        return bytes;
    };

    /// All result formats are consumed while output remains text.
    auto deserializeKeepsAligned = [](const std::string & bytes)
    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        char marker = 0;
        in.readStrict(&marker, 1);
        EXPECT_EQ(marker, 'X');
        EXPECT_TRUE(in.eof());
    };

    /// No result format codes (text results).
    deserializeKeepsAligned(build({}));
    /// Explicit text result format code.
    deserializeKeepsAligned(build({0}));
    /// A single binary result format code (accepted, ignored, stream aligned).
    deserializeKeepsAligned(build({1}));
    /// Mixed per-column result format codes (accepted, ignored, stream aligned).
    deserializeKeepsAligned(build({0, 1}));
}

namespace
{

/// Run `Parse` -> `Bind` -> `Execute` and return the assembled SQL.
String bindAndGetStatement(
    const String & body,
    const std::vector<Int32> & oids,
    const std::vector<std::optional<String>> & values)
{
    PreparedStatements::PreparedStatemetsManager manager(std::nullopt);

    ASTPreparedStatement statement;
    statement.function_name = "s";
    statement.function_body = body;
    for (Int32 oid : oids)
        statement.parameter_types.push_back(oid);
    manager.addStatement(&statement);

    auto bind = std::make_unique<Messaging::BindQuery>();
    bind->function_name = "s";
    for (const auto & value : values)
        bind->parameters.push_back(value);
    manager.attachBindQuery(std::move(bind));

    return manager.getStatmentFromBind();
}

}

TEST(PostgreSQLProtocol, BindPreservesDeclaredParameterTypes)
{
    /// Mapped OIDs use `accurateCast` with a quoted value.
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + 1", {23}, {{"41"}}), "SELECT accurateCast('41', 'Int32') + 1");
    EXPECT_EQ(bindAndGetStatement("SELECT * FROM t LIMIT $1", {23}, {{"10"}}),
              "SELECT * FROM t LIMIT accurateCast('10', 'Int32')");

    /// Each integer OID maps to the correspondingly-sized ClickHouse integer.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {21}, {{"32000"}}), "SELECT accurateCast('32000', 'Int16')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {20}, {{"9223372036854775807"}}),
              "SELECT accurateCast('9223372036854775807', 'Int64')");
    /// `oid` maps to an unsigned integer.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {26}, {{"42"}}), "SELECT accurateCast('42', 'UInt32')");

    /// float4/float8 map to Float32/Float64.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {700}, {{"3.14"}}), "SELECT accurateCast('3.14', 'Float32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {701}, {{"-2.5e-3"}}), "SELECT accurateCast('-2.5e-3', 'Float64')");

    /// Preserve mapped non-numeric types.
    EXPECT_EQ(bindAndGetStatement("SELECT NOT $1", {16}, {{"true"}}), "SELECT NOT accurateCast('true', 'Bool')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1082}, {{"2024-01-15"}}),
              "SELECT accurateCast('2024-01-15', 'Date32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1114}, {{"2024-01-15 12:30:45"}}),
              "SELECT accurateCast('2024-01-15 12:30:45', 'DateTime64(6)')");
    /// `timestamptz` preserves its UTC semantics.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1184}, {{"2024-01-15 12:30:45+02"}}),
              "SELECT accurateCast('2024-01-15 12:30:45+02', 'DateTime64(6, \\'UTC\\')')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {2950}, {{"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}}),
              "SELECT accurateCast('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID')");

    /// Unmapped declared OIDs remain quoted strings; OID 0 uses inference.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {25}, {{"hi"}}), "SELECT 'hi'");

    /// A NULL parameter is the SQL keyword NULL regardless of declared type.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {std::nullopt}), "SELECT NULL");
}

TEST(PostgreSQLProtocol, BindTypedNumericIsInjectionSafeInsideCast)
{
    /// Invalid typed values stay quoted inside `accurateCast` and cannot splice SQL.
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {23}, {{"1--"}}),
              "SELECT id = accurateCast('1--', 'Int32')");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {23}, {{"1+2"}}),
              "SELECT id = accurateCast('1+2', 'Int32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {{"1 UNION ALL SELECT secret FROM s"}}),
              "SELECT accurateCast('1 UNION ALL SELECT secret FROM s', 'Int32')");
    /// Quotes cannot close the cast argument.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {{"1'"}}),
              "SELECT accurateCast('1\\'', 'Int32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {2950}, {{"x' OR 1=1--"}}),
              "SELECT accurateCast('x\\' OR 1=1--', 'UUID')");
}

TEST(PostgreSQLProtocol, BindPreservesNumericParameterType)
{
    auto throwsBadArgument = [](Int32 oid, const String & value)
    {
        try
        {
            bindAndGetStatement("SELECT $1", {oid}, {{value}});
            return false;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
            return e.code() == ErrorCodes::BAD_ARGUMENTS;
        }
    };

    /// `numeric` is normalized to an exact `Decimal256`.
    EXPECT_EQ(bindAndGetStatement("SELECT toTypeName($1)", {1700}, {{"2.11"}}),
              "SELECT toTypeName(accurateCast('2.11', 'Decimal256(2)'))");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"-5"}}), "SELECT accurateCast('-5', 'Decimal256(0)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"100"}}), "SELECT accurateCast('100', 'Decimal256(0)')");
    /// Exponent forms are normalized to a plain decimal string with the correct scale.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"1e2"}}), "SELECT accurateCast('100', 'Decimal256(0)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"-2.5e-3"}}), "SELECT accurateCast('-0.0025', 'Decimal256(4)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{".5"}}), "SELECT accurateCast('0.5', 'Decimal256(1)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"5."}}), "SELECT accurateCast('5', 'Decimal256(0)')");
    /// Reject values that exceed `Decimal256` precision.
    EXPECT_TRUE(throwsBadArgument(1700, String(78, '9')));

    /// Reject huge exponents before arithmetic or padding.
    EXPECT_TRUE(throwsBadArgument(1700, "1e1000000"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e-1000000"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e99999999999999999999"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e-99999999999999999999"));

    /// A `numeric` injection payload is not one literal.
    EXPECT_TRUE(throwsBadArgument(1700, "1--"));
    EXPECT_TRUE(throwsBadArgument(1700, "1+2"));
    EXPECT_TRUE(throwsBadArgument(1700, "1-2"));
    EXPECT_TRUE(throwsBadArgument(1700, "1 UNION ALL SELECT secret FROM s"));
    EXPECT_TRUE(throwsBadArgument(1700, "1.2.3"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e"));
    EXPECT_TRUE(throwsBadArgument(1700, "."));
    EXPECT_TRUE(throwsBadArgument(1700, ""));
}

TEST(PostgreSQLProtocol, BindTextParameterStaysQuotedString)
{
    /// Unmapped values remain quoted and escaped.
    auto quotesAsString = [](const String & value, const String & expected)
    {
        EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{value}}), expected);
    };

    quotesAsString("1 UNION ALL SELECT secret FROM s", "SELECT '1 UNION ALL SELECT secret FROM s'");
    quotesAsString("1); DROP TABLE t; --", "SELECT '1); DROP TABLE t; --'");
    quotesAsString("1--", "SELECT '1--'");
    quotesAsString("x' UNION ALL SELECT 1--", "SELECT 'x\\' UNION ALL SELECT 1--'");

    /// `1--` remains inside the cast argument and cannot hide the trailing predicate.
    EXPECT_EQ(
        bindAndGetStatement("SELECT * FROM t WHERE id = $1 AND tenant_id = 42", {23}, {{"1--"}}),
        "SELECT * FROM t WHERE id = accurateCast('1--', 'Int32') AND tenant_id = 42");

    /// The same payload declared as text (OID 0) is safely quoted.
    EXPECT_EQ(
        bindAndGetStatement("SELECT $1", {0}, {{"1 UNION ALL SELECT secret FROM s"}}),
        "SELECT '1 UNION ALL SELECT secret FROM s'");
}

TEST(PostgreSQLProtocol, BindUnspecifiedOidInfersType)
{
    /// OID 0 preserves unambiguous literals for server inference.
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + 1", {0}, {{"41"}}), "SELECT  41  + 1");
    EXPECT_EQ(bindAndGetStatement("SELECT * FROM t LIMIT $1", {0}, {{"10"}}), "SELECT * FROM t LIMIT  10 ");
    /// Missing trailing OIDs infer per slot.
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + $2", {23}, {{"1"}, {"2"}}),
              "SELECT accurateCast('1', 'Int32') +  2 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + $2", {}, {{"1"}, {"2"}}), "SELECT  1  +  2 ");

    /// Preserve supported numeric literal forms.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"-5"}}), "SELECT  -5 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"+5"}}), "SELECT  +5 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"3.14"}}), "SELECT  3.14 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"-2.5e-3"}}), "SELECT  -2.5e-3 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{".5"}}), "SELECT  .5 ");

    /// Exact boolean keywords infer as `Bool`.
    EXPECT_EQ(bindAndGetStatement("SELECT NOT $1", {0}, {{"true"}}), "SELECT NOT  true ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"false"}}), "SELECT  false ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"TRUE"}}), "SELECT  TRUE ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"False"}}), "SELECT  False ");

    /// Other values infer as quoted text.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"hi"}}), "SELECT 'hi'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"2024-01-15"}}), "SELECT '2024-01-15'");
    /// Boolean-like values that are not exact keywords stay text.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"t"}}), "SELECT 't'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"truex"}}), "SELECT 'truex'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"true--"}}), "SELECT 'true--'");
}

TEST(PostgreSQLProtocol, BindUnspecifiedOidIsInjectionSafe)
{
    /// Only one validated numeric literal can remain bare.
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1--"}}), "SELECT id = '1--'");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1+2"}}), "SELECT id = '1+2'");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1-2"}}), "SELECT id = '1-2'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1 UNION ALL SELECT secret FROM s"}}),
              "SELECT '1 UNION ALL SELECT secret FROM s'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1.2.3"}}), "SELECT '1.2.3'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1e"}}), "SELECT '1e'");
    /// A boolean with trailing syntax stays quoted.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"true; DROP TABLE s"}}), "SELECT 'true; DROP TABLE s'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"true OR 1=1"}}), "SELECT 'true OR 1=1'");
    /// Spaces prevent a negative value from forming `--` with adjacent SQL.
    EXPECT_EQ(bindAndGetStatement("SELECT 5-$1", {0}, {{"-5"}}), "SELECT 5- -5 ");
    /// Escape quotes in inferred text.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"x' OR 1=1--"}}), "SELECT 'x\\' OR 1=1--'");
}

TEST(PostgreSQLProtocol, BindSnapshotsStatementForPortalContract)
{
    /// A portal owns the statement snapshot captured by `Bind`.
    auto addStatement = [](PreparedStatements::PreparedStatemetsManager & manager, const String & name, const String & body)
    {
        ASTPreparedStatement statement;
        statement.function_name = name;
        statement.function_body = body;
        manager.addStatement(&statement);
    };
    auto bind = [](PreparedStatements::PreparedStatemetsManager & manager, const String & name)
    {
        auto msg = std::make_unique<Messaging::BindQuery>();
        msg->function_name = name;
        manager.attachBindQuery(std::move(msg));
    };

    /// Redefining the prepared statement after Bind does not affect the portal.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStatement(manager, "s", "SELECT 1");
        bind(manager, "s");
        addStatement(manager, "s", "SELECT 2"); /// Parse s AS SELECT 2
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT 1");
    }

    /// Deallocating the statement does not invalidate its portal.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStatement(manager, "s", "SELECT 1");
        bind(manager, "s");
        manager.tryDeleteStatement("s"); /// Close('S', 's')
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT 1");
    }
}

TEST(PostgreSQLProtocol, ExecuteWithoutBindIsRejected)
{
    /// `Execute` without a portal must not run stale state.
    PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
    ASTPreparedStatement statement;
    statement.function_name = "s";
    statement.function_body = "SELECT 1";
    manager.addStatement(&statement);

    try
    {
        manager.getStatmentFromBind();
        FAIL() << "expected Execute without prior Bind to throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    /// After a valid Bind, resetBindQuery (Sync/portal Close) clears the portal.
    auto msg = std::make_unique<Messaging::BindQuery>();
    msg->function_name = "s";
    manager.attachBindQuery(std::move(msg));
    manager.resetBindQuery();
    EXPECT_THROW(manager.getStatmentFromBind(), Exception);
}

TEST(PostgreSQLProtocol, BindRejectsArityMismatch)
{
    /// `Bind` requires exactly one value per referenced placeholder.
    PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
    ASTPreparedStatement statement;
    statement.function_name = "s";
    statement.function_body = "SELECT $1, $2";
    statement.parameter_types.push_back(23);
    statement.parameter_types.push_back(23);
    manager.addStatement(&statement);

    auto tooFew = std::make_unique<Messaging::BindQuery>();
    tooFew->function_name = "s";
    tooFew->parameters.push_back(String{"1"}); /// only one value for two placeholders
    try
    {
        manager.attachBindQuery(std::move(tooFew));
        FAIL() << "expected arity mismatch to throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }

    /// Exactly matching arity is accepted.
    auto ok = std::make_unique<Messaging::BindQuery>();
    ok->function_name = "s";
    ok->parameters.push_back(String{"1"});
    ok->parameters.push_back(String{"2"});
    EXPECT_NO_THROW(manager.attachBindQuery(std::move(ok)));
    EXPECT_EQ(manager.getStatmentFromBind(), "SELECT accurateCast('1', 'Int32'), accurateCast('2', 'Int32')");
}

TEST(PostgreSQLProtocol, BindArityIsPlaceholderCountNotDeclaredTypeCount)
{
    /// Arity is the highest `$N`, not the number of declared OIDs.
    auto addStmt = [](PreparedStatements::PreparedStatemetsManager & manager, const String & body, std::vector<Int32> oids)
    {
        ASTPreparedStatement statement;
        statement.function_name = "s";
        statement.function_body = body;
        for (Int32 oid : oids)
            statement.parameter_types.push_back(oid);
        manager.addStatement(&statement);
    };
    auto bindN = [](PreparedStatements::PreparedStatemetsManager & manager, size_t n)
    {
        auto msg = std::make_unique<Messaging::BindQuery>();
        msg->function_name = "s";
        for (size_t i = 0; i < n; ++i)
            msg->parameters.push_back(String{std::to_string(i + 1)});
        manager.attachBindQuery(std::move(msg));
    };

    /// Two placeholders, zero declared OIDs: one value must be rejected (the bug),
    /// two values accepted.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1, $2", {});
        try
        {
            bindN(manager, 1);
            FAIL() << "expected one value for two placeholders to throw";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1, $2", {});
        EXPECT_NO_THROW(bindN(manager, 2));
        /// Split the literal to avoid source-format checks on the intentional spaces.
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT  1 " "," "  2 ");
    }

    /// One placeholder: an extra value is rejected (previously silently dropped).
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1", {});
        try
        {
            bindN(manager, 2);
            FAIL() << "expected two values for one placeholder to throw";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    /// A repeated placeholder counts once: `$1 + $1` has arity 1.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1 + $1", {});
        EXPECT_NO_THROW(bindN(manager, 1));
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT  1  +  1 ");
    }

    /// A statement with no placeholders has arity 0: any value is rejected.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT 1", {});
        EXPECT_NO_THROW(bindN(manager, 0));
        try
        {
            addStmt(manager, "SELECT 1", {});
            bindN(manager, 1);
            FAIL() << "expected a value for a zero-parameter statement to throw";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

TEST(PostgreSQLProtocol, ExecuteArityMatchesPlaceholderCount)
{
    /// Simple `PREPARE`/`EXECUTE` enforces the same exact arity as `Bind`.
    auto prepare = [](PreparedStatements::PreparedStatemetsManager & manager, const String & body)
    {
        ASTPreparedStatement statement;
        statement.function_name = "s";
        statement.function_body = body;
        manager.addStatement(&statement);
    };
    auto execute = [](PreparedStatements::PreparedStatemetsManager & manager, std::vector<String> args)
    {
        ASTExecute ast;
        ast.function_name = "s";
        for (auto & a : args)
            ast.arguments.push_back(std::move(a));
        return manager.getStatement(&ast);
    };
    auto expectRejected = [&](const String & body, std::vector<String> args)
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        prepare(manager, body);
        try
        {
            execute(manager, std::move(args));
            FAIL() << "expected arity mismatch to throw for body: " << body;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        }
    };

    /// Over-supply: extra argument previously silently dropped.
    expectRejected("SELECT $1", {"1", "2"});
    /// Under-supply: `$2` previously left in the executed SQL.
    expectRejected("SELECT $1, $2", {"1"});
    /// Zero-placeholder statement rejects any argument.
    expectRejected("SELECT 1", {"1"});

    /// Exact arity is accepted and substituted (a repeated placeholder counts once).
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        prepare(manager, "SELECT $1 + $2");
        EXPECT_EQ(execute(manager, {"1", "2"}), "SELECT  1  +  2 ");
    }
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        prepare(manager, "SELECT $1 + $1");
        EXPECT_EQ(execute(manager, {"7"}), "SELECT  7  +  7 ");
    }
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        prepare(manager, "SELECT 1");
        EXPECT_EQ(execute(manager, {}), "SELECT 1");
    }
}

TEST(PostgreSQLProtocol, ExecuteArgumentStaysASeparateToken)
{
    /// A bare argument next to an operator must not merge with it: `--` would start a comment
    /// and drop the rest of the statement.
    PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
    ASTPreparedStatement statement;
    statement.function_name = "s";
    statement.function_body = "SELECT 5-$1 AS v, 'tail' AS t";
    manager.addStatement(&statement);

    auto execute = [&](const String & argument)
    {
        ASTExecute ast;
        ast.function_name = "s";
        ast.arguments.push_back(argument);
        return manager.getStatement(&ast);
    };

    EXPECT_EQ(execute("-1"), "SELECT 5- -1  AS v, 'tail' AS t");
    /// Control: an argument that cannot merge is substituted the same way.
    EXPECT_EQ(execute("1"), "SELECT 5- 1  AS v, 'tail' AS t");
}

TEST(PostgreSQLProtocol, ExecuteRejectsNonLiteralArguments)
{
    /// Reject expressions because substitution can change precedence or evaluate them twice.
    auto parse_args = [](const String & query)
    {
        ParserExecute parser;
        ASTPtr ast = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        return ast->as<ASTExecute>()->arguments;
    };

    /// Reject non-literal expressions cleanly.
    EXPECT_THROW(parse_args("EXECUTE s(1 + 1)"), Exception);
    EXPECT_THROW(parse_args("EXECUTE s(now())"), Exception);
    EXPECT_THROW(parse_args("EXECUTE s(rand())"), Exception);
    EXPECT_THROW(parse_args("EXECUTE s(concat('a', 'b'))"), Exception);
    /// SQL text inside a string remains one literal.
    EXPECT_THROW(parse_args("EXECUTE s(1 UNION ALL SELECT secret)"), Exception);

    /// A negative number already parses as a single literal (not an expression),
    /// so negative arguments are still accepted.
    {
        auto args = parse_args("EXECUTE s(-1)");
        ASSERT_EQ(args.size(), 1u);
        EXPECT_EQ(args[0], "-1");
    }
    {
        auto args = parse_args("EXECUTE s(-1.5)");
        ASSERT_EQ(args.size(), 1u);
        EXPECT_EQ(args[0], "-1.5");
    }
    /// Plain literals keep their existing formatting: numbers bare, strings quoted.
    {
        auto args = parse_args("EXECUTE s(42, 'abc')");
        ASSERT_EQ(args.size(), 2u);
        EXPECT_EQ(args[0], "42");
        EXPECT_EQ(args[1], "'abc'");
    }
    /// Injection stays impossible: a string argument holding SQL syntax is a single
    /// quoted, escaped literal, never raw SQL.
    {
        auto args = parse_args("EXECUTE s('1 UNION ALL SELECT secret -- ')");
        ASSERT_EQ(args.size(), 1u);
        EXPECT_EQ(args[0], "'1 UNION ALL SELECT secret -- '");
    }
}

TEST(PostgreSQLProtocol, ExecuteZeroArityReachableThroughGrammar)
{
    /// Both PostgreSQL forms execute a zero-parameter statement.
    auto parse = [](const String & query)
    {
        ParserExecute parser;
        return parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    };

    {
        ASTPtr ast = parse("EXECUTE s");
        const auto * execute = ast->as<ASTExecute>();
        ASSERT_TRUE(execute);
        EXPECT_EQ(execute->function_name, "s");
        EXPECT_EQ(execute->arguments.size(), 0u);
    }
    {
        ASTPtr ast = parse("EXECUTE s()");
        const auto * execute = ast->as<ASTExecute>();
        ASSERT_TRUE(execute);
        EXPECT_EQ(execute->function_name, "s");
        EXPECT_EQ(execute->arguments.size(), 0u);
    }

    /// Substitution leaves a zero-parameter body unchanged.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        ASTPreparedStatement prepared;
        prepared.function_name = "s";
        prepared.function_body = "SELECT 1";
        manager.addStatement(&prepared);

        ParserExecute parser;
        ASTPtr ast = parseQuery(parser, "EXECUTE s", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        EXPECT_EQ(manager.getStatement(ast->as<ASTExecute>()), "SELECT 1");
    }
}

TEST(PostgreSQLProtocol, InferredUnspecifiedOidFractionalIntCastIsRejectedNotTruncated)
{
    /// OID 0 emits an unambiguous numeric value as a bare, space-padded literal.
    auto addStmt = [](PreparedStatements::PreparedStatemetsManager & manager, const String & body, std::vector<Int32> oids)
    {
        ASTPreparedStatement statement;
        statement.function_name = "s";
        statement.function_body = body;
        for (Int32 oid : oids)
            statement.parameter_types.push_back(oid);
        manager.addStatement(&statement);
    };
    auto bindValue = [](PreparedStatements::PreparedStatemetsManager & manager, const String & value)
    {
        auto msg = std::make_unique<Messaging::BindQuery>();
        msg->function_name = "s";
        msg->parameters.push_back(value);
        manager.attachBindQuery(std::move(msg));
    };

    /// The statement applies its own cast to the inferred numeric literal.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1::Int32", {});
        bindValue(manager, "3.14");
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT  3.14 ::Int32");
    }
    /// An integer value against the same statement stays an integer literal.
    {
        PreparedStatements::PreparedStatemetsManager manager(std::nullopt);
        addStmt(manager, "SELECT $1::Int32", {});
        bindValue(manager, "42");
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT  42 ::Int32");
    }
}

TEST(PostgreSQLProtocol, CopyDataRejectsLengthBelowFour)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::CopyInData msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }

    /// A well-formed length carries `size - 4` payload bytes.
    std::string bytes;
    putInt32(bytes, 6);
    bytes += "ab";
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    Messaging::CopyInData msg;
    EXPECT_NO_THROW(msg.deserialize(in));
    EXPECT_EQ(msg.query, "ab");
}

TEST(PostgreSQLProtocol, CommandCompletePreservesUInt64RowCount)
{
    WriteBufferFromOwnString out;
    Messaging::CommandComplete response(Messaging::CommandComplete::SELECT, std::numeric_limits<UInt64>::max());
    response.serialize(out);
    out.finalize();

    std::string expected;
    expected.push_back('C');
    putInt32(expected, 32);
    expected += "SELECT 18446744073709551615";
    expected.push_back('\0');

    EXPECT_EQ(out.str(), expected);
}
