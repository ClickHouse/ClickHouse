#include <gtest/gtest.h>

#include <Core/PostgreSQLProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <optional>
#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
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
        std::string bytes;
        putInt32(bytes, 0); /// the outer size field is not used for bounds here
        bytes.push_back('\0'); /// empty portal name
        bytes.push_back('\0'); /// empty statement name
        putInt16(bytes, 0); /// no parameter format codes
        putInt16(bytes, 1); /// one parameter
        putInt32(bytes, sz_param);
        bytes += data;
        putInt16(bytes, 0); /// no result format codes
        return bytes;
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](ReadBuffer & in)
    {
        Messaging::BindQuery msg;
        msg.deserialize(in);
    }));

    /// -1 is the protocol sentinel for a NULL parameter; no value bytes follow.
    /// It maps to a disengaged optional, distinct from the literal text "NULL".
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

TEST(PostgreSQLProtocol, BindRejectsBinaryFormatParameters)
{
    /// Build a Bind message whose parameter format codes are given explicitly.
    /// `format_codes` become the per-parameter format-code array (0 = text,
    /// 1 = binary); a single text parameter "hi" follows.
    auto build = [](const std::vector<Int16> & format_codes)
    {
        std::string bytes;
        putInt32(bytes, 0); /// outer size field, unused for bounds here
        bytes.push_back('\0'); /// empty portal name
        bytes.push_back('\0'); /// empty statement name
        putInt16(bytes, static_cast<Int16>(format_codes.size()));
        for (Int16 code : format_codes)
            putInt16(bytes, code);
        putInt16(bytes, 1); /// one parameter
        putInt32(bytes, 2);
        bytes += "hi";
        putInt16(bytes, 0); /// no result format codes
        return bytes;
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

    /// No format codes: all parameters are text (accepted).
    {
        std::string bytes = build({});
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
    }

    /// Explicit text format code (accepted).
    {
        std::string bytes = build({0});
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
    }

    /// Binary format code must be rejected rather than silently misbound.
    EXPECT_TRUE(deserializeThrows(build({1}), ErrorCodes::NOT_IMPLEMENTED));

    /// A binary code anywhere in the array is rejected.
    EXPECT_TRUE(deserializeThrows(build({0, 1}), ErrorCodes::NOT_IMPLEMENTED));

    /// A negative format-code count is malformed.
    {
        std::string bytes;
        putInt32(bytes, 0);
        bytes.push_back('\0');
        bytes.push_back('\0');
        putInt16(bytes, -1); /// negative format-code count
        EXPECT_TRUE(deserializeThrows(bytes, ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT));
    }
}

namespace
{

/// Drive Parse -> Bind -> Execute through the PreparedStatemetsManager and return
/// the SQL body that gets executed. `oids` are the declared parameter type OIDs
/// from the Parse message; `values` are the text-format Bind values (nullopt is a
/// SQL NULL). This mirrors what PostgreSQLHandler does for the extended protocol.
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

TEST(PostgreSQLProtocol, BindPreservesNumericParameterTypes)
{
    /// A parameter declared with a numeric OID (int4 = 23) is emitted as a bare
    /// numeric literal, so `SELECT $1 + 1` and `LIMIT $1` keep working instead of
    /// being coerced to a String literal.
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + 1", {23}, {{"41"}}), "SELECT 41 + 1");
    EXPECT_EQ(bindAndGetStatement("SELECT * FROM t LIMIT $1", {23}, {{"10"}}), "SELECT * FROM t LIMIT 10");

    /// int8, float8 and numeric OIDs behave the same.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {20}, {{"9223372036854775807"}}), "SELECT 9223372036854775807");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {701}, {{"3.14"}}), "SELECT 3.14");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {701}, {{"-2.5e-3"}}), "SELECT -2.5e-3");

    /// A parameter with no declared type (OID 0) or a text OID (25 = text) stays a
    /// quoted+escaped string literal.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"41"}}), "SELECT '41'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {25}, {{"hi"}}), "SELECT 'hi'");

    /// A NULL parameter is the SQL keyword NULL regardless of declared type.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {std::nullopt}), "SELECT NULL");
}

TEST(PostgreSQLProtocol, BindRejectsInjectionInNumericParameter)
{
    /// Emitting a numeric parameter unquoted is only safe because the value is
    /// validated to contain numeric characters only. An injection payload
    /// declared as a numeric type is rejected, never spliced into the body.
    auto throwsBadArgument = [](const String & value)
    {
        try
        {
            bindAndGetStatement("SELECT $1", {23}, {{value}});
            return false;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
            return e.code() == ErrorCodes::BAD_ARGUMENTS;
        }
    };

    EXPECT_TRUE(throwsBadArgument("1 UNION ALL SELECT secret FROM s"));
    EXPECT_TRUE(throwsBadArgument("1); DROP TABLE t; --"));
    EXPECT_TRUE(throwsBadArgument("1'"));
    EXPECT_TRUE(throwsBadArgument(""));
    EXPECT_TRUE(throwsBadArgument("0x10"));

    /// The same payload declared as text (OID 0) is safely quoted, not rejected.
    EXPECT_EQ(
        bindAndGetStatement("SELECT $1", {0}, {{"1 UNION ALL SELECT secret FROM s"}}),
        "SELECT '1 UNION ALL SELECT secret FROM s'");
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
