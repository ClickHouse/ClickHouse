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

    /// deserialize fully consumes the message and only records a binary format
    /// code in `has_binary_format_param` (so the byte stream stays aligned for the
    /// error-recovery skip-until-Sync path); attachBindQuery is what rejects it.
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

    /// A binary format code is consumed by deserialize (which records the flag but
    /// does not throw, keeping the stream aligned) and rejected by attachBindQuery.
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

TEST(PostgreSQLProtocol, BindPreservesDeclaredParameterTypes)
{
    /// A parameter declared with a mapped OID is emitted as an accurateCast to the
    /// matching ClickHouse type, so `SELECT $1 + 1` and `LIMIT $1` keep working and
    /// the declared type is preserved instead of being coerced to a String literal.
    /// The value is always quoted+escaped inside the cast, so nothing can break out.
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

    /// Non-numeric typed OIDs are preserved too (previously they were silently
    /// downgraded to a String literal, regressing standards-compliant typed binds).
    EXPECT_EQ(bindAndGetStatement("SELECT NOT $1", {16}, {{"true"}}), "SELECT NOT accurateCast('true', 'Bool')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1082}, {{"2024-01-15"}}),
              "SELECT accurateCast('2024-01-15', 'Date32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1114}, {{"2024-01-15 12:30:45"}}),
              "SELECT accurateCast('2024-01-15 12:30:45', 'DateTime64(6)')");
    /// timestamptz (OID 1184) carries its timezone in the type: it maps to
    /// DateTime64(6, 'UTC'), not bare DateTime64(6), so toTypeName reports the
    /// right type and offset-bearing values are interpreted as UTC, not local
    /// wall-clock. The type name's inner quotes are escaped by quoteString.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1184}, {{"2024-01-15 12:30:45+02"}}),
              "SELECT accurateCast('2024-01-15 12:30:45+02', 'DateTime64(6, \\'UTC\\')')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {2950}, {{"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}}),
              "SELECT accurateCast('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID')");

    /// An OID with no safe mapping (0 = none, 25 = text) stays a quoted+escaped
    /// string literal.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"41"}}), "SELECT '41'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {25}, {{"hi"}}), "SELECT 'hi'");

    /// A NULL parameter is the SQL keyword NULL regardless of declared type.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {std::nullopt}), "SELECT NULL");
}

TEST(PostgreSQLProtocol, BindTypedNumericIsInjectionSafeInsideCast)
{
    /// The mapped-OID path emits accurateCast('<escaped value>', '<type>'). Range,
    /// width and type validation for these OIDs is delegated to ClickHouse's parser
    /// at execution time (accurateCast rejects out-of-range or malformed input for
    /// the declared type); the integration test asserts that rejection. Here we
    /// assert the assembled fragment is injection-safe: an injection payload stays
    /// quoted+escaped inside the cast argument and can never splice SQL, even though
    /// it is not a valid value for the declared type.
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {23}, {{"1--"}}),
              "SELECT id = accurateCast('1--', 'Int32')");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {23}, {{"1+2"}}),
              "SELECT id = accurateCast('1+2', 'Int32')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {23}, {{"1 UNION ALL SELECT secret FROM s"}}),
              "SELECT accurateCast('1 UNION ALL SELECT secret FROM s', 'Int32')");
    /// A single quote in the value is backslash-escaped so it cannot close the cast
    /// argument's string literal.
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

    /// `numeric` (OID 1700) has no Decimal literal form in ClickHouse SQL, so a bare
    /// `2.11` would be reparsed as Float64 and lose precision. It is validated and
    /// re-serialized as an exact Decimal via accurateCast, preserving value and type.
    EXPECT_EQ(bindAndGetStatement("SELECT toTypeName($1)", {1700}, {{"2.11"}}),
              "SELECT toTypeName(accurateCast('2.11', 'Decimal256(2)'))");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"-5"}}), "SELECT accurateCast('-5', 'Decimal256(0)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"100"}}), "SELECT accurateCast('100', 'Decimal256(0)')");
    /// Exponent forms are normalized to a plain decimal string with the correct scale.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"1e2"}}), "SELECT accurateCast('100', 'Decimal256(0)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"-2.5e-3"}}), "SELECT accurateCast('-0.0025', 'Decimal256(4)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{".5"}}), "SELECT accurateCast('0.5', 'Decimal256(1)')");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {1700}, {{"5."}}), "SELECT accurateCast('5', 'Decimal256(0)')");
    /// A value needing more significant digits than Decimal256 can hold is rejected,
    /// not silently rounded.
    EXPECT_TRUE(throwsBadArgument(1700, String(78, '9')));

    /// An oversize exponent is rejected up front, before the normalizer does any
    /// exponent arithmetic or zero-padding. This keeps the huge-exponent path cheap
    /// and safe: `1e1000000` / `1e-1000000` can never fit Decimal256, so there is no
    /// O(exponent) zero-padding blow-up, and an exponent large enough to overflow a
    /// signed Int64 (`1e99999999999999999999`) can never be folded in (no UB).
    EXPECT_TRUE(throwsBadArgument(1700, "1e1000000"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e-1000000"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e99999999999999999999"));
    EXPECT_TRUE(throwsBadArgument(1700, "1e-99999999999999999999"));

    /// The numeric branch validates the value as one literal at assembly time, so an
    /// injection payload declared `numeric` is rejected, never spliced or cast.
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
    /// A parameter with no declared type or an unmapped OID is always a
    /// quoted+escaped string literal, so an injection payload can never break out.
    auto quotesAsString = [](const String & value, const String & expected)
    {
        EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{value}}), expected);
    };

    quotesAsString("1 UNION ALL SELECT secret FROM s", "SELECT '1 UNION ALL SELECT secret FROM s'");
    quotesAsString("1); DROP TABLE t; --", "SELECT '1); DROP TABLE t; --'");
    quotesAsString("1--", "SELECT '1--'");
    quotesAsString("x' UNION ALL SELECT 1--", "SELECT 'x\\' UNION ALL SELECT 1--'");

    /// The `1--` payload proves injection containment concretely: with a per-character
    /// numeric check it would splice in as `id = 1-- AND ...`, dropping the trailing
    /// predicate. Because the value is emitted inside a quoted cast argument, the
    /// trailing predicate is preserved verbatim and the `--` cannot start a comment:
    /// the assembled query is `id = accurateCast('1--', 'Int32') AND tenant_id = 42`
    /// (which the parser then rejects at execution time, so no rows leak).
    EXPECT_EQ(
        bindAndGetStatement("SELECT * FROM t WHERE id = $1 AND tenant_id = 42", {23}, {{"1--"}}),
        "SELECT * FROM t WHERE id = accurateCast('1--', 'Int32') AND tenant_id = 42");

    /// The same payload declared as text (OID 0) is safely quoted.
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
