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

TEST(PostgreSQLProtocol, BindRejectsNegativeCounts)
{
    /// A malformed Bind carrying a negative parameter count must be rejected
    /// rather than silently skipping the params loop: a negative num_params would
    /// leave the parameter payload unread and the next bytes would be misread as
    /// the result-format-code count, desynchronizing the skip-until-Sync recovery.
    {
        std::string bytes;
        putInt32(bytes, 0); /// outer size field is unused for bounds here
        bytes.push_back('\0'); /// empty portal name
        bytes.push_back('\0'); /// empty statement name
        putInt16(bytes, 0); /// no parameter format codes
        putInt16(bytes, -1); /// negative parameter count
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::BindQuery msg;
            msg.deserialize(in);
        }));
    }

    /// A negative result-format-code count is malformed for the same reason and
    /// must also be rejected.
    {
        std::string bytes;
        putInt32(bytes, 0);
        bytes.push_back('\0');
        bytes.push_back('\0');
        putInt16(bytes, 0); /// no parameter format codes
        putInt16(bytes, 0); /// no parameters
        putInt16(bytes, -1); /// negative result-format-code count
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::BindQuery msg;
            msg.deserialize(in);
        }));
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

TEST(PostgreSQLProtocol, BindConsumesResultFormatCodesAndKeepsStreamAligned)
{
    /// Build a Bind with a single text parameter "hi", explicit result column
    /// format codes, and a trailing marker byte. `result_codes` become the
    /// requested result-format-code array (0 = text, 1 = binary). We always emit
    /// text rows and RowDescription advertises text for every column, so a binary
    /// result request is accepted and ignored (real clients such as Npgsql request
    /// binary by default). The only requirement is that deserialize consumes the
    /// codes exactly and leaves the stream aligned on the next message boundary.
    auto build = [](const std::vector<Int16> & result_codes)
    {
        std::string bytes;
        putInt32(bytes, 0); /// outer size field, unused for bounds here
        bytes.push_back('\0'); /// empty portal name
        bytes.push_back('\0'); /// empty statement name
        putInt16(bytes, 0); /// no parameter format codes (all text)
        putInt16(bytes, 1); /// one parameter
        putInt32(bytes, 2);
        bytes += "hi";
        putInt16(bytes, static_cast<Int16>(result_codes.size()));
        for (Int16 code : result_codes)
            putInt16(bytes, code);
        bytes.push_back('X'); /// trailing marker: must remain unread after deserialize
        return bytes;
    };

    /// deserialize must succeed for any result format request and leave exactly the
    /// trailing marker byte in the buffer (stream aligned for the next message).
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

    /// A declared-but-unmapped OID (25 = text) stays a quoted+escaped string literal.
    /// OID 0 (unspecified) is handled by the inference path, tested separately below.
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

TEST(PostgreSQLProtocol, BindUnspecifiedOidInfersType)
{
    /// An OID of 0 (or an omitted trailing OID) means "the server infers the parameter
    /// type from the statement" in the PostgreSQL protocol, NOT "text". A numeric value
    /// is emitted as a bare, space-padded numeric literal so `Parse("SELECT $1 + 1")`,
    /// `Parse("... LIMIT $1")` and `Parse("SELECT $1::Int32")` keep working as numbers
    /// instead of regressing to `'41' + 1` (type error) / `LIMIT '1'` (rejected) /
    /// `CAST('(42)', 'Int32')` (parse error).
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + 1", {0}, {{"41"}}), "SELECT  41  + 1");
    EXPECT_EQ(bindAndGetStatement("SELECT * FROM t LIMIT $1", {0}, {{"10"}}), "SELECT * FROM t LIMIT  10 ");
    /// An omitted trailing OID (Parse declared fewer OIDs than placeholders) infers
    /// per-slot: the slot with a declared OID keeps its accurateCast, the slot with
    /// the omitted OID infers.
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + $2", {23}, {{"1"}, {"2"}}),
              "SELECT accurateCast('1', 'Int32') +  2 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1 + $2", {}, {{"1"}, {"2"}}), "SELECT  1  +  2 ");

    /// Every numeric literal form the value's own text carries is preserved verbatim,
    /// space-padded so ClickHouse infers the numeric type exactly as an inline literal
    /// would (signed, decimal, exponent).
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"-5"}}), "SELECT  -5 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"+5"}}), "SELECT  +5 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"3.14"}}), "SELECT  3.14 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"-2.5e-3"}}), "SELECT  -2.5e-3 ");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{".5"}}), "SELECT  .5 ");

    /// A non-numeric value's only safe inference is text, so it stays a
    /// quoted+escaped string literal.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"hi"}}), "SELECT 'hi'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"2024-01-15"}}), "SELECT '2024-01-15'");
}

TEST(PostgreSQLProtocol, BindUnspecifiedOidIsInjectionSafe)
{
    /// The inference path emits a bare numeric literal only when the value passes
    /// isSingleNumericLiteral, which accepts exactly one optionally-signed
    /// decimal/exponent literal. Any injection payload fails that check and falls
    /// through to a quoted+escaped string literal, so it can never splice SQL.
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1--"}}), "SELECT id = '1--'");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1+2"}}), "SELECT id = '1+2'");
    EXPECT_EQ(bindAndGetStatement("SELECT id = $1", {0}, {{"1-2"}}), "SELECT id = '1-2'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1 UNION ALL SELECT secret FROM s"}}),
              "SELECT '1 UNION ALL SELECT secret FROM s'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1.2.3"}}), "SELECT '1.2.3'");
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"1e"}}), "SELECT '1e'");
    /// The surrounding spaces additionally block token-adjacency: a body `5-$1` with
    /// value `-5` becomes `5- -5 ` (= 5 - (-5)), never the comment-truncating `5--5`.
    EXPECT_EQ(bindAndGetStatement("SELECT 5-$1", {0}, {{"-5"}}), "SELECT 5- -5 ");
    /// A single quote in a non-numeric value is backslash-escaped so it cannot close
    /// the string literal.
    EXPECT_EQ(bindAndGetStatement("SELECT $1", {0}, {{"x' OR 1=1--"}}), "SELECT 'x\\' OR 1=1--'");
}

TEST(PostgreSQLProtocol, BindSnapshotsStatementForPortalContract)
{
    /// Per the extended-query protocol, once `Bind` creates the portal it owns a
    /// snapshot of the referenced prepared statement. A later `Parse` that
    /// redefines the statement, or a `Close` that deallocates it, must not change
    /// what the already-bound `Execute` runs. Before this fix `Execute`
    /// re-resolved the statement from the live map, so redefinition/close leaked
    /// into the bound portal.
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

    /// Deallocating the prepared statement after Bind does not invalidate the
    /// portal — Execute still runs the bound statement, no "Execute without prior
    /// Bind".
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
    /// Execute with no prior Bind (and after a Sync/Close reset) must fail cleanly
    /// rather than run stale state.
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
    /// Bind must supply exactly one value per placeholder the statement references.
    /// Fewer values leaves a `$N` in the SQL at Execute; more values are silently
    /// dropped by substitute. Both are rejected.
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
    /// The arity Bind must match is the statement's true parameter count — the
    /// highest `$N` in the body — NOT the number of parameter type OIDs Parse
    /// chose to declare. PostgreSQL lets Parse send zero or fewer OIDs than there
    /// are placeholders (the rest are inferred). Checking against the declared-type
    /// count let `Parse "SELECT $1, $2"` (no OIDs) + one-value Bind through,
    /// leaving `$2` in the SQL, and silently dropped extra values.
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
        /// Both placeholders are substituted (arity satisfied). With no declared OID
        /// the numeric values infer as bare, space-padded numeric literals.
        EXPECT_EQ(manager.getStatmentFromBind(), "SELECT  1 ,  2 ");
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
