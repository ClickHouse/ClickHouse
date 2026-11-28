#include <base/EnumReflection.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/HTTPResponse.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

namespace
{
enum class Code : int
{
    Known = 1,
    TransportFailure = -195,
};

enum class WideCode : uint64_t
{
    Known = 1,
};

enum class ByteCode : char
{
    Known = 1,
};

void expectText(is_enum auto value, std::string_view expected)
{
    EXPECT_EQ(fmt::format("{}", value), expected);

    DB::WriteBufferFromOwnString buf;
    DB::writeText(value, buf);
    EXPECT_EQ(buf.str(), expected);
}

TEST(EnumReflection, NameOrDecimalValue)
{
    expectText(Code::Known, "Known");
    expectText(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "400");
    expectText(Code::TransportFailure, "-195");
    expectText(static_cast<Code>(2), "2");
    expectText(static_cast<WideCode>(std::numeric_limits<uint64_t>::max()), "18446744073709551615");
    expectText(static_cast<ByteCode>(65), "65");
}

TEST(EnumReflection, StringAlignment)
{
    EXPECT_EQ(fmt::format("{:>6}", Code::Known), " Known");
    EXPECT_EQ(fmt::format("{:>6}", Poco::Net::HTTPResponse::HTTP_BAD_REQUEST), "   400");
}
}
