#include <gtest/gtest.h>

#include <IO/ReadHelpers.h>
#include <base/UUID.h>

#include <span>
#include <string_view>

using namespace DB;

namespace
{

std::span<const UInt8> asSpan(std::string_view s)
{
    return {reinterpret_cast<const UInt8 *>(s.data()), s.size()};
}

}

TEST(ParseUUID, Valid36)
{
    UUID uuid;
    ASSERT_TRUE(tryParseUUID(asSpan("550e8400-e29b-41d4-a716-446655440000"), uuid));
    EXPECT_EQ(uuid, parseUUID(asSpan("550e8400-e29b-41d4-a716-446655440000")));
}

TEST(ParseUUID, Valid32)
{
    UUID uuid;
    ASSERT_TRUE(tryParseUUID(asSpan("550e8400e29b41d4a716446655440000"), uuid));
    EXPECT_EQ(uuid, parseUUID(asSpan("550e8400-e29b-41d4-a716-446655440000")));
}

/// tryParseUUID / tryReadUUIDText must leave the output object untouched when parsing fails, so
/// callers that keep a sentinel/default in the output slot are not corrupted by a rejected input.
TEST(ParseUUID, OutputUnchangedOnFailure)
{
    const UUID sentinel = parseUUID(asSpan("11111111-1111-1111-1111-111111111111"));

    /// Invalid hex digit in a 36-char string (correct dashes, bad content).
    {
        UUID uuid = sentinel;
        ASSERT_FALSE(tryParseUUID(asSpan("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"), uuid));
        EXPECT_EQ(uuid, sentinel);
    }

    /// Invalid hex digit only in the trailing group of a 36-char string (prefix groups are valid).
    {
        UUID uuid = sentinel;
        ASSERT_FALSE(tryParseUUID(asSpan("550e8400-e29b-41d4-a716-4466554400zz"), uuid));
        EXPECT_EQ(uuid, sentinel);
    }

    /// Correct hex but wrong dash positions in a 36-char string.
    {
        UUID uuid = sentinel;
        ASSERT_FALSE(tryParseUUID(asSpan("550e8400e-29b-41d4-a716-44665544000"), uuid));
        EXPECT_EQ(uuid, sentinel);
    }

    /// Invalid hex digit in a 32-char string.
    {
        UUID uuid = sentinel;
        ASSERT_FALSE(tryParseUUID(asSpan("550e8400e29b41d4a71644665544zzzz"), uuid));
        EXPECT_EQ(uuid, sentinel);
    }

    /// Unexpected length.
    {
        UUID uuid = sentinel;
        ASSERT_FALSE(tryParseUUID(asSpan("550e8400"), uuid));
        EXPECT_EQ(uuid, sentinel);
    }
}
