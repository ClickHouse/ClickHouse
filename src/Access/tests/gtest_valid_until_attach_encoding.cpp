#include <gtest/gtest.h>

#include <Access/AuthenticationData.h>
#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTAuthenticationData.h>

#include <vector>

using namespace DB;

namespace
{

/// The string literal that `AuthenticationData::toAST` puts into the stored (`ATTACH USER`) form.
String serializedValidUntil(time_t valid_until)
{
    AuthenticationData auth_data;
    auth_data.setValidUntil(valid_until);
    auto ast = auth_data.toAST(/* attach_mode= */ true);
    return ast->valid_until->as<ASTLiteral &>().value.safeGet<String>();
}

time_t roundTrip(time_t valid_until)
{
    AuthenticationData auth_data;
    auth_data.setValidUntil(valid_until);
    auto ast = auth_data.toAST(/* attach_mode= */ true);
    /// No context is exactly the deserialization path used for stored access entities.
    return getValidUntilFromAST(ast->valid_until, /* context= */ nullptr, /* is_interval= */ false);
}

}

/// Stored access entities must round-trip `valid_until` across restarts (disk access storage) and
/// between servers (replicated access storage), regardless of the server time zone.

TEST(ValidUntilAttachEncoding, PostEpochDeadlinesRoundTripExactly)
{
    const std::vector<time_t> deadlines =
    {
        1, /// the smallest expired deadline (0 means "no expiration" and is not serialized)
        5,
        9999, /// the largest value that would be rejected as ambiguous without zero-padding
        10000,
        1234567890,
        4294967295, /// the `DateTime` upper bound (year 2106)
        10413792000, /// year 2300, beyond the former `DateTime64` upper bound
        253402300799, /// 9999-12-31 23:59:59 UTC, the `DateTime64` upper bound
    };
    for (time_t deadline : deadlines)
        EXPECT_EQ(roundTrip(deadline), deadline);
}

TEST(ValidUntilAttachEncoding, Pre1970DeadlinesRoundTripDownToYear1900)
{
    const std::vector<time_t> deadlines =
    {
        -1,
        -12345,
        -2208988800, /// 1900-01-01 00:00:00 UTC
    };
    for (time_t deadline : deadlines)
        EXPECT_EQ(roundTrip(deadline), deadline);

    /// Deadlines before 1900 cannot be represented in a form that every supported reader of the
    /// stored format parses. All deadlines in the past are equivalent (the credential is expired),
    /// so they are clamped to the 1900 floor.
    EXPECT_EQ(roundTrip(-62135596800 /* 0001-01-01 00:00:00 UTC */), -2208988800);
}

TEST(ValidUntilAttachEncoding, StoredFormIsParsedByEverySupportedReader)
{
    /// A post-epoch deadline is stored as an all-digit Unix timestamp of at least 5 digits:
    /// shorter numeric strings are rejected as ambiguous by `readDateTimeText`, which older
    /// versions (and the current no-context reader) use for this form.
    for (time_t deadline : {static_cast<time_t>(1), static_cast<time_t>(9999), static_cast<time_t>(253402300799)})
    {
        const String stored = serializedValidUntil(deadline);
        EXPECT_GE(stored.size(), 5u) << stored;
        EXPECT_EQ(stored.find_first_not_of("0123456789"), String::npos) << stored;
    }
    EXPECT_EQ(serializedValidUntil(1), "0000000001");

    /// A pre-1970 deadline is stored in the datetime form older versions have always written and
    /// read themselves (a leading '-' would fail to parse there), with an explicit `UTC` suffix
    /// that pins the instant for current versions.
    EXPECT_EQ(serializedValidUntil(-1), "1969-12-31 23:59:59 UTC");
    EXPECT_EQ(serializedValidUntil(-2208988800), "1900-01-01 00:00:00 UTC");
    EXPECT_EQ(serializedValidUntil(-62135596800), "1900-01-01 00:00:00 UTC");
}
