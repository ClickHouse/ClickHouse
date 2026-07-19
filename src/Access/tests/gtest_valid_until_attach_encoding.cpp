#include <gtest/gtest.h>

#include <Access/AccessEntityIO.h>
#include <Access/AuthenticationData.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
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

TEST(ValidUntilAttachEncoding, Pre1970DeadlinesAreStoredFailClosedAsExpired)
{
    /// A pre-1970 (negative) deadline cannot be stored in a form that every supported reader interprets
    /// fail-closed: an older or downgraded server resolves a pre-1970 datetime to `0`, which is the
    /// "no expiration" sentinel, turning an already-expired credential into a non-expiring one. Every
    /// deadline in the past is equivalent (the credential is expired), so such a deadline is normalized
    /// to the smallest expired instant (`1`) before serialization - the same way the `VALID FOR` path
    /// clamps a pre-epoch deadline - and stored as a plain Unix timestamp that reads back as `1` (expired)
    /// on every reader.
    const std::vector<time_t> deadlines =
    {
        -1,
        -12345,
        -2208988800, /// 1900-01-01 00:00:00 UTC
        -62135596800, /// 0001-01-01 00:00:00 UTC
    };
    for (time_t deadline : deadlines)
        EXPECT_EQ(roundTrip(deadline), 1) << deadline;
}

TEST(ValidUntilAttachEncoding, StoredFormIsParsedByEverySupportedReader)
{
    /// Every deadline is stored as an all-digit Unix timestamp of at least 5 digits: shorter numeric
    /// strings are rejected as ambiguous by `readDateTimeText`, which older versions (and the current
    /// no-context reader) use for this form. A Unix timestamp denotes the same instant regardless of the
    /// time zone and is read the same way by every supported version, unlike a datetime string.
    for (time_t deadline : {static_cast<time_t>(1), static_cast<time_t>(9999), static_cast<time_t>(253402300799)})
    {
        const String stored = serializedValidUntil(deadline);
        EXPECT_GE(stored.size(), 5u) << stored;
        EXPECT_EQ(stored.find_first_not_of("0123456789"), String::npos) << stored;
    }
    EXPECT_EQ(serializedValidUntil(1), "0000000001");

    /// A pre-1970 (negative) deadline is normalized to the smallest expired instant (`1`) before
    /// serialization, so it is stored as the same fail-closed all-digit timestamp - never as a datetime
    /// string that an older reader would resolve to the `0` "no expiration" sentinel.
    EXPECT_EQ(serializedValidUntil(-1), "0000000001");
    EXPECT_EQ(serializedValidUntil(-2208988800), "0000000001");
    EXPECT_EQ(serializedValidUntil(-62135596800), "0000000001");
}

TEST(ValidUntilAttachEncoding, HandEditedOutOfRangeDeadlineFailsToLoad)
{
    /// `deserializeAccessEntity` is the entry point used to load stored access entities. A definition
    /// with an explicit year `0000` must fail to load instead of silently resolving to a different
    /// deadline: best-effort parsing substitutes the current year for an explicit year `0000`, and no
    /// older release ever persisted such a value (year `0000` was outside the representable `DateTime64`
    /// range), so it can only come from a hand-edited definition. The server still starts: the directory
    /// scan skips a broken definition with a logged error, and a lazy per-entity read reports the error
    /// to the operation that touches it.
    const char * definition = "ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '0000-01-01 00:00:00 UTC';";
    try
    {
        deserializeAccessEntity(definition);
        FAIL() << "expected the deadline to be rejected: " << definition;
    }
    catch (const Exception & e)
    {
        EXPECT_TRUE(e.message().contains("too far in the past")) << e.message();
    }

    /// A pre-1900 deadline (earlier than `MIN_VALID_UNTIL_TIME`), on the other hand, must NOT be rejected
    /// while deserializing a stored entity: an older release accepted `VALID UNTIL '1899-...'` at query
    /// time and persisted it as a bare datetime string, so rejecting it on load would make an upgraded
    /// server skip that user on startup - a backward-incompatible change to stored access data. Being
    /// pre-epoch (negative), it is instead normalized fail-closed to the smallest expired instant (`1`),
    /// the same value stored for any negative deadline (see `Pre1970DeadlinesAreStoredFailClosedAsExpired`).
    /// The `MIN_VALID_UNTIL_TIME` floor still rejects such a value when it comes from a query - that path
    /// is covered by the stateless test `04602_valid_until_pre_1900_rejected`.
    for (const char * pre_1900 :
         {"ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '1899-12-31 23:59:59 UTC';",
          "ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '1900-01-01 00:00:00 UTC';"})
    {
        const auto entity = deserializeAccessEntity(pre_1900);
        const auto * user = typeid_cast<const User *>(entity.get());
        ASSERT_NE(user, nullptr) << pre_1900;
        ASSERT_EQ(user->authentication_methods.size(), 1u) << pre_1900;
        EXPECT_EQ(user->authentication_methods.front().getValidUntil(), 1) << pre_1900;
    }
}

TEST(ValidUntilAttachEncoding, EpochZeroDeadlineIsNotConfusedWithNoExpiration)
{
    /// A datetime deadline at exactly the Unix epoch parses to `time_t` 0, which is the sentinel for
    /// "no expiration": `AuthenticationData::toAST` serializes the deadline only when it is non-zero,
    /// and the authentication check skips the expiration test when `valid_until` is 0. Only the literal
    /// `infinity` is meant to map to 0; a real deadline at the epoch is expired, so it is normalized to
    /// the smallest expired instant (1) - the same way the `VALID FOR` path clamps a pre-epoch deadline -
    /// instead of collapsing to a non-expiring credential.
    ASTPtr literal = make_intrusive<ASTLiteral>(Field(String("1970-01-01 00:00:00 UTC")));
    EXPECT_EQ(getValidUntilFromAST(literal, /* context= */ nullptr, /* is_interval= */ false), 1);

    const auto entity = deserializeAccessEntity("ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '1970-01-01 00:00:00 UTC';");
    const auto * user = typeid_cast<const User *>(entity.get());
    ASSERT_NE(user, nullptr);
    ASSERT_EQ(user->authentication_methods.size(), 1u);
    EXPECT_EQ(user->authentication_methods.front().getValidUntil(), 1);
}

TEST(ValidUntilAttachEncoding, HandEditedNumericEpochZeroIsNotConfusedWithNoExpiration)
{
    /// A stored deadline is serialized as an all-digit Unix timestamp (see `AuthenticationData::toAST`),
    /// which the no-context reader parses through the numeric branch. That branch must apply the same
    /// post-parse normalization as the datetime-text form: a hand-edited `'0'` (or its zero-padded
    /// `'0000000000'`) reads as `time_t` 0, which is the "no expiration" sentinel. The server never
    /// writes `0` in this form (it drops the clause for a non-expiring credential), so such a value can
    /// only be hand-edited, and it must fail closed - normalized to the smallest expired instant (1) -
    /// rather than silently turning the credential into a non-expiring one.
    for (const char * literal_value : {"0", "0000000000"})
    {
        ASTPtr literal = make_intrusive<ASTLiteral>(Field(String(literal_value)));
        EXPECT_EQ(getValidUntilFromAST(literal, /* context= */ nullptr, /* is_interval= */ false), 1) << literal_value;
    }

    for (const char * definition :
         {"ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '0';",
          "ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '0000000000';"})
    {
        const auto entity = deserializeAccessEntity(definition);
        const auto * user = typeid_cast<const User *>(entity.get());
        ASSERT_NE(user, nullptr) << definition;
        ASSERT_EQ(user->authentication_methods.size(), 1u) << definition;
        EXPECT_EQ(user->authentication_methods.front().getValidUntil(), 1) << definition;
    }
}

TEST(ValidUntilAttachEncoding, HandEditedSpacedYearZeroFailsToLoad)
{
    /// `parseDateTimeBestEffort` skips leading spaces before it decides whether the year field was
    /// specified, so a hand-edited definition with a leading space in front of an explicit year `0000`
    /// must be rejected the same way the space-free form is - otherwise best-effort parsing would
    /// substitute the current year and load a deadline the definition never asked for.
    for (const char * definition :
         {"ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL ' 0000-01-01 00:00:00 UTC';",
          "ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '   0000-01-01 00:00:00 UTC';"})
    {
        try
        {
            deserializeAccessEntity(definition);
            FAIL() << "expected the deadline to be rejected: " << definition;
        }
        catch (const Exception & e)
        {
            EXPECT_TRUE(e.message().contains("too far in the past")) << e.message();
        }
    }
}

TEST(ValidUntilAttachEncoding, HandEditedDeadlineBeyondMaxFailsToLoad)
{
    /// A deadline after the latest `DateLUT`-representable instant would be displayed clamped to
    /// `9999-12-31 23:59:59` by `SHOW CREATE USER` / `AuthenticationData::toAST` while the authentication
    /// check keeps enforcing the larger stored value, so the credential would outlive the shown deadline.
    /// This is reachable with an explicit time-zone offset that crosses the year-9999 boundary, and must
    /// fail to load rather than silently clamp on display. (A literal year past `9999` is rejected earlier,
    /// by the date-time parser itself, so it is not covered here.)
    const char * definition = "ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '9999-12-31 23:59:59 -01:00';";
    try
    {
        deserializeAccessEntity(definition);
        FAIL() << "expected the deadline to be rejected: " << definition;
    }
    catch (const Exception & e)
    {
        EXPECT_TRUE(e.message().contains("too far in the future")) << e.message();
    }

    /// The latest representable deadline itself still loads, exactly.
    const auto entity = deserializeAccessEntity("ATTACH USER u IDENTIFIED WITH no_password VALID UNTIL '9999-12-31 23:59:59 UTC';");
    const auto * user = typeid_cast<const User *>(entity.get());
    ASSERT_NE(user, nullptr);
    ASSERT_EQ(user->authentication_methods.size(), 1u);
    EXPECT_EQ(user->authentication_methods.front().getValidUntil(), 253402300799);
}

TEST(ValidUntilAttachEncoding, NoAuthenticationTypeKeepsDeadline)
{
    /// `AuthenticationData::fromAST` builds the `no_authentication` method in a dedicated branch, which
    /// used to return without setting the already-resolved deadline, silently dropping `VALID UNTIL` /
    /// `VALID FOR` for this authentication type only. The deadline must survive the same way it does for
    /// every other authentication type.
    const auto entity = deserializeAccessEntity("ATTACH USER u IDENTIFIED WITH no_authentication VALID UNTIL '0000000123';");
    const auto * user = typeid_cast<const User *>(entity.get());
    ASSERT_NE(user, nullptr);
    ASSERT_EQ(user->authentication_methods.size(), 1u);
    EXPECT_EQ(user->authentication_methods.front().getType(), AuthenticationType::NO_AUTHENTICATION);
    EXPECT_EQ(user->authentication_methods.front().getValidUntil(), 123);
}
