#include <Common/HTTPHeaderFilter.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/Exception.h>
#include <Poco/Util/XMLConfiguration.h>
#include <gtest/gtest.h>
#include <sstream>

using namespace DB;

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> configFromXML(const std::string & xml)
{
    std::istringstream stream(xml);
    return new Poco::Util::XMLConfiguration(stream);
}

/// HTTPHeaderFilter holds a std::mutex, so it is neither copyable nor movable;
/// configure it in place rather than returning it by value.
void configure(HTTPHeaderFilter & filter, const std::string & xml)
{
    auto config = configFromXML(xml);
    filter.setValuesFromConfig(*config);
}

bool isForbidden(const HTTPHeaderFilter & filter, const std::string & name)
{
    HTTPHeaderEntries entries{{name, "value"}};
    try
    {
        filter.checkAndNormalizeHeaders(entries);
    }
    catch (const Exception &)
    {
        return true;
    }
    return false;
}

/// The same question asked the way an S3 caller asks it: the name reaches the filter lower-cased.
bool isForbiddenForS3(const HTTPHeaderFilter & filter, const std::string & name)
{
    NormalizedHTTPHeaderEntries entries(HTTPHeaderEntries{{name, "value"}});
    try
    {
        filter.checkAndNormalizeHeaders(entries);
    }
    catch (const Exception &)
    {
        return true;
    }
    return false;
}

}

/// HTTP header names are case-insensitive (RFC 7230 section 3.2). A forbidden
/// exact header configured as "Authorization" must block every case variant,
/// otherwise the http_forbid_headers blocklist is trivially bypassed.
TEST(HTTPHeaderFilter, ExactMatchIsCaseInsensitive)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>Authorization</header>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    EXPECT_TRUE(isForbidden(filter, "authorization"));
    EXPECT_TRUE(isForbidden(filter, "AUTHORIZATION"));
    EXPECT_TRUE(isForbidden(filter, "aUtHoRiZaTiOn"));
}

/// The configured name itself may be in any case; matching must still be
/// case-insensitive against the incoming header.
TEST(HTTPHeaderFilter, ExactMatchLowerCaseConfigMixedCaseInput)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>authorization</header>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    EXPECT_TRUE(isForbidden(filter, "AUTHORIZATION"));
}

/// An operator who writes the name in upper case blocks the lower-case header too. The exact set
/// holds the configured name lower-cased, so the spelling in the config does not matter.
TEST(HTTPHeaderFilter, ExactMatchUpperCaseConfigBlocksEveryCase)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>AUTHORIZATION</header>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "authorization"));
    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    EXPECT_TRUE(isForbidden(filter, "AUTHORIZATION"));
}

/// The same for a regexp written in upper case. The pattern is not lower-cased -- that would
/// corrupt a metacharacter such as \D or [A-Z] -- so the case insensitivity comes from the RE2
/// option instead.
TEST(HTTPHeaderFilter, RegexpUpperCaseConfigBlocksEveryCase)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>AUTHORIZATION</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "authorization"));
    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    EXPECT_TRUE(isForbidden(filter, "AUTHORIZATION"));
}

/// A regexp pattern without an explicit (?i) flag must still match
/// case-insensitively, because header names are case-insensitive.
TEST(HTTPHeaderFilter, RegexpMatchIsCaseInsensitiveWithoutFlag)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>x-custom-.*</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "x-custom-token"));
    EXPECT_TRUE(isForbidden(filter, "X-Custom-Token"));
    EXPECT_TRUE(isForbidden(filter, "X-CUSTOM-SECRET"));
}

/// An explicit (?i) prefix (the pre-existing way admins requested case
/// insensitivity) must keep working.
TEST(HTTPHeaderFilter, RegexpMatchExplicitInlineFlagStillWorks)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>(?i)(secret_header)</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "secret_header"));
    EXPECT_TRUE(isForbidden(filter, "SECRET_HEADER"));
    EXPECT_TRUE(isForbidden(filter, "Secret_Header"));
}

/// The next two describe how `http_forbid_headers` behaves today, not how it ought to. A rule can
/// be accepted and then forbid nothing: an inline (?-i) scope makes it match a single spelling of a
/// name the RFC calls case-insensitive, and a pattern that does not compile is skipped with only a
/// warning. Catching either needs the parsed pattern -- `re2::Regexp::Parse` and the FoldCase flag
/// of each literal node -- rather than a substring check, so it is left to a follow-up. These
/// expectations are meant to flip when that lands.
TEST(HTTPHeaderFilter, RegexpInlineCaseSensitiveScopeBlocksOneSpellingOnly)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>(?-i)Authorization</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    /// The very same header, which RFC 7230 3.2 says is the same name, goes through.
    EXPECT_FALSE(isForbidden(filter, "authorization"));
    /// And it never blocks an S3 header, whose name arrives lower-cased whatever the caller wrote.
    EXPECT_FALSE(isForbiddenForS3(filter, "Authorization"));
}

TEST(HTTPHeaderFilter, RegexpThatDoesNotCompileForbidsNothing)
{
    HTTPHeaderFilter filter;
    configure(filter,
        "<clickhouse><http_forbid_headers><header_regexp>x-custom-[</header_regexp>"
        "</http_forbid_headers></clickhouse>");

    EXPECT_FALSE(isForbidden(filter, "x-custom-token"));
}

/// The filter also guards a collection that already holds lower-cased names. The verdict must be
/// the same there, and the collection must still hold lower-cased names afterwards.
TEST(HTTPHeaderFilter, ChecksNormalizedEntries)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>Authorization</header>
            </http_forbid_headers>
        </clickhouse>
    )");

    NormalizedHTTPHeaderEntries forbidden(HTTPHeaderEntries{{"Authorization", "Bearer token"}});
    EXPECT_THROW(filter.checkAndNormalizeHeaders(forbidden), Exception);

    NormalizedHTTPHeaderEntries allowed(HTTPHeaderEntries{{"X-Amz-Meta\tOwner", "analytics"}});
    EXPECT_NO_THROW(filter.checkAndNormalizeHeaders(allowed));

    const HTTPHeaderEntries seen(allowed.begin(), allowed.end());
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].name, "x-amz-metaowner");
}

/// Case normalization must compose with whitespace/control-character stripping:
/// a name padded with whitespace and in a different case is still forbidden.
TEST(HTTPHeaderFilter, CaseInsensitiveComposesWithWhitespaceStripping)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>Authorization</header>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "  aUtHoRiZaTiOn  "));
    EXPECT_TRUE(isForbidden(filter, "Auth\torization"));
}

/// Headers not on the blocklist must still be allowed, in any case.
TEST(HTTPHeaderFilter, UnrelatedHeadersStillAllowed)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header>Authorization</header>
                <header_regexp>x-custom-.*</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_FALSE(isForbidden(filter, "Content-Type"));
    EXPECT_FALSE(isForbidden(filter, "accept"));
    EXPECT_FALSE(isForbidden(filter, "X-Other-Header"));
}

/// Regexp metacharacters must keep their meaning under case-insensitive
/// matching: case insensitivity must come from RE2 options, not from
/// lower-casing the pattern string (which would corrupt \d, char classes, ...).
TEST(HTTPHeaderFilter, RegexpMetacharactersPreserved)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>x-id-\d+</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    EXPECT_TRUE(isForbidden(filter, "x-id-123"));
    EXPECT_TRUE(isForbidden(filter, "X-ID-456"));
    EXPECT_FALSE(isForbidden(filter, "x-id-abc"));
}

/// An empty configuration forbids nothing.
TEST(HTTPHeaderFilter, EmptyConfigForbidsNothing)
{
    HTTPHeaderFilter filter;
    EXPECT_FALSE(isForbidden(filter, "Authorization"));
}

/// A bare CR or LF in a header name or value terminates the header line, so it could smuggle an
/// extra header into the request. Both must be rejected (not only LF), for names and values,
/// independently of the http_forbid_headers blocklist.
TEST(HTTPHeaderFilter, RejectsCarriageReturnAndNewline)
{
    HTTPHeaderFilter filter;

    auto rejects = [&](const std::string & name, const std::string & value)
    {
        HTTPHeaderEntries entries{{name, value}};
        try { filter.checkAndNormalizeHeaders(entries); }
        catch (const Exception &) { return true; }
        return false;
    };

    EXPECT_TRUE(rejects("Authorization", "Bearer token\rX-Injected: evil"));
    EXPECT_TRUE(rejects("Authorization", "Bearer token\nX-Injected: evil"));
    EXPECT_TRUE(rejects("Authorization", "Bearer token\r\nX-Injected: evil"));
    EXPECT_TRUE(rejects("Bad\rName", "value"));
    EXPECT_FALSE(rejects("Authorization", "Bearer good-token"));
}
