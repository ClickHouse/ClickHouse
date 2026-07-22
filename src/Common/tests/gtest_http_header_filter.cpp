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

/// An inline (?-i) scope re-enables case-sensitive matching for that literal.
/// On master the regexp matched the original-case header, so such a config must
/// keep blocking it: the regexp is matched against the original-case name, not a
/// lower-cased one, otherwise an existing (?-i) blocklist would silently weaken.
TEST(HTTPHeaderFilter, RegexpInlineCaseSensitiveScopeStillBlocksOriginalCase)
{
    HTTPHeaderFilter filter;
    configure(filter, R"(
        <clickhouse>
            <http_forbid_headers>
                <header_regexp>(?-i)Authorization</header_regexp>
            </http_forbid_headers>
        </clickhouse>
    )");

    /// The case-sensitive literal still matches the header it matched on master.
    EXPECT_TRUE(isForbidden(filter, "Authorization"));
    /// And the (?-i) scope keeps its case-sensitive semantics for other cases.
    EXPECT_FALSE(isForbidden(filter, "authorization"));
    EXPECT_FALSE(isForbidden(filter, "AUTHORIZATION"));
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
