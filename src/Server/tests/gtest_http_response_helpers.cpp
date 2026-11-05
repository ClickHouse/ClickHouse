#include <gtest/gtest.h>
#include <Server/HTTP/HTTPResponseHelpers.h>

using namespace DB;

TEST(AddVaryField, SetsWhenAbsent)
{
    HTTPResponse response;
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "Accept-Encoding");
}

TEST(AddVaryField, AppendsToExistingList)
{
    HTTPResponse response;
    response.set("Vary", "Origin");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "Origin, Accept-Encoding");
}

TEST(AddVaryField, DoesNotDuplicateExactToken)
{
    HTTPResponse response;
    response.set("Vary", "Accept-Encoding");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "Accept-Encoding");
}

TEST(AddVaryField, DoesNotDuplicateCaseInsensitively)
{
    HTTPResponse response;
    response.set("Vary", "origin, accept-encoding");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "origin, accept-encoding");
}

TEST(AddVaryField, SubstringOfAnotherTokenDoesNotCount)
{
    /// A different header name that merely contains "Accept-Encoding" as a substring
    /// must not suppress adding the real one.
    HTTPResponse response;
    response.set("Vary", "X-Accept-Encoding-Debug");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "X-Accept-Encoding-Debug, Accept-Encoding");
}

TEST(AddVaryField, StarMeansEverything)
{
    HTTPResponse response;
    response.set("Vary", "*");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "*");
}

TEST(AddVaryField, TokensAreTrimmed)
{
    HTTPResponse response;
    response.set("Vary", "  Origin, Accept-Encoding  ");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "  Origin, Accept-Encoding  ");
}

TEST(AddVaryField, EmptyValueIsReplaced)
{
    HTTPResponse response;
    response.set("Vary", "");
    addVaryField(response, "Accept-Encoding");
    EXPECT_EQ(response.get("Vary"), "Accept-Encoding");
}
