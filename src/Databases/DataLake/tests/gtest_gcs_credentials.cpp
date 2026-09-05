#include <Databases/DataLake/StorageCredentials.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/HTTPHeaderFilter.h>
#include <gtest/gtest.h>

namespace DataLake::Test
{

class GCSCredentialsTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(GCSCredentialsTest, BasicTokenStorage)
{
    GCSCredentials creds("my-test-token");
    EXPECT_EQ(creds.getToken(), "my-test-token");
}

TEST_F(GCSCredentialsTest, AddCredentialsToEngineArgs)
{
    GCSCredentials creds("my-test-token");
    DB::ASTs engine_args;
    engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>("gs://bucket/path"));
    creds.addCredentialsToEngineArgs(engine_args);
    EXPECT_EQ(engine_args.size(), 3u);
}

TEST_F(GCSCredentialsTest, AddCredentialsDuplicateThrows)
{
    GCSCredentials creds("my-test-token");
    DB::ASTs engine_args;
    engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>("gs://bucket/path"));
    engine_args.push_back(DB::make_intrusive<DB::ASTLiteral>("extra"));
    EXPECT_THROW(creds.addCredentialsToEngineArgs(engine_args), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsNewline)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Authorization", "Bearer token\nX-Injected: malicious"});
    EXPECT_THROW((void)filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsCarriageReturn)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Authorization", "Bearer token\r\nX-Injected: malicious"});
    EXPECT_THROW((void)filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationAcceptsValidToken)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Authorization", "Bearer ya29.valid-gcs-token_1234"});
    EXPECT_NO_THROW((void)filter.checkAndNormalizeHeaders(headers));
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsColonInName)
{
    /// A ':' in the name is not equal to any forbidden entry, but a peer still parses a
    /// forbidden header from it, so a name containing ':' is rejected.
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Cookie:j=\"x", "y\";session=abc"});
    EXPECT_THROW((void)filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsCarriageReturnInName)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"X-Foo\rX-Injected", "value"});
    EXPECT_THROW((void)filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsLineFeedInName)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"X-Foo\nX-Injected", "value"});
    EXPECT_THROW((void)filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationAcceptsColonInValue)
{
    /// ':' is legal in a value (for example "Host: example.com:8080"); only names reject it.
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Host", "example.com:8080"});
    EXPECT_NO_THROW((void)filter.checkAndNormalizeHeaders(headers));
}

TEST_F(GCSCredentialsTest, HeaderValidationNormalizesWhitespaceInName)
{
    /// Whitespace/control in a name is still stripped rather than rejected, preserving the
    /// historical behaviour for stored objects that are re-validated on ATTACH. Assert the
    /// normalized name on the returned entries (callers send what is returned), so a change that
    /// stopped normalizing would be caught here.
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"X-A B", "value"});
    DB::HTTPHeaderEntries normalized;
    EXPECT_NO_THROW(normalized = filter.checkAndNormalizeHeaders(headers));
    ASSERT_EQ(normalized.size(), 1u);
    EXPECT_EQ(normalized[0].name, "X-AB");
}

TEST_F(GCSCredentialsTest, HeaderValidationAcceptsNameEmptyAfterNormalization)
{
    /// A name that is only whitespace/control normalizes to "" and is accepted, not rejected: it
    /// cannot forge a forbidden header or split the request, and rejecting it would break ATTACH
    /// of a table stored with such a name. This matches the behaviour before this change.
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({" \t ", "value"});
    DB::HTTPHeaderEntries normalized;
    EXPECT_NO_THROW(normalized = filter.checkAndNormalizeHeaders(headers));
    ASSERT_EQ(normalized.size(), 1u);
    EXPECT_EQ(normalized[0].name, "");
}

}
