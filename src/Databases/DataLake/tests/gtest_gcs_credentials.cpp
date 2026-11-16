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
    EXPECT_THROW(filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationRejectsCarriageReturn)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Authorization", "Bearer token\r\nX-Injected: malicious"});
    EXPECT_THROW(filter.checkAndNormalizeHeaders(headers), DB::Exception);
}

TEST_F(GCSCredentialsTest, HeaderValidationAcceptsValidToken)
{
    DB::HTTPHeaderFilter filter;
    DB::HTTPHeaderEntries headers;
    headers.push_back({"Authorization", "Bearer ya29.valid-gcs-token_1234"});
    EXPECT_NO_THROW(filter.checkAndNormalizeHeaders(headers));
}

}
