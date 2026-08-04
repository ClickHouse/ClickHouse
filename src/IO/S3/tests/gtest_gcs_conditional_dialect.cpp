#include "config.h"
#if USE_AWS_S3
#include <gtest/gtest.h>
#include <IO/S3/GCSConditionalDialect.h>
#include <Common/Exception.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <base/defines.h>   /// DEBUG_OR_SANITIZER_BUILD

using namespace DB::S3;

static Aws::Http::Standard::StandardHttpRequest makeRequest(
    const char * url = "https://storage.googleapis.com/b/k",
    Aws::Http::HttpMethod method = Aws::Http::HttpMethod::HTTP_PUT)
{
    Aws::Http::Standard::StandardHttpRequest request{Aws::Http::URI(url), method};
    request.SetHeaderValue("host", "storage.googleapis.com");
    return request;
}

TEST(GCSConditionalDialect, IfNoneMatchStarBecomesGenerationZero)
{
    auto r = makeRequest();
    r.SetHeaderValue("if-none-match", "*");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_FALSE(r.HasHeader("if-none-match"));
    EXPECT_EQ(r.GetHeaderValue("x-goog-if-generation-match"), "0");
}

TEST(GCSConditionalDialect, IfMatchDigitsMappedQuotesStripped)
{
    auto r = makeRequest();
    r.SetHeaderValue("if-match", "\"1783078552147137\"");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_FALSE(r.HasHeader("if-match"));
    EXPECT_EQ(r.GetHeaderValue("x-goog-if-generation-match"), "1783078552147137");
}

TEST(GCSConditionalDialect, IfMatchUnquotedDigitsAlsoAccepted)
{
    auto r = makeRequest();
    r.SetHeaderValue("if-match", "1783078552147137");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_EQ(r.GetHeaderValue("x-goog-if-generation-match"), "1783078552147137");
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(GCSConditionalDialect, NonNumericIfMatchThrows)
{
    /// The guard throws LOGICAL_ERROR (a broken-invariant signal: an S3-style ETag reached a
    /// generation-dialect client). Under abort_on_logical_error that aborts at construction instead of
    /// being catchable, so GCSConditionalDialectDeathTest.NonNumericIfMatchAborts proves it there.
    auto r = makeRequest();
    r.SetHeaderValue("if-match", "\"6654c734ccab8f440ff0825eb443dc7f\"");  /// an ETag leaked into a generation dialect
    EXPECT_THROW(applyGcsConditionalDialectToRequest(r), DB::Exception);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(GCSConditionalDialectDeathTest, NonNumericIfMatchAborts)
{
    auto r = makeRequest();
    r.SetHeaderValue("if-match", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    EXPECT_DEATH({ applyGcsConditionalDialectToRequest(r); }, "");
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(GCSConditionalDialect, NonStarIfNoneMatchThrows)
{
    /// LOGICAL_ERROR (broken invariant); aborts under abort_on_logical_error -- see the DeathTest below.
    auto r = makeRequest();
    r.SetHeaderValue("if-none-match", "\"123\"");
    EXPECT_THROW(applyGcsConditionalDialectToRequest(r), DB::Exception);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(GCSConditionalDialectDeathTest, NonStarIfNoneMatchAborts)
{
    auto r = makeRequest();
    r.SetHeaderValue("if-none-match", "\"123\"");
    EXPECT_DEATH({ applyGcsConditionalDialectToRequest(r); }, "");
}
#endif

TEST(GCSConditionalDialect, AmzHeadersRenamedAuthArtifactsDropped)
{
    auto r = makeRequest();
    r.SetHeaderValue("authorization", "AWS4-HMAC-SHA256 ...");
    r.SetHeaderValue("x-amz-date", "20260703T000000Z");
    r.SetHeaderValue("x-amz-content-sha256", "deadbeef");
    r.SetHeaderValue("x-amz-security-token", "tok");
    r.SetHeaderValue("x-amz-api-version", "2006-03-01");
    r.SetHeaderValue("x-amz-meta-foo", "bar");
    r.SetHeaderValue("x-amz-storage-class", "STANDARD");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_FALSE(r.HasHeader("authorization"));
    EXPECT_FALSE(r.HasHeader("x-amz-date"));
    EXPECT_FALSE(r.HasHeader("x-amz-content-sha256"));
    EXPECT_FALSE(r.HasHeader("x-amz-security-token"));
    EXPECT_FALSE(r.HasHeader("x-amz-api-version"));
    EXPECT_FALSE(r.HasHeader("x-amz-meta-foo"));
    EXPECT_FALSE(r.HasHeader("x-amz-storage-class"));
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-foo"), "bar");
    EXPECT_EQ(r.GetHeaderValue("x-goog-storage-class"), "STANDARD");
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(GCSConditionalDialect, ConditionalCompleteMultipartUploadThrows)
{
    /// GCS silently IGNORES preconditions on CompleteMultipartUpload (measured live 2026-07-03) --
    /// sending one would be silent data loss, so the dialect fails closed client-side with a
    /// LOGICAL_ERROR; aborts under abort_on_logical_error -- see the DeathTest below.
    auto r = makeRequest("https://storage.googleapis.com/b/k?uploadId=abc", Aws::Http::HttpMethod::HTTP_POST);
    r.SetHeaderValue("if-none-match", "*");
    EXPECT_THROW(applyGcsConditionalDialectToRequest(r), DB::Exception);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(GCSConditionalDialectDeathTest, ConditionalCompleteMultipartUploadAborts)
{
    auto r = makeRequest("https://storage.googleapis.com/b/k?uploadId=abc", Aws::Http::HttpMethod::HTTP_POST);
    r.SetHeaderValue("if-none-match", "*");
    EXPECT_DEATH({ applyGcsConditionalDialectToRequest(r); }, "");
}
#endif

TEST(GCSConditionalDialect, UnconditionalCompleteMultipartUploadPasses)
{
    auto r = makeRequest("https://storage.googleapis.com/b/k?uploadId=abc", Aws::Http::HttpMethod::HTTP_POST);
    EXPECT_NO_THROW(applyGcsConditionalDialectToRequest(r));
}

TEST(GCSConditionalDialect, UploadPartIsNotComplete)
{
    /// PUT ?partNumber=N&uploadId=... is an UploadPart, not a Complete — must not trip the guard.
    auto r = makeRequest("https://storage.googleapis.com/b/k?partNumber=1&uploadId=abc", Aws::Http::HttpMethod::HTTP_PUT);
    EXPECT_NO_THROW(applyGcsConditionalDialectToRequest(r));
}

TEST(GCSConditionalDialect, ResponseGenerationOverridesETag)
{
    Poco::Net::HTTPResponse response;
    response.set("ETag", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    response.set("x-goog-generation", "1783078552147137");
    auto override_etag = gcsGenerationETagOverride(response);
    ASSERT_TRUE(override_etag.has_value());
    EXPECT_EQ(*override_etag, "\"1783078552147137\"");
}

TEST(GCSConditionalDialect, ResponseWithoutGenerationNoOverride)
{
    Poco::Net::HTTPResponse response;
    response.set("ETag", "\"abc\"");
    EXPECT_FALSE(gcsGenerationETagOverride(response).has_value());
}
#endif
