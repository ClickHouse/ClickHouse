#include "config.h"
#if USE_AWS_S3
#include <gtest/gtest.h>
#include <IO/S3/GCSConditionalDialect.h>
#include <Common/Exception.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <Poco/Net/HTTPResponse.h>
#include <base/defines.h>   /// DEBUG_OR_SANITIZER_BUILD
#include <memory>

using namespace DB::S3;

static Aws::Http::Standard::StandardHttpRequest makeRequest(
    const char * url = "https://storage.googleapis.com/b/k",
    Aws::Http::HttpMethod method = Aws::Http::HttpMethod::HTTP_PUT)
{
    Aws::Http::Standard::StandardHttpRequest request{Aws::Http::URI(url), method};
    request.SetHeaderValue("host", "storage.googleapis.com");
    return request;
}

/// Installs every AWS SigV4 artifact both authentication paths must clear.
static void addAwsAuthArtifacts(Aws::Http::HttpRequest & r)
{
    r.SetHeaderValue("authorization", "AWS4-HMAC-SHA256 ...");
    r.SetHeaderValue("x-amz-date", "20260703T000000Z");
    r.SetHeaderValue("x-amz-content-sha256", "deadbeef");
    r.SetHeaderValue("x-amz-security-token", "tok");
    r.SetHeaderValue("x-amz-api-version", "2006-03-01");
}

static void expectNoAwsAuthArtifacts(const Aws::Http::HttpRequest & r)
{
    EXPECT_FALSE(r.HasHeader("authorization"));
    EXPECT_FALSE(r.HasHeader("x-amz-date"));
    EXPECT_FALSE(r.HasHeader("x-amz-content-sha256"));
    EXPECT_FALSE(r.HasHeader("x-amz-security-token"));
    EXPECT_FALSE(r.HasHeader("x-amz-api-version"));
}

/// ---------------------------------------------------------------------------------------------
/// Conditions: only the native-conditional adapter translates them.
/// ---------------------------------------------------------------------------------------------

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

TEST(GCSConditionalDialect, NonNumericIfMatchThrows)
{
    /// CORRUPTED_DATA, not a broken internal invariant: the value can come from a persisted manifest
    /// token or from a storage HEAD whose response carried no generation, and `mintingTypeMatches`
    /// upstream only compares the token KIND, never the shape of its value.
    auto r = makeRequest();
    r.SetHeaderValue("if-match", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    EXPECT_THROW(applyGcsConditionalDialectToRequest(r), DB::Exception);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(GCSConditionalDialect, NonStarIfNoneMatchThrows)
{
    /// LOGICAL_ERROR: CAS only ever sends `*`, so any other value is a wiring break, not input.
    /// Under abort_on_logical_error that aborts at construction instead of being catchable, so
    /// GCSConditionalDialectDeathTest.NonStarIfNoneMatchAborts proves it there.
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

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(GCSConditionalDialect, ConditionalCompleteMultipartUploadThrows)
{
    /// GCS silently IGNORES preconditions on CompleteMultipartUpload (measured live 2026-07-03) --
    /// sending one would be silent data loss, so this fails closed client-side with a LOGICAL_ERROR:
    /// every conditional non-blob write, including create-if-absent artifacts and conditional
    /// replacements, forces a single PUT. Reaching here is a wiring break and aborts under
    /// abort_on_logical_error; see the DeathTest below. Blob publication uses unconditional multipart.
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

/// Neither authentication preparation may acquire condition semantics: a request that was never
/// marked native-conditional must keep its standard ETag preconditions all the way to the wire.
TEST(GCSConditionalDialect, AuthenticationPreparationLeavesConditionsAlone)
{
    auto goog4 = makeRequest();
    goog4.SetHeaderValue("if-match", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    goog4.SetHeaderValue("if-none-match", "*");
    prepareGcsRequestForGoog4Authentication(goog4);
    EXPECT_EQ(goog4.GetHeaderValue("if-match"), "\"6654c734ccab8f440ff0825eb443dc7f\"");
    EXPECT_EQ(goog4.GetHeaderValue("if-none-match"), "*");
    EXPECT_FALSE(goog4.HasHeader("x-goog-if-generation-match"));

    auto oauth = makeRequest();
    oauth.SetHeaderValue("if-match", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    prepareGcsRequestForOAuthAuthentication(oauth);
    EXPECT_EQ(oauth.GetHeaderValue("if-match"), "\"6654c734ccab8f440ff0825eb443dc7f\"");
    EXPECT_FALSE(oauth.HasHeader("x-goog-if-generation-match"));
}

/// ---------------------------------------------------------------------------------------------
/// Request metadata: the one targeted prefix mapping the adapter owns.
/// ---------------------------------------------------------------------------------------------

TEST(GCSConditionalDialect, RequestMetadataPrefixIsMapped)
{
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-meta-foo", "bar");
    r.SetHeaderValue("x-goog-meta-already", "kept");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_FALSE(r.HasHeader("x-amz-meta-foo"));
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-foo"), "bar");
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-already"), "kept");
}

TEST(GCSConditionalDialect, RequestMetadataDoesNotTouchOtherAmzHeaders)
{
    /// The adapter is not a blanket rewrite: only conditions and `x-amz-meta-*` are its business.
    /// Whatever else the SDK put on the request is the authentication preparation's problem.
    auto r = makeRequest();
    addAwsAuthArtifacts(r);
    r.SetHeaderValue("x-amz-storage-class", "STANDARD");
    r.SetHeaderValue("x-amz-tagging", "a=b");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_EQ(r.GetHeaderValue("authorization"), "AWS4-HMAC-SHA256 ...");
    EXPECT_EQ(r.GetHeaderValue("x-amz-date"), "20260703T000000Z");
    EXPECT_EQ(r.GetHeaderValue("x-amz-storage-class"), "STANDARD");
    EXPECT_EQ(r.GetHeaderValue("x-amz-tagging"), "a=b");
    EXPECT_FALSE(r.HasHeader("x-goog-storage-class"));
}

TEST(GCSConditionalDialect, ConflictingRequestMetadataRejected)
{
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-meta-foo", "one");
    r.SetHeaderValue("x-goog-meta-foo", "two");
    EXPECT_THROW(applyGcsConditionalDialectToRequest(r), DB::Exception);
}

TEST(GCSConditionalDialect, AgreeingRequestMetadataAccepted)
{
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-meta-foo", "same");
    r.SetHeaderValue("x-goog-meta-foo", "same");
    EXPECT_NO_THROW(applyGcsConditionalDialectToRequest(r));
    EXPECT_FALSE(r.HasHeader("x-amz-meta-foo"));
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-foo"), "same");
}

/// ---------------------------------------------------------------------------------------------
/// Native OAuth authentication preparation.
/// ---------------------------------------------------------------------------------------------

TEST(GCSConditionalDialect, OAuthPreparationDropsAwsAuthArtifacts)
{
    auto r = makeRequest();
    addAwsAuthArtifacts(r);
    prepareGcsRequestForOAuthAuthentication(r);
    expectNoAwsAuthArtifacts(r);
}

TEST(GCSConditionalDialect, OAuthPreparationPassesRemainingAmzHeadersThrough)
{
    /// Native OAuth has no GOOG4-style allowlist by design: after the signing artifacts are gone it
    /// matches the ordinary OAuth path, which leaves SDK headers untouched. Pinning this stops a
    /// later adapter change from silently broadening OAuth rewriting.
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-sdk-checksum-algorithm", "CRC32");
    r.SetHeaderValue("x-amz-checksum-crc32", "abcd==");
    r.SetHeaderValue("x-amz-trailer", "x-amz-checksum-crc32");
    r.SetHeaderValue("x-amz-decoded-content-length", "1024");
    r.SetHeaderValue("x-amz-storage-class", "STANDARD");
    r.SetHeaderValue("x-amz-tagging", "a=b");
    EXPECT_NO_THROW(prepareGcsRequestForOAuthAuthentication(r));
    EXPECT_EQ(r.GetHeaderValue("x-amz-sdk-checksum-algorithm"), "CRC32");
    EXPECT_EQ(r.GetHeaderValue("x-amz-checksum-crc32"), "abcd==");
    EXPECT_EQ(r.GetHeaderValue("x-amz-trailer"), "x-amz-checksum-crc32");
    EXPECT_EQ(r.GetHeaderValue("x-amz-decoded-content-length"), "1024");
    EXPECT_EQ(r.GetHeaderValue("x-amz-storage-class"), "STANDARD");
    EXPECT_EQ(r.GetHeaderValue("x-amz-tagging"), "a=b");
}

/// ---------------------------------------------------------------------------------------------
/// GOOG4 authentication preparation: every x-amz-* header has a decided fate.
/// ---------------------------------------------------------------------------------------------

TEST(GCSConditionalDialect, Goog4PreparationDropsAwsAuthArtifacts)
{
    auto r = makeRequest();
    addAwsAuthArtifacts(r);
    prepareGcsRequestForGoog4Authentication(r);
    expectNoAwsAuthArtifacts(r);
    /// Dropped, not renamed: a `x-goog-`-prefixed copy of a SigV4 artifact would be signed as part of
    /// the GOOG4 canonical request.
    EXPECT_FALSE(r.HasHeader("x-goog-date"));
    EXPECT_FALSE(r.HasHeader("x-goog-content-sha256"));
    EXPECT_FALSE(r.HasHeader("x-goog-security-token"));
    EXPECT_FALSE(r.HasHeader("x-goog-api-version"));
}

TEST(GCSConditionalDialect, Goog4PreparationRenamesTargetedStorageAndCopyHeaders)
{
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-meta-foo", "bar");
    r.SetHeaderValue("x-amz-storage-class", "STANDARD");
    r.SetHeaderValue("x-amz-copy-source", "b/src");
    r.SetHeaderValue("x-amz-copy-source-range", "bytes=0-9");
    r.SetHeaderValue("x-amz-metadata-directive", "REPLACE");
    prepareGcsRequestForGoog4Authentication(r);
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-foo"), "bar");
    EXPECT_EQ(r.GetHeaderValue("x-goog-storage-class"), "STANDARD");
    EXPECT_EQ(r.GetHeaderValue("x-goog-copy-source"), "b/src");
    EXPECT_EQ(r.GetHeaderValue("x-goog-copy-source-range"), "bytes=0-9");
    EXPECT_EQ(r.GetHeaderValue("x-goog-metadata-directive"), "REPLACE");
    EXPECT_FALSE(r.HasHeader("x-amz-meta-foo"));
    EXPECT_FALSE(r.HasHeader("x-amz-storage-class"));
    EXPECT_FALSE(r.HasHeader("x-amz-copy-source"));
    EXPECT_FALSE(r.HasHeader("x-amz-copy-source-range"));
    EXPECT_FALSE(r.HasHeader("x-amz-metadata-directive"));
}

TEST(GCSConditionalDialect, Goog4PreparationRenameIsIdempotentAfterTheAdapter)
{
    /// A marked request runs the adapter first, which already moved `x-amz-meta-*` across. The
    /// preparation must then find nothing to do rather than tripping its own conflict check.
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-meta-foo", "bar");
    applyGcsConditionalDialectToRequest(r);
    EXPECT_NO_THROW(prepareGcsRequestForGoog4Authentication(r));
    EXPECT_EQ(r.GetHeaderValue("x-goog-meta-foo"), "bar");
}

TEST(GCSConditionalDialect, Goog4PreparationConsumesSdkChecksumHeaders)
{
    /// Flexible checksums are an S3 protocol feature with no GCS XML API counterpart; the body they
    /// describe goes out unchanged, so consuming them is lossless on the wire.
    auto r = makeRequest();
    r.SetHeaderValue("x-amz-sdk-checksum-algorithm", "CRC32");
    r.SetHeaderValue("x-amz-checksum-crc32", "abcd==");
    r.SetHeaderValue("x-amz-checksum-sha256", "efgh==");
    prepareGcsRequestForGoog4Authentication(r);
    EXPECT_FALSE(r.HasHeader("x-amz-sdk-checksum-algorithm"));
    EXPECT_FALSE(r.HasHeader("x-amz-checksum-crc32"));
    EXPECT_FALSE(r.HasHeader("x-amz-checksum-sha256"));
    /// Consumed, not renamed — GCS would not understand them under the other prefix either.
    EXPECT_FALSE(r.HasHeader("x-goog-sdk-checksum-algorithm"));
    EXPECT_FALSE(r.HasHeader("x-goog-checksum-crc32"));
}

TEST(GCSConditionalDialect, Goog4PreparationRejectsAwsChunkedFraming)
{
    /// BAD_ARGUMENTS: these announce a body framing GCS cannot parse, and consuming them would
    /// misdescribe a body already on the wire, so refuse rather than guess.
    auto trailer = makeRequest();
    trailer.SetHeaderValue("x-amz-trailer", "x-amz-checksum-crc32");
    EXPECT_THROW(prepareGcsRequestForGoog4Authentication(trailer), DB::Exception);

    auto decoded = makeRequest();
    decoded.SetHeaderValue("x-amz-decoded-content-length", "1024");
    EXPECT_THROW(prepareGcsRequestForGoog4Authentication(decoded), DB::Exception);
}

TEST(GCSConditionalDialect, Goog4PreparationRejectsUnknownAmzExtension)
{
    /// BAD_ARGUMENTS before any network I/O: GCS rejects a mixed-prefix request, so an unmapped
    /// header can be neither translated nor sent.
    for (const char * header : {"x-amz-tagging", "x-amz-acl", "x-amz-server-side-encryption",
                                "x-amz-server-side-encryption-customer-key", "x-amz-website-redirect-location"})
    {
        auto r = makeRequest();
        r.SetHeaderValue(header, "whatever");
        EXPECT_THROW(prepareGcsRequestForGoog4Authentication(r), DB::Exception) << header;
    }
}

TEST(GCSConditionalDialect, Goog4PreparationLeavesNonAmzHeadersAlone)
{
    /// `amz-sdk-invocation-id` and `amz-sdk-request` do not carry the `x-amz-` prefix and are not
    /// part of any canonical request, so they pass through untouched.
    auto r = makeRequest();
    r.SetHeaderValue("amz-sdk-invocation-id", "id");
    r.SetHeaderValue("amz-sdk-request", "attempt=1");
    r.SetHeaderValue("content-type", "binary/octet-stream");
    prepareGcsRequestForGoog4Authentication(r);
    EXPECT_EQ(r.GetHeaderValue("amz-sdk-invocation-id"), "id");
    EXPECT_EQ(r.GetHeaderValue("amz-sdk-request"), "attempt=1");
    EXPECT_EQ(r.GetHeaderValue("content-type"), "binary/octet-stream");
    EXPECT_EQ(r.GetHeaderValue("host"), "storage.googleapis.com");
}

/// ---------------------------------------------------------------------------------------------
/// Response adaptation.
/// ---------------------------------------------------------------------------------------------

namespace
{

/// A real SDK response object, so these tests exercise the type `PocoHTTPClient` actually fills.
struct ResponseFixture
{
    /// `StandardHttpResponse`'s constructor builds its body stream by CALLING the originating
    /// request's response-stream factory, so the request must carry one or the response cannot be
    /// constructed at all.
    static std::shared_ptr<Aws::Http::Standard::StandardHttpRequest> makeOriginatingRequest()
    {
        auto request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
            Aws::Http::URI("https://storage.googleapis.com/b/k"), Aws::Http::HttpMethod::HTTP_HEAD);
        request->SetResponseStreamFactory([] { return Aws::New<Aws::StringStream>("gtest", ""); });
        return request;
    }

    std::shared_ptr<Aws::Http::Standard::StandardHttpRequest> request = makeOriginatingRequest();
    Aws::Http::Standard::StandardHttpResponse sdk{request};
    Poco::Net::HTTPResponse poco;

    /// Mirrors PocoHTTPClient: every response header is copied onto the SDK response first, and the
    /// adaptation runs on top of that.
    void copyThenAdapt()
    {
        for (const auto & [name, value] : poco)
            sdk.AddHeader(name, value);
        applyGcsConditionalDialectToResponse(poco, sdk);
    }
};

}

/// This test and `ResponseMetadataPrefixIsMapped` look redundant and are not: only this one can
/// catch an install that fails to REPLACE. The copy loop above has already put the server's `etag` on
/// the response, so a wrong `AddHeader` overload — the `Aws::String &&` one emplaces instead of
/// assigning — leaves the server value standing and the substitution silently does nothing. No
/// `x-amz-meta-*` key is pre-occupied, so the metadata test would insert successfully either way and
/// stay green. Do not delete this as a duplicate.
TEST(GCSConditionalDialect, ResponseGenerationOverridesETag)
{
    ResponseFixture f;
    f.poco.set("ETag", "\"6654c734ccab8f440ff0825eb443dc7f\"");
    f.poco.set("x-goog-generation", "1783078552147137");
    f.copyThenAdapt();
    EXPECT_EQ(f.sdk.GetHeader("etag"), "\"1783078552147137\"");
}

TEST(GCSConditionalDialect, ResponseWithoutGenerationKeepsETag)
{
    ResponseFixture f;
    f.poco.set("ETag", "\"abc\"");
    f.copyThenAdapt();
    EXPECT_EQ(f.sdk.GetHeader("etag"), "\"abc\"");
}

/// This one stays despite `IOTestAwsS3Client.ResponseGenerationAndMetadataAdaptedOnlyWhenMarked`
/// covering the same mapping end to end: that test drives a whole client, so it can only report that
/// the mapping is absent, while this one localises the absence to the response adapter itself.
TEST(GCSConditionalDialect, ResponseMetadataPrefixIsMapped)
{
    ResponseFixture f;
    f.poco.set("x-goog-generation", "42");
    f.poco.set("x-goog-meta-cas-envelope", "v1");
    f.copyThenAdapt();
    EXPECT_EQ(f.sdk.GetHeader("x-amz-meta-cas-envelope"), "v1");
}

TEST(GCSConditionalDialect, ConflictingResponseMetadataRejected)
{
    ResponseFixture f;
    f.poco.set("x-goog-meta-cas-envelope", "v1");
    f.poco.set("x-amz-meta-cas-envelope", "v2");
    EXPECT_THROW(f.copyThenAdapt(), DB::Exception);
}

TEST(GCSConditionalDialect, AgreeingResponseMetadataAccepted)
{
    ResponseFixture f;
    f.poco.set("x-goog-meta-cas-envelope", "v1");
    f.poco.set("x-amz-meta-cas-envelope", "v1");
    EXPECT_NO_THROW(f.copyThenAdapt());
    EXPECT_EQ(f.sdk.GetHeader("x-amz-meta-cas-envelope"), "v1");
}
#endif
