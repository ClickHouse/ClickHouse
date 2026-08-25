#include "config.h"
#if USE_AWS_S3
#include <gtest/gtest.h>
#include <IO/S3/GCSConditionalDialect.h>
#include <IO/S3/GOOG4Signer.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/auth/AWSCredentials.h>

using namespace DB::S3;

static std::chrono::system_clock::time_point fixedNow()
{
    /// 2026-07-03 00:00:00 UTC
    return std::chrono::system_clock::from_time_t(1783036800);
}

TEST(GOOG4Signer, PutWithGenerationPrecondition)
{
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dir/obj.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    request.SetHeaderValue("host", "storage.googleapis.com");
    request.SetHeaderValue("x-goog-if-generation-match", "0");

    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    EXPECT_EQ(request.GetHeaderValue("x-goog-date"), "20260703T000000Z");
    EXPECT_EQ(request.GetHeaderValue("x-goog-content-sha256"), "UNSIGNED-PAYLOAD");
    EXPECT_EQ(request.GetHeaderValue("authorization"),
        "GOOG4-HMAC-SHA256 Credential=GOOGTESTACCESSKEY/20260703/auto/storage/goog4_request, "
        "SignedHeaders=host;x-goog-content-sha256;x-goog-date;x-goog-if-generation-match, "
        "Signature=4f82e49c69753329afd4768ccf1db6b472dbbd86d082a08b5b9f9fe368fb6ef6");
}

TEST(GOOG4Signer, GetWithQueryString)
{
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/?versioning"), Aws::Http::HttpMethod::HTTP_GET);
    request.SetHeaderValue("host", "storage.googleapis.com");

    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    EXPECT_EQ(request.GetHeaderValue("authorization"),
        "GOOG4-HMAC-SHA256 Credential=GOOGTESTACCESSKEY/20260703/auto/storage/goog4_request, "
        "SignedHeaders=host;x-goog-content-sha256;x-goog-date, "
        "Signature=28a981c32acff334738b9ea1a0f82c28c9a1ccff5b6dc8fb92a2e6622c8db73f");
}

TEST(GOOG4Signer, NonGoogHeadersAreNotSigned)
{
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dir/obj.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    request.SetHeaderValue("host", "storage.googleapis.com");
    request.SetHeaderValue("x-goog-if-generation-match", "0");
    request.SetHeaderValue("content-type", "binary/octet-stream");
    request.SetHeaderValue("amz-sdk-invocation-id", "whatever");

    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    /// Unsigned headers must not perturb the signature: same vector as PutWithGenerationPrecondition.
    EXPECT_NE(request.GetHeaderValue("authorization").find(
        "Signature=4f82e49c69753329afd4768ccf1db6b472dbbd86d082a08b5b9f9fe368fb6ef6"), std::string::npos);
}

TEST(GOOG4Signer, NothingAmzPrefixedSurvivesIntoTheSignature)
{
    /// The composition the GOOG4 client performs: authentication preparation first, then signing.
    /// GCS rejects a request that mixes the prefixes, so after preparation the canonical request must
    /// contain no `x-amz-*` header at all — and the request itself must carry none either.
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dir/obj.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    request.SetHeaderValue("host", "storage.googleapis.com");
    request.SetHeaderValue("x-goog-if-generation-match", "0");
    request.SetHeaderValue("authorization", "AWS4-HMAC-SHA256 ...");
    request.SetHeaderValue("x-amz-date", "20260703T000000Z");
    request.SetHeaderValue("x-amz-content-sha256", "deadbeef");
    request.SetHeaderValue("x-amz-meta-foo", "bar");
    request.SetHeaderValue("x-amz-storage-class", "STANDARD");
    request.SetHeaderValue("x-amz-sdk-checksum-algorithm", "CRC32");

    prepareGcsRequestForGoog4Authentication(request);
    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    for (const auto & [name, value] : request.GetHeaders())
        EXPECT_FALSE(name.starts_with("x-amz-")) << name;

    const auto authorization = request.GetHeaderValue("authorization");
    EXPECT_EQ(authorization.find("x-amz-"), std::string::npos) << authorization;
    /// The surviving x-goog- headers ARE signed, so the preparation did not simply drop everything.
    EXPECT_NE(authorization.find("x-goog-meta-foo"), std::string::npos) << authorization;
    EXPECT_NE(authorization.find("x-goog-storage-class"), std::string::npos) << authorization;
}

TEST(GOOG4Signer, DefaultPutHasNoGenerationPreconditionInTheSignature)
{
    /// The `Default`-mode counterpart of `PutWithGenerationPrecondition`: an ordinary (non-CAS) write
    /// through `gcs_hmac` never acquires `x-goog-if-generation-match` at all, so GOOG4 authentication
    /// must sign it the same way it would sign any other GOOG4 PUT, with that header simply absent from
    /// `SignedHeaders` -- not replaced by some other precondition, not rejected.
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dir/obj.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    request.SetHeaderValue("host", "storage.googleapis.com");

    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    /// Compare against a request signed the same way but WITH the precondition set: identical
    /// SignedHeaders/Credential scope apart from the one header, proving the precondition's absence (not
    /// some other divergence) is what changes between the two -- the same fixed key/time/path as
    /// `PutWithGenerationPrecondition` isolates that one variable.
    Aws::Http::Standard::StandardHttpRequest with_precondition(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dir/obj.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    with_precondition.SetHeaderValue("host", "storage.googleapis.com");
    with_precondition.SetHeaderValue("x-goog-if-generation-match", "0");
    signRequestGOOG4(with_precondition, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    const auto authorization = request.GetHeaderValue("authorization");
    EXPECT_EQ(authorization.find("x-goog-if-generation-match"), std::string::npos) << authorization;
    EXPECT_NE(authorization.find("SignedHeaders=host;x-goog-content-sha256;x-goog-date,"), std::string::npos) << authorization;
    EXPECT_NE(authorization, with_precondition.GetHeaderValue("authorization"));
}

TEST(GOOG4Signer, CopyObjectHeadersAreRenamedAndSignedAsGoogPrefixed)
{
    /// The other deferred shape from Task 4/5: a CopyObject request, prepared for GOOG4 the same way
    /// `PocoHTTPClientGCSHMAC::makeRequestInternal` does it before signing. `x-amz-copy-source` and
    /// `x-amz-metadata-directive` have their own `Rename` rule in `GOOG4_HEADER_RULES` distinct from the
    /// storage-class/meta-* one `NothingAmzPrefixedSurvivesIntoTheSignature` already covers, so nothing
    /// existing exercised them until now.
    Aws::Http::Standard::StandardHttpRequest request(
        Aws::Http::URI("https://storage.googleapis.com/test-bucket/dest.txt"), Aws::Http::HttpMethod::HTTP_PUT);
    request.SetHeaderValue("host", "storage.googleapis.com");
    request.SetHeaderValue("x-amz-copy-source", "test-bucket/src.txt");
    request.SetHeaderValue("x-amz-metadata-directive", "REPLACE");
    request.SetHeaderValue("x-amz-meta-cas-envelope", "v1");

    prepareGcsRequestForGoog4Authentication(request);
    signRequestGOOG4(request, Aws::Auth::AWSCredentials("GOOGTESTACCESSKEY", "testsecretkey"), fixedNow());

    for (const auto & [name, value] : request.GetHeaders())
        EXPECT_FALSE(name.starts_with("x-amz-")) << name;

    EXPECT_EQ(request.GetHeaderValue("x-goog-copy-source"), "test-bucket/src.txt");
    EXPECT_EQ(request.GetHeaderValue("x-goog-metadata-directive"), "REPLACE");
    EXPECT_EQ(request.GetHeaderValue("x-goog-meta-cas-envelope"), "v1");

    const auto authorization = request.GetHeaderValue("authorization");
    EXPECT_EQ(authorization.find("x-amz-"), std::string::npos) << authorization;
    EXPECT_NE(authorization.find("x-goog-copy-source"), std::string::npos) << authorization;
    EXPECT_NE(authorization.find("x-goog-metadata-directive"), std::string::npos) << authorization;
    EXPECT_NE(authorization.find("x-goog-meta-cas-envelope"), std::string::npos) << authorization;
}
#endif
