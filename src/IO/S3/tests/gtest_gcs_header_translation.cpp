#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/Requests.h>

namespace
{

std::string headerOrEmpty(const Aws::Http::HeaderValueCollection & headers, const std::string & name)
{
    auto it = headers.find(name);
    return it == headers.end() ? "" : it->second;
}

}

/// The names GCS spells differently are renamed, and the `x-amz-` spelling does not survive: GCS
/// rejects a request mixing the two prefixes.
TEST(GCSHeaderTranslation, RenamesTheTranslatedNames)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-copy-source", "bucket/key"},
        {"x-amz-metadata-directive", "REPLACE"},
        {"x-amz-storage-class", "COLDLINE"},
        {"x-amz-meta-owner", "analytics"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-goog-copy-source"), "bucket/key");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-metadata-directive"), "REPLACE");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-storage-class"), "COLDLINE");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-meta-owner"), "analytics");

    EXPECT_EQ(translated.count("x-amz-copy-source"), 0u);
    EXPECT_EQ(translated.count("x-amz-metadata-directive"), 0u);
    EXPECT_EQ(translated.count("x-amz-storage-class"), 0u);
    EXPECT_EQ(translated.count("x-amz-meta-owner"), 0u);
}

/// Everything outside the list is left alone. Server-side encryption is the case that matters:
/// GCS spells CMEK as a single `x-goog-encryption-kms-key-name` with a different value, so renaming
/// the prefix would produce a header GCS ignores just as silently as the one it replaced.
TEST(GCSHeaderTranslation, LeavesEverythingElseAlone)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-server-side-encryption", "aws:kms"},
        {"x-amz-server-side-encryption-aws-kms-key-id", "some-key"},
        {"x-amz-request-payer", "requester"},
        {"content-type", "binary/octet-stream"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-amz-server-side-encryption"), "aws:kms");
    EXPECT_EQ(headerOrEmpty(translated, "x-amz-server-side-encryption-aws-kms-key-id"), "some-key");
    EXPECT_EQ(headerOrEmpty(translated, "x-amz-request-payer"), "requester");
    EXPECT_EQ(headerOrEmpty(translated, "content-type"), "binary/octet-stream");
}

/// `x-amz-metadata-directive` begins with `x-amz-meta` but is not custom metadata. The prefix that
/// selects metadata carries a trailing hyphen, so it must not be swept up by the family rule.
TEST(GCSHeaderTranslation, MetadataDirectiveIsNotCustomMetadata)
{
    Aws::Http::HeaderValueCollection headers{{"x-amz-metadata-directive", "COPY"}};

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-goog-metadata-directive"), "COPY");
    EXPECT_EQ(translated.count("x-goog-meta-data-directive"), 0u);
}

/// `x-amz-api-version` has no GCS counterpart and some GCS requests reject it, so it is dropped
/// rather than renamed. The SDK's own `amz-sdk-*` headers carry no `x-amz-` prefix and stay.
TEST(GCSHeaderTranslation, DropsHeadersWithNoCounterpart)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-api-version", "2006-03-01"},
        {"amz-sdk-invocation-id", "some-id"},
        {"amz-sdk-request", "attempt=1; max=3"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(translated.count("x-amz-api-version"), 0u);
    EXPECT_EQ(translated.count("x-goog-api-version"), 0u);
    EXPECT_EQ(headerOrEmpty(translated, "amz-sdk-invocation-id"), "some-id");
    EXPECT_EQ(headerOrEmpty(translated, "amz-sdk-request"), "attempt=1; max=3");
}

/// The response side recognises exactly what the request side renames, so the two cannot drift.
TEST(GCSHeaderTranslation, RecognisesTheSameNamesComingBack)
{
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-meta-owner"), "x-amz-meta-owner");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-storage-class"), "x-amz-storage-class");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-copy-source"), "x-amz-copy-source");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-metadata-directive"), "x-amz-metadata-directive");

    /// HTTP/1.1 preserves the case a server sent. The whole name comes back lower-cased, because the
    /// SDK makes the part after the prefix a key in a case-sensitive map.
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("X-Goog-Meta-Owner"), "x-amz-meta-owner");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("X-GOOG-META-CLICKHOUSE-IDEMPOTENCY-ID"), "x-amz-meta-clickhouse-idempotency-id");

    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-generation").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-hash").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("etag").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-meta-").has_value());
}

/// A mixed-case name would otherwise be classified as an ordinary header by both `x-amz-` checks,
/// attached to the request after signing, and never reach the rename.
TEST(GCSHeaderTranslation, NormalizesHeaderNames)
{
    DB::HTTPHeaderEntries headers{
        {"X-Amz-Meta-Owner", "analytics"},
        {"X-AMZ-STORAGE-CLASS", "GLACIER"},
        {"Custom-Auth-Token", "KeepTheValue"},
    };

    DB::S3::normalizeHeaderNames(headers);

    EXPECT_EQ(headers[0].name, "x-amz-meta-owner");
    EXPECT_EQ(headers[1].name, "x-amz-storage-class");
    EXPECT_EQ(headers[2].name, "custom-auth-token");
    /// Values are untouched.
    EXPECT_EQ(headers[2].value, "KeepTheValue");
}

#endif
