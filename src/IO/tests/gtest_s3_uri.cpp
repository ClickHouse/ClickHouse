#include <gtest/gtest.h>
#include "config.h"

#if USE_AWS_S3

#include <IO/S3Common.h>

namespace
{
using namespace DB;

struct TestCase
{
    S3::URI uri;
    String endpoint;
    String bucket;
    String key;
    String version_id;
    bool is_virtual_hosted_style;
};

const TestCase TestCases[] = {
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&secondKey=anotherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data?firstKey=someKey&secondKey=anotherKey",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId&anotherKey=someOtherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&versionId=testVersionId&anotherKey=someOtherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?anotherKey=someOtherKey&versionId=testVersionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId="),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId&"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com",
     "root",
     "nested/file.txt",
     "",
     false},
    // Test with a file with no extension
    {S3::URI("https://bucket.vpce-03b2c987f1bd55c5f-j3b4vg7w.s3.ap-southeast-2.vpce.amazonaws.com/some_bucket/document"),
     "https://bucket.vpce-03b2c987f1bd55c5f-j3b4vg7w.s3.ap-southeast-2.vpce.amazonaws.com",
     "some_bucket",
     "document",
     "",
     false},
    // Test with a deeply nested file path
    {S3::URI("https://bucket.vpce-0242cd56f1bd55c5f-l5b7vg8x.s3.sa-east-1.vpce.amazonaws.com/some_bucket/b/c/d/e/f/g/h/i/j/data.json"),
     "https://bucket.vpce-0242cd56f1bd55c5f-l5b7vg8x.s3.sa-east-1.vpce.amazonaws.com",
     "some_bucket",
     "b/c/d/e/f/g/h/i/j/data.json",
     "",
     false},
    // Zonal
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com",
     "root",
     "nested/file.txt",
     "",
     false},
    // Non standard port
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com:65535/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com:65535",
     "root",
     "nested/file.txt",
     "",
     false},
};

class S3UriTest : public testing::TestWithParam<std::string>
{
};

TEST(S3UriTest, validPatterns)
{
    {
        S3::URI uri("https://jokserfn.s3.amazonaws.com/");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3.amazonaws.com/jokserfn/");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://amazonaws.com/bucket/");
        ASSERT_EQ("https://amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucket", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://jokserfn.s3.amazonaws.com/data");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://storage.amazonaws.com/jokserfn/data");
        ASSERT_EQ("https://storage.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.cos.ap-beijing.myqcloud.com/data");
        ASSERT_EQ("https://cos.ap-beijing.myqcloud.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.s3.us-east-2.amazonaws.com/data");
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3.us-east-2.amazonaws.com/bucketname/data");
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.s3-us-east-2.amazonaws.com/data");
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.dots-are-allowed.s3-us-east-2.amazonaws.com/data");
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname.dots-are-allowed", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3-us-east-2.amazonaws.com/bucketname/data");
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://test-perf-bucket--eun1-az1--x-s3.s3express-eun1-az1.eu-north-1.amazonaws.com/test.csv");
        ASSERT_EQ("https://s3express-eun1-az1.eu-north-1.amazonaws.com", uri.endpoint);
        ASSERT_EQ("test-perf-bucket--eun1-az1--x-s3", uri.bucket);
        ASSERT_EQ("test.csv", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucket-test1.oss-cn-beijing-internal.aliyuncs.com/ab-test");
        ASSERT_EQ("https://oss-cn-beijing-internal.aliyuncs.com", uri.endpoint);
        ASSERT_EQ("bucket-test1", uri.bucket);
        ASSERT_EQ("ab-test", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucket-test.cn-beijing-internal.oss-data-acc.aliyuncs.com/ab-test");
        ASSERT_EQ("https://cn-beijing-internal.oss-data-acc.aliyuncs.com", uri.endpoint);
        ASSERT_EQ("bucket-test", uri.bucket);
        ASSERT_EQ("ab-test", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://some-bucket-name123.yet.another.object-storage.com/a/b/c", false, true, S3UriStyle::VIRTUAL_HOSTED);
        ASSERT_EQ("https://yet.another.object-storage.com", uri.endpoint);
        ASSERT_EQ("some-bucket-name123", uri.bucket);
        ASSERT_EQ("a/b/c", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://strage-prefix.s3.yet.another.object-storage.com/bucket-name123/a/b/c", false, true, S3UriStyle::PATH);
        ASSERT_EQ("https://strage-prefix.s3.yet.another.object-storage.com", uri.endpoint);
        ASSERT_EQ("bucket-name123", uri.bucket);
        ASSERT_EQ("a/b/c", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
}

TEST(S3UriTest, GcsV2PresignedQueryNotFolded)
{
    using namespace DB;
    S3::URI uri("https://storage.googleapis.com/test-bucket/path/to/object.txt?GoogleAccessId=x&Expires=1&Signature=x");

    ASSERT_EQ("https://storage.googleapis.com", uri.endpoint);
    ASSERT_EQ("test-bucket", uri.bucket);
    ASSERT_EQ("path/to/object.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, GcsV4PresignedQueryNotFolded)
{
    using namespace DB;
    S3::URI uri("https://storage.googleapis.com/test-bucket/obj.txt?X-Goog-Algorithm=x&X-Goog-Credential=x&X-Goog-Date=x&X-Goog-Expires=x&X-Goog-SignedHeaders=x&X-Goog-Signature=x");

    ASSERT_EQ("https://storage.googleapis.com", uri.endpoint);
    ASSERT_EQ("test-bucket", uri.bucket);
    ASSERT_EQ("obj.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, AwsV4PresignedQueryNotFolded)
{
    using namespace DB;
    S3::URI uri("https://bucketname.s3.amazonaws.com/path/to/object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x%2F20250101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=x");

    ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
    ASSERT_EQ("bucketname", uri.bucket);
    ASSERT_EQ("path/to/object.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_TRUE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, AwsV4PresignedQueryFoldedInCompatibilityMode)
{
    using namespace DB;
    S3::URI uri(
        "https://bucketname.s3.amazonaws.com/data?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20250101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=x",
        /*allow_archive_path_syntax*/ false,
        /*keep_presigned_query_parameters*/ false);

    ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
    ASSERT_EQ("bucketname", uri.bucket);
    ASSERT_EQ("data?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20250101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=x", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_TRUE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, AwsV4PresignedQueryNotFoldedWithExplicitPathStyle)
{
    using namespace DB;
    S3::URI uri(
        "https://minio1:9001/bucket/path/to/object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20250101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=x",
        /*allow_archive_path_syntax*/ false,
        /*keep_presigned_query_parameters*/ true,
        /*uri_style*/ S3UriStyle::PATH);

    ASSERT_EQ("https://minio1:9001", uri.endpoint);
    ASSERT_EQ("bucket", uri.bucket);
    ASSERT_EQ("path/to/object.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, AwsV4PresignedQueryNotFoldedWithExplicitVirtualHostedStyle)
{
    using namespace DB;
    S3::URI uri(
        "https://bucketname.s3.amazonaws.com/path/to/object.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20250101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=x",
        /*allow_archive_path_syntax*/ false,
        /*keep_presigned_query_parameters*/ true,
        /*uri_style*/ S3UriStyle::VIRTUAL_HOSTED);

    ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
    ASSERT_EQ("bucketname", uri.bucket);
    ASSERT_EQ("path/to/object.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_TRUE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, GcsPresignedQueryNotFoldedWithExplicitPathStyle)
{
    using namespace DB;
    S3::URI uri(
        "https://storage.googleapis.com/test-bucket/path/to/object.txt?GoogleAccessId=x&Expires=1&Signature=x",
        /*allow_archive_path_syntax*/ false,
        /*keep_presigned_query_parameters*/ true,
        /*uri_style*/ S3UriStyle::PATH);

    ASSERT_EQ("https://storage.googleapis.com", uri.endpoint);
    ASSERT_EQ("test-bucket", uri.bucket);
    ASSERT_EQ("path/to/object.txt", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, versionIdChecks)
{
    for (const auto& test_case : TestCases)
    {
        ASSERT_EQ(test_case.endpoint, test_case.uri.endpoint);
        ASSERT_EQ(test_case.bucket, test_case.uri.bucket);
        ASSERT_EQ(test_case.key, test_case.uri.key);
        ASSERT_EQ(test_case.version_id, test_case.uri.version_id);
        ASSERT_EQ(test_case.is_virtual_hosted_style, test_case.uri.is_virtual_hosted_style);
    }
}

TEST(S3UriTest, WildcardQuestionMarksCustomEndpoint)
{
    // Custom endpoint (like MinIO) with bucket in the first path segment and
    // wildcard '??' in the path. We ensure the bucket and key are parsed correctly
    // and that '?' from the path is preserved in the key (folded from query),
    // while query is cleared and versionId remains empty.
    using namespace DB;
    S3::URI uri("http://minio1:9001/root/data/wildcard_test_??.tsv.gz");

    ASSERT_EQ("http://minio1:9001", uri.endpoint);
    ASSERT_EQ("root", uri.bucket);
    ASSERT_EQ("data/wildcard_test_??.tsv.gz", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3UriTest, TrailingQuestionMarkWildcard)
{
    using namespace DB;
    S3::URI uri("http://minio1:9001/root/data/wildcard_end_?");

    ASSERT_EQ("http://minio1:9001", uri.endpoint);
    ASSERT_EQ("root", uri.bucket);
    ASSERT_EQ("data/wildcard_end_?", uri.key);
    ASSERT_EQ("", uri.version_id);
    ASSERT_FALSE(uri.is_virtual_hosted_style);
}

TEST(S3CommonTest, SanitizeAwsArnsInErrorMessages)
{
    using namespace DB;

    /// Passthrough when no ARN is present.
    ASSERT_EQ("AccessDenied", sanitizeS3ErrorMessage("AccessDenied"));

    /// Empty input.
    ASSERT_EQ("", sanitizeS3ErrorMessage(""));

    /// `arn:` inside another word is not an AWS ARN token.
    ASSERT_EQ("warning: warn: keep unchanged", sanitizeS3ErrorMessage("warning: warn: keep unchanged"));

    /// Single ARN in the middle of a message.
    ASSERT_EQ(
        "User: [REDACTED_AWS_ARN] is not authorized",
        sanitizeS3ErrorMessage("User: arn:aws:sts::123456789012:assumed-role/ClickHouseRole/session is not authorized"));

    /// ARN at the start of the message.
    ASSERT_EQ(
        "[REDACTED_AWS_ARN] is not authorized",
        sanitizeS3ErrorMessage("arn:aws:iam::123456789012:role/Admin is not authorized"));

    /// ARN at the end of the message.
    ASSERT_EQ(
        "User: [REDACTED_AWS_ARN]",
        sanitizeS3ErrorMessage("User: arn:aws:iam::123456789012:role/Admin"));

    /// Multiple ARNs.
    ASSERT_EQ(
        "[REDACTED_AWS_ARN] -> [REDACTED_AWS_ARN]",
        sanitizeS3ErrorMessage("arn:aws:iam::111111111111:role/A -> arn:aws:iam::222222222222:role/B"));

    /// `arn:` alone is still redacted.
    ASSERT_EQ("[REDACTED_AWS_ARN]", sanitizeS3ErrorMessage("arn:"));
}

TEST(S3CommonTest, SanitizePreformattedMessage)
{
    using namespace DB;

    /// `sanitizeS3PreformattedMessage` must redact ARNs in both `text` and
    /// every element of `format_string_args` (these get propagated into
    /// structured logs / telemetry), and must NOT touch `format_string`.
    PreformattedMessage msg{
        .text = "User: arn:aws:iam::123456789012:role/Admin is not authorized to use arn:aws:iam::999:role/Other",
        .format_string = "User: {} is not authorized to use {}",
        .format_string_args = {
            "arn:aws:iam::123456789012:role/Admin",
            "arn:aws:iam::999:role/Other",
        },
    };

    PreformattedMessage sanitized = sanitizeS3PreformattedMessage(std::move(msg));

    ASSERT_EQ(
        "User: [REDACTED_AWS_ARN] is not authorized to use [REDACTED_AWS_ARN]",
        sanitized.text);
    ASSERT_EQ("User: {} is not authorized to use {}", sanitized.format_string);
    ASSERT_EQ(2u, sanitized.format_string_args.size());
    ASSERT_EQ("[REDACTED_AWS_ARN]", sanitized.format_string_args[0]);
    ASSERT_EQ("[REDACTED_AWS_ARN]", sanitized.format_string_args[1]);

    /// Args that don't contain ARNs are passed through unchanged.
    PreformattedMessage no_arn{
        .text = "AccessDenied: bucket nonexistent",
        .format_string = "{}: bucket {}",
        .format_string_args = {"AccessDenied", "nonexistent"},
    };
    PreformattedMessage no_arn_sanitized = sanitizeS3PreformattedMessage(std::move(no_arn));
    ASSERT_EQ("AccessDenied: bucket nonexistent", no_arn_sanitized.text);
    ASSERT_EQ("AccessDenied", no_arn_sanitized.format_string_args[0]);
    ASSERT_EQ("nonexistent", no_arn_sanitized.format_string_args[1]);
}

}
#endif
