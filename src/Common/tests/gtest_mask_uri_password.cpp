#include <gtest/gtest.h>

#include <Common/maskURIPassword.h>
#include <Common/re2.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The regular expression `maskURIPassword` used to be. Kept here so that the hand-written scan
/// can be checked against it: this is masking of secrets, so "close enough" is not good enough.
bool maskURIPasswordWithRE2(std::string * uri)
{
    return RE2::Replace(uri, R"(([^:]+://[^:]*):([^@]*)@(.*))", "\\1:[HIDDEN]@\\3");
}

const std::vector<std::string> corpus = {
    /// Ordinary URIs with credentials.
    "http://user:password@host",
    "https://user:password@host:8080/path?query=1",
    "amqp://guest:guest@localhost:5672/",
    "nats://u:p@127.0.0.1:4222",
    "postgres://user:p@ss@host/db",
    "mysql://root:@host",
    "scheme://:password@host",

    /// No credentials to mask.
    "http://host",
    "http://host/path",
    "http://user@host",
    "just some text",
    "",
    "://",
    ":",
    "a://b",

    /// The scheme must be non-empty and colon-free, so these do not match at the first "://".
    "://user:password@host",
    ":://user:password@host",
    "x:://user:password@host",

    /// A second "://" can match when the first one cannot.
    "://nope http://user:password@host",
    "http://host ftp://user:password@elsewhere",

    /// Colons and at signs in unusual places.
    "http://user:pass:word@host",
    "http://user:password@host@another",
    "http://a:b@c://d:e@f",
    "@:://",
    "http://user:password",
    "user:password@host",

    /// Newlines: `[^:]` matches them but `.` does not, which only affects the tail.
    "http://user:password@host\nsecond line",
    "line one\nhttp://user:password@host",
};

}

TEST(MaskURIPassword, MatchesTheRegularExpressionItReplaced)
{
    for (const auto & input : corpus)
    {
        std::string with_scan = input;
        std::string with_re2 = input;

        bool scanned = maskURIPassword(&with_scan);
        bool matched = maskURIPasswordWithRE2(&with_re2);

        EXPECT_EQ(scanned, matched) << "return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "result differs for: " << input;
    }
}

TEST(MaskURIPassword, AgreesWithTheRegularExpressionOnRandomStrings)
{
    /// The fixed corpus above documents the interesting cases; this is the safety net for the ones
    /// nobody thought of. The alphabet is the characters the regular expression assigns meaning to.
    constexpr std::string_view alphabet = "ab:@/.\n";
    std::mt19937_64 rng(20260731); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible
    std::uniform_int_distribution<size_t> length_dist(0, 32);
    std::uniform_int_distribution<size_t> char_dist(0, alphabet.size() - 1);

    for (size_t round = 0; round < 200000; ++round)
    {
        std::string input(length_dist(rng), ' ');
        for (auto & c : input)
            c = alphabet[char_dist(rng)];

        std::string with_scan = input;
        std::string with_re2 = input;

        bool scanned = maskURIPassword(&with_scan);
        bool matched = maskURIPasswordWithRE2(&with_re2);

        ASSERT_EQ(scanned, matched) << "return value differs for: " << input;
        ASSERT_EQ(with_scan, with_re2) << "result differs for: " << input;
    }
}

TEST(MaskURIPassword, MasksThePassword)
{
    std::string uri = "https://user:hunter2@example.com:443/db";
    EXPECT_TRUE(maskURIPassword(&uri));
    EXPECT_EQ(uri, "https://user:[HIDDEN]@example.com:443/db");
}

TEST(MaskURIPassword, LeavesURIsWithoutAPasswordAlone)
{
    std::string uri = "https://example.com/db";
    EXPECT_FALSE(maskURIPassword(&uri));
    EXPECT_EQ(uri, "https://example.com/db");
}


namespace
{

/// The regular expressions `maskURIUserinfo` and `maskPresignedURLParameters` used to be.
bool maskURIUserinfoWithRE2(std::string & url)
{
    return RE2::Replace(&url, R"(^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@)", "\\1[HIDDEN]@");
}

bool maskPresignedURLParametersWithRE2(std::string & url)
{
    return RE2::GlobalReplace(
        &url,
        R"(([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|X-Amz-[A-Za-z0-9\-]*|X-Goog-[A-Za-z0-9\-]*)=)[^&#]*)",
        "\\1[HIDDEN]");
}

const std::vector<std::string> url_corpus = {
    /// Userinfo.
    "https://user:password@bucket.s3.amazonaws.com/key",
    "https://user@bucket/key",
    "https://user:pass@word@bucket/key",
    "s3://key:secret@bucket/path?query=1",
    "s3+http://key:secret@bucket/path",
    "https://bucket/key",
    "https://bucket/user:pass@key",
    "https://bucket/key?redirect=http://u:p@elsewhere",
    "https://@bucket/key",
    "://user:password@bucket",
    "1https://user:password@bucket",
    "-https://user:password@bucket",
    "https:/user:password@bucket",
    "user:password@bucket",
    "https://user:password@bucket?a=b@c",
    "https://user#:password@bucket",
    "https://user?:password@bucket",

    /// Presigned parameters.
    "https://bucket/key?AWSAccessKeyId=AKIA&Signature=abc&Expires=1",
    "https://bucket/key?X-Amz-Signature=abc&X-Amz-Credential=def&list-type=2",
    "https://bucket/key?GoogleAccessId=x&X-Goog-Signature=y",
    "https://bucket/key?X-Amz-=empty",
    "https://bucket/key?X-Amz-Bad_Name=visible",
    "https://bucket/key?signature=lowercase",
    "https://bucket/key?xSignature=notamatch",
    "https://bucket/key?Signature",
    "https://bucket/key?Signature=",
    "https://bucket/key?Signature=abc#fragment",
    "https://bucket/key?a=1&Signature=abc",
    "https://bucket/key&Signature=abc",
    "https://bucket/key?Signature=a&Signature=b",
    "https://bucket/key?Expires=1&Expires=2&Expires=3",
    "https://bucket/key?Signature=a=b&next=1",

    /// Both at once, and neither.
    "https://user:password@bucket/key?X-Amz-Signature=abc&format=CSV",
    "",
    "?&=",
    "https://",
};

}

TEST(MaskS3URLCredentials, MatchTheRegularExpressionsTheyReplaced)
{
    for (const auto & input : url_corpus)
    {
        std::string with_scan = input;
        std::string with_re2 = input;

        EXPECT_EQ(maskURIUserinfo(with_scan), maskURIUserinfoWithRE2(with_re2)) << "userinfo return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "userinfo result differs for: " << input;

        EXPECT_EQ(maskPresignedURLParameters(with_scan), maskPresignedURLParametersWithRE2(with_re2))
            << "presign return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "presign result differs for: " << input;
    }
}

TEST(MaskS3URLCredentials, AgreeWithTheRegularExpressionsOnRandomStrings)
{
    /// Random characters would almost never spell a presigned parameter name, so the strings are
    /// built from the tokens the two regular expressions care about.
    const std::vector<std::string> tokens = {
        "https://", "s3://", "1://", "-x://", ":/", "user", "pass", ":", "@", "/", "?", "&", "=", "#",
        "Signature", "AWSAccessKeyId", "Expires", "GoogleAccessId", "X-Amz-", "X-Goog-", "Credential", "_", "a", "\n", "",
    };
    std::mt19937_64 rng(20260731); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible
    std::uniform_int_distribution<size_t> count_dist(0, 12);
    std::uniform_int_distribution<size_t> token_dist(0, tokens.size() - 1);

    /// Fewer rounds than for `maskURIPassword`: each round runs two scans and two regular
    /// expressions, and `RE2::GlobalReplace` dominates - sanitizer builds multiply that.
    for (size_t round = 0; round < 50000; ++round)
    {
        std::string input;
        for (size_t i = 0, count = count_dist(rng); i < count; ++i)
            input += tokens[token_dist(rng)];

        std::string with_scan = input;
        std::string with_re2 = input;

        ASSERT_EQ(maskURIUserinfo(with_scan), maskURIUserinfoWithRE2(with_re2)) << "userinfo return value differs for: " << input;
        ASSERT_EQ(with_scan, with_re2) << "userinfo result differs for: " << input;

        ASSERT_EQ(maskPresignedURLParameters(with_scan), maskPresignedURLParametersWithRE2(with_re2))
            << "presign return value differs for: " << input;
        ASSERT_EQ(with_scan, with_re2) << "presign result differs for: " << input;
    }
}

TEST(MaskS3URLCredentials, MasksUserinfoAndPresignedParameters)
{
    std::string url = "https://key:secret@bucket/path?X-Amz-Signature=abcdef&format=CSV";
    EXPECT_TRUE(maskURIUserinfo(url));
    EXPECT_TRUE(maskPresignedURLParameters(url));
    EXPECT_EQ(url, "https://[HIDDEN]@bucket/path?X-Amz-Signature=[HIDDEN]&format=CSV");
}
