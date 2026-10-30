#include <gtest/gtest.h>

#include <Common/maskURIPassword.h>
#include <Common/re2.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

TEST(MaskURIPassword, FollowsRFC3986AndFailsClosed)
{
    const std::vector<std::pair<std::string, std::string>> cases = {
        /// Ordinary URIs with credentials.
        {"http://user:password@host", "http://user:[HIDDEN]@host"},
        {"https://user:password@host:8080/path?query=1", "https://user:[HIDDEN]@host:8080/path?query=1"},
        {"amqp://guest:guest@localhost:5672/", "amqp://guest:[HIDDEN]@localhost:5672/"},
        {"mongodb://u:p@h1:27017,h2:27017/db?replicaSet=r", "mongodb://u:[HIDDEN]@h1:27017,h2:27017/db?replicaSet=r"},
        {"http://u:p@[::1]:8123/", "http://u:[HIDDEN]@[::1]:8123/"},
        {"http://user:pass:word@host", "http://user:[HIDDEN]@host"},
        {"http://user%3Aname:password@host", "http://user%3Aname:[HIDDEN]@host"},
        {"http://:password@host", "http://:[HIDDEN]@host"},
        {"mysql://root:@host", "mysql://root:[HIDDEN]@host"},

        /// A '@' after the authority is not the end of a userinfo.
        {"http://localhost:11111/x?email=a@b.com", "http://localhost:11111/x?email=a@b.com"},
        {"http://host/user:password@x", "http://host/user:password@x"},
        {"http://u:p@a/b@host", "http://u:[HIDDEN]@a/b@host"},
        {"http://host?email=a@b.com", "http://host?email=a@b.com"},

        /// No password: a userinfo without ':' is a user name, and an authority without '@' has no userinfo.
        {"http://user@host", "http://user@host"},
        {"http://host:8080", "http://host:8080"},
        {"http://[::1]:8123/path@x", "http://[::1]:8123/path@x"},
        {"file:///etc/passwd", "file:///etc/passwd"},
        {"http://user:password", "http://user:password"},
        {"user:password@host", "user:password@host"},
        {"just some text", "just some text"},
        {"", ""},
        {"://", "://"},

        /// Fails closed: a raw '@' in the password extends the userinfo to the last '@' of the authority.
        {"https://user:SEKRIT@PW@localhost:11111/x", "https://user:[HIDDEN]@localhost:11111/x"},
        {"http://user:password@host@another/x", "http://user:[HIDDEN]@another/x"},

        /// Fails closed: an authority that is not `host[:port]` was cut by a raw '/', '?' or '#' in the password.
        {"http://u:pa/ss@host/x", "http://u:[HIDDEN]@host/x"},
        {"http://u:pa?ss@host", "http://u:[HIDDEN]@host"},
        {"http://u:pa#ss@host", "http://u:[HIDDEN]@host"},
        {"http://u:12/ss@host", "http://u:12/ss@host"},
        {"http://u:pa/ss", "http://u:pa/ss"},

        /// Any `://` can start a URI, also inside a longer text.
        {"://user:password@host", "://user:[HIDDEN]@host"},
        {"line one\nhttp://user:password@host", "line one\nhttp://user:[HIDDEN]@host"},
        {"http://host/ ftp://user:password@elsewhere", "http://host/ ftp://user:[HIDDEN]@elsewhere"},
        /// Here the first authority is `host ftp:`, which is not `host[:port]`, so it fails closed.
        {"http://host ftp://user:password@elsewhere", "http://host ftp:[HIDDEN]@elsewhere"},
        {"https://host/key?redirect=http://u:p@elsewhere", "https://host/key?redirect=http://u:[HIDDEN]@elsewhere"},

        /// Fails closed by default: a consumer like the MongoDB driver ends the userinfo at the first '/' only.
        {"mongodb://usr:999999?TOPSECRET@localhost:27017/db", "mongodb://usr:[HIDDEN]@localhost:27017/db"},
        {"http://host#frag:x@y", "http://host#frag:[HIDDEN]@y"},
    };

    /// `Poco::URI` ends the authority where the RFC does, so a '@' after '?' or '#' is not in its userinfo.
    const std::vector<std::pair<std::string, std::string>> rfc_cases = {
        {"mongodb://usr:999999?TOPSECRET@localhost:27017/db", "mongodb://usr:999999?TOPSECRET@localhost:27017/db"},
        {"http://host#frag:x@y", "http://host#frag:x@y"},
        {"http://u:pa?ss@host", "http://u:[HIDDEN]@host"},
        {"https://user:SEKRIT@PW@localhost:11111/x", "https://user:[HIDDEN]@localhost:11111/x"},
    };

    for (const auto & [input, expected] : cases)
    {
        std::string uri = input;
        EXPECT_EQ(maskURIPassword(&uri), input != expected) << "return value for: " << input;
        EXPECT_EQ(uri, expected) << "result for: " << input;
    }

    for (const auto & [input, expected] : rfc_cases)
    {
        std::string uri = input;
        EXPECT_EQ(maskURIPassword(&uri, URI_AUTHORITY_TERMINATORS), input != expected) << "return value for: " << input;
        EXPECT_EQ(uri, expected) << "result for: " << input;
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
        R"(([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|sig|(?i:X-Amz-|X-Goog-)[^=?&]*)=)[^&#]*)",
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

    /// The prefixes are header names, so any case: a SigV2 presigned URL carries `x-amz-security-token`.
    "https://s3.amazonaws.com/b/k?AWSAccessKeyId=AKIA&Signature=abc&x-amz-security-token=tok&Expires=1",
    "https://h/f?X-AMZ-SECURITY-TOKEN=a",
    "https://h/f?x-goog-signature=a",
    "https://h/f?x-amz-=empty",
    "https://h/f?x-amz-any_name=hidden",

    /// An Azure shared access signature: `sig` is the signature, the other fields say what it grants.
    "https://acct.blob.core.windows.net/c/b?sp=r&sig=abc",
    "abfss://c@a.dfs.core.windows.net/d/?sp=r&sig=abc",
    "https://h/f?X-Amz-Signature=a&sig=b",
    "https://h/f?SIG=abc",
    "https://h/f?Sig=abc",
    "https://h/f?design=visible",
    "https://h/f?signed=visible",
    "https://h/f?sig",
    "https://h/f?sig=",
    "https://h/f?sig=a&sig=b",

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
        "sig", "SIG", "design", "x-amz-", "X-AMZ-", "x-Goog-",
    };
    std::mt19937_64 rng(20260731); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible
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

TEST(MaskS3URLCredentials, MasksPercentEncodedSecretParameterNames)
{
    /// Which of these spellings authenticates was measured against Azurite 3.35 with one valid
    /// shared access signature: the lower-case ones do, encoded or not, and the upper-case ones
    /// do not. The name itself is never rewritten, only its value, so the expectations keep it.
    struct Arm
    {
        std::string url;
        std::string expected;
    };

    const std::vector<Arm> arms = {
        {"https://h/f?%73ig=x", "https://h/f?%73ig=[HIDDEN]"},
        {"https://h/f?s%69g=x", "https://h/f?s%69g=[HIDDEN]"},
        {"https://h/f?%73%69%67=x", "https://h/f?%73%69%67=[HIDDEN]"},

        /// The decoding serves every name in the set, and the prefixes too.
        {"https://h/f?%53ignature=x", "https://h/f?%53ignature=[HIDDEN]"},
        {"https://h/f?%58-Amz-Signature=x", "https://h/f?%58-Amz-Signature=[HIDDEN]"},
        {"https://h/f?%78-amz-security-token=x", "https://h/f?%78-amz-security-token=[HIDDEN]"},

        /// Decodes to `SIG`, which does not authenticate, so it stays readable like `?SIG=` itself.
        {"https://h/f?%53IG=x", "https://h/f?%53IG=x"},
        /// The endpoint decodes once as well, so to it this name is `%73ig` rather than `sig`.
        {"https://h/f?%2573ig=x", "https://h/f?%2573ig=x"},
        /// A malformed and a truncated escape are copied through rather than throwing.
        {"https://h/f?%7Xig=x", "https://h/f?%7Xig=x"},
        {"https://h/f?%7=x", "https://h/f?%7=x"},
        /// `+` decodes to a space, so this name is not `sig`.
        {"https://h/f?s+ig=x", "https://h/f?s+ig=x"},
    };

    for (const auto & arm : arms)
    {
        std::string url = arm.url;
        EXPECT_EQ(maskPresignedURLParameters(url), arm.url != arm.expected) << "return value differs for: " << arm.url;
        EXPECT_EQ(url, arm.expected) << "result differs for: " << arm.url;
    }
}

TEST(MaskS3URLCredentials, MasksUserinfoAndPresignedParameters)
{
    std::string url = "https://key:secret@bucket/path?X-Amz-Signature=abcdef&sig=ghijkl&format=CSV";
    EXPECT_TRUE(maskURIUserinfo(url));
    EXPECT_TRUE(maskPresignedURLParameters(url));
    EXPECT_EQ(url, "https://[HIDDEN]@bucket/path?X-Amz-Signature=[HIDDEN]&sig=[HIDDEN]&format=CSV");
}
