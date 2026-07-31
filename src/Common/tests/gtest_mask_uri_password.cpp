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

/// The regular expressions `maskConnectionStringKey` replaced, one per key it is used with.
bool maskConnectionStringKeyWithRE2(std::string & str, const std::string & key_with_eq)
{
    const re2::RE2 pattern(key_with_eq + ".*?(;|$)");
    return RE2::Replace(&str, pattern, key_with_eq + "[HIDDEN]\\1");
}

/// The regular expression `maskURIUserInfo` replaced.
bool maskURIUserInfoWithRE2(std::string & url)
{
    static const re2::RE2 pattern = "^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@";
    return RE2::Replace(&url, pattern, "\\1[HIDDEN]@");
}

/// The regular expression `maskURIPresignedCredentials` replaced.
bool maskURIPresignedCredentialsWithRE2(std::string & url)
{
    static const re2::RE2 pattern
        = "([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|X-Amz-[A-Za-z0-9\\-]*|X-Goog-[A-Za-z0-9\\-]*)=)[^&#]*";
    return RE2::GlobalReplace(&url, pattern, "\\1[HIDDEN]") > 0;
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


TEST(MaskURIPassword, AgreesWithTheRegularExpressionOnRandomStrings)
{
    /// The alphabet is the one the expression is sensitive to: the separator characters, something
    /// to put between them, and a newline, which `[^:]` matches but `.` does not.
    static constexpr std::string_view ALPHABET = "ab:@/.\n";

    std::mt19937 random(0); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    std::uniform_int_distribution<size_t> length(0, 24);
    std::uniform_int_distribution<size_t> character(0, ALPHABET.length() - 1);

    for (size_t iteration = 0; iteration < 200000; ++iteration)
    {
        std::string input(length(random), ' ');
        for (char & c : input)
            c = ALPHABET[character(random)];

        std::string with_scan = input;
        std::string with_re2 = input;

        EXPECT_EQ(maskURIPassword(&with_scan), maskURIPasswordWithRE2(&with_re2)) << "return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "result differs for: " << input;
    }
}

TEST(MaskConnectionStringKey, MasksTheValueUpToTheSeparator)
{
    std::string connection_string = "DefaultEndpointsProtocol=https;AccountKey=c2VjcmV0;AccountName=n";
    EXPECT_TRUE(maskConnectionStringKey(connection_string, "AccountKey="));
    EXPECT_EQ(connection_string, "DefaultEndpointsProtocol=https;AccountKey=[HIDDEN];AccountName=n");

    std::string at_the_end = "AccountName=n;SharedAccessSignature=sv=2021";
    EXPECT_TRUE(maskConnectionStringKey(at_the_end, "SharedAccessSignature="));
    EXPECT_EQ(at_the_end, "AccountName=n;SharedAccessSignature=[HIDDEN]");

    std::string without_the_key = "AccountName=n";
    EXPECT_FALSE(maskConnectionStringKey(without_the_key, "AccountKey="));
    EXPECT_EQ(without_the_key, "AccountName=n");
}

TEST(MaskConnectionStringKey, MasksAtLeastAsMuchAsTheRegularExpression)
{
    /// `;` and `=` delimit the connection string, `\n` is where the scan and the expression differ:
    /// `.*?` cannot cross a newline, so the expression left a value containing one exposed. The scan
    /// masks it, which is why the two are compared for "masks at least as much" rather than for
    /// equality.
    static constexpr std::string_view ALPHABET = "ab;=\n";
    static constexpr std::string_view KEY = "AccountKey=";

    std::mt19937 random(0); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    std::uniform_int_distribution<size_t> length(0, 16);
    std::uniform_int_distribution<size_t> character(0, ALPHABET.length() - 1);

    for (size_t iteration = 0; iteration < 200000; ++iteration)
    {
        std::string input{KEY};
        input.resize(KEY.length() + length(random), ' ');
        for (size_t i = KEY.length(); i < input.length(); ++i)
            input[i] = ALPHABET[character(random)];

        std::string with_scan = input;
        std::string with_re2 = input;

        EXPECT_TRUE(maskConnectionStringKey(with_scan, KEY)) << "the key is always present in: " << input;
        maskConnectionStringKeyWithRE2(with_re2, std::string{KEY});

        std::string_view value{input};
        value.remove_prefix(KEY.length());
        value = value.substr(0, value.find(';'));

        if (value.find('\n') == std::string::npos)
        {
            EXPECT_EQ(with_scan, with_re2) << "result differs for: " << input;
        }
        else
        {
            /// A value containing a newline is what the expression used to miss: `.*?` cannot cross
            /// one, so it did not match and left the secret in place. The scan masks it.
            EXPECT_EQ(with_scan, std::string{KEY} + "[HIDDEN]" + input.substr(KEY.length() + value.length()))
                << "the value was not masked in: " << input;
            EXPECT_EQ(with_re2, input) << "re2 was expected to leave this alone: " << input;
        }
    }
}

TEST(MaskURIUserInfo, MasksTheUserInfo)
{
    std::string url = "https://AKIA123:se+cret@bucket.s3.amazonaws.com/key";
    EXPECT_TRUE(maskURIUserInfo(url));
    EXPECT_EQ(url, "https://[HIDDEN]@bucket.s3.amazonaws.com/key");

    /// `[^/?#]+` cannot cross a slash, so a slash in the password hides the at sign from the
    /// expression, and nothing is masked (the regular expression it replaced behaved the same).
    std::string slash_in_password = "https://user:se/cret@host/key";
    EXPECT_FALSE(maskURIUserInfo(slash_in_password));

    /// The greedy `[^/?#]+` runs to the last at sign before the path.
    std::string with_at_in_password = "s3://user:p@ss@host/bucket";
    EXPECT_TRUE(maskURIUserInfo(with_at_in_password));
    EXPECT_EQ(with_at_in_password, "s3://[HIDDEN]@host/bucket");

    std::string without_userinfo = "https://bucket.s3.amazonaws.com/key";
    EXPECT_FALSE(maskURIUserInfo(without_userinfo));
    EXPECT_EQ(without_userinfo, "https://bucket.s3.amazonaws.com/key");

    /// An at sign after the authority does not make a match.
    std::string at_in_path = "https://host/name@example.com";
    EXPECT_FALSE(maskURIUserInfo(at_in_path));
}

TEST(MaskURIUserInfo, AgreesWithTheRegularExpressionOnRandomStrings)
{
    /// Every character class the expression is sensitive to: scheme characters, the separator,
    /// the authority terminators, the at sign, and a newline (which `[^/?#]` matches).
    static const std::vector<std::string_view> tokens
        = {"http", "://", "@", "/", "?", "#", ":", "a", "1", "+", ".", "-", "_", "\n"};

    std::mt19937 random(0); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    std::uniform_int_distribution<size_t> count(0, 8);
    std::uniform_int_distribution<size_t> token(0, tokens.size() - 1);

    for (size_t iteration = 0; iteration < 200000; ++iteration)
    {
        std::string input;
        for (size_t i = 0, n = count(random); i < n; ++i)
            input += tokens[token(random)];

        std::string with_scan = input;
        std::string with_re2 = input;

        EXPECT_EQ(maskURIUserInfo(with_scan), maskURIUserInfoWithRE2(with_re2)) << "return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "result differs for: " << input;
    }
}

TEST(MaskURIPresignedCredentials, MasksEveryCredentialParameter)
{
    std::string url = "https://b.s3.amazonaws.com/k?X-Amz-Credential=AKIA%2F123&X-Amz-Signature=abc&X-Amz-Expires=3600&other=1#frag";
    EXPECT_TRUE(maskURIPresignedCredentials(url));
    EXPECT_EQ(url, "https://b.s3.amazonaws.com/k?X-Amz-Credential=[HIDDEN]&X-Amz-Signature=[HIDDEN]&X-Amz-Expires=[HIDDEN]&other=1#frag");

    std::string v2 = "https://b.s3.amazonaws.com/k?AWSAccessKeyId=AKIA&Signature=s%3D&Expires=1";
    EXPECT_TRUE(maskURIPresignedCredentials(v2));
    EXPECT_EQ(v2, "https://b.s3.amazonaws.com/k?AWSAccessKeyId=[HIDDEN]&Signature=[HIDDEN]&Expires=[HIDDEN]");

    /// The key must match whole: `SignatureVersion` is not `Signature`.
    std::string other_key = "https://host/?SignatureVersion=4&x=y";
    EXPECT_FALSE(maskURIPresignedCredentials(other_key));
    EXPECT_EQ(other_key, "https://host/?SignatureVersion=4&x=y");
}

TEST(MaskURIPresignedCredentials, AgreesWithTheRegularExpressionOnRandomStrings)
{
    /// Key names and their fragments, the parameter separators, and the characters the value
    /// class is sensitive to. Fragments of key names exercise the whole-key requirement and the
    /// `X-Amz-`/`X-Goog-` prefix forms.
    static const std::vector<std::string_view> tokens
        = {"?", "&", "#", "=", "AWSAccessKeyId", "Signature", "Expires", "GoogleAccessId",
           "X-Amz-", "X-Goog-", "Sig", "a", "1", "-", "_", "\n"};

    std::mt19937 random(0); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    std::uniform_int_distribution<size_t> count(0, 8);
    std::uniform_int_distribution<size_t> token(0, tokens.size() - 1);

    for (size_t iteration = 0; iteration < 200000; ++iteration)
    {
        std::string input;
        for (size_t i = 0, n = count(random); i < n; ++i)
            input += tokens[token(random)];

        std::string with_scan = input;
        std::string with_re2 = input;

        EXPECT_EQ(maskURIPresignedCredentials(with_scan), maskURIPresignedCredentialsWithRE2(with_re2))
            << "return value differs for: " << input;
        EXPECT_EQ(with_scan, with_re2) << "result differs for: " << input;
    }
}
