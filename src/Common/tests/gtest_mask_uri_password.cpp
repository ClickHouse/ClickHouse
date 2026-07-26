#include <gtest/gtest.h>

#include <Common/maskURIPassword.h>
#include <Common/re2.h>

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
