#include <gtest/gtest.h>

#include <Client/sanitizeUntrustedServerString.h>

using namespace DB;

/// `sanitizeUntrustedServerString` strips every byte that can drive a terminal
/// escape sequence (OSC 0 window-title set, OSC 52 clipboard injection, OSC 8
/// hyperlink fakery, SGR colour, ...) from server-supplied Hello fields that the
/// client prints verbatim — `server_name`, `server_display_name`, etc.

TEST(SanitizeUntrustedServerString, PreservesPrintableAscii)
{
    String s = "ClickHouse server v25.10";
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s, "ClickHouse server v25.10");
}

TEST(SanitizeUntrustedServerString, PreservesUtf8HighBytes)
{
    /// Non-ASCII display names (e.g. "Привет", "clïckhöuse") must survive — they
    /// cannot drive a terminal sequence on their own.
    const String original = "Привет clïckhöuse 你好";
    String s = original;
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s, original);
}

TEST(SanitizeUntrustedServerString, ReplacesEscapeAndControlBytes)
{
    /// Representative payload: OSC 0 window-title set plus an SGR colour run.
    String s;
    s.append("\x1b]0;PWNED\x07");
    s.append("\x1b[31m**INJECTED**\x1b[0m");
    sanitizeUntrustedServerString(s);

    EXPECT_EQ(s.find('\x1b'), std::string::npos);
    EXPECT_EQ(s.find('\x07'), std::string::npos);
    EXPECT_NE(s.find("PWNED"), std::string::npos);
    EXPECT_NE(s.find("**INJECTED**"), std::string::npos);
}

TEST(SanitizeUntrustedServerString, ReplacesAllAsciiControlChars)
{
    /// Every byte in [0x00, 0x1F] plus 0x7F (DEL) must be rewritten to '?'.
    String s;
    for (int i = 0; i < 0x20; ++i)
        s.push_back(static_cast<char>(i));
    s.push_back('\x7F');

    const size_t expected_replacements = s.size();
    sanitizeUntrustedServerString(s);

    EXPECT_EQ(s.size(), expected_replacements);
    for (char c : s)
        EXPECT_EQ(c, '?');
}

TEST(SanitizeUntrustedServerString, PreservesLengthAndPositions)
{
    /// The helper replaces in place, byte-for-byte. Length and the position of
    /// surviving bytes must be unchanged; consumers that rely on the string
    /// length (prompt rendering, regex compilation for password rules) keep
    /// working.
    String s = "a\x1b" "b\x07" "c";
    EXPECT_EQ(s.size(), 5u);
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s.size(), 5u);
    EXPECT_EQ(s, "a?b?c");
}

TEST(SanitizeUntrustedServerString, EmptyString)
{
    String s;
    sanitizeUntrustedServerString(s);
    EXPECT_TRUE(s.empty());
}

TEST(SanitizeUntrustedServerString, MaxStringSizeCapIsTight)
{
    /// 4 KiB is well above any legitimate server name / time zone / display name
    /// (hostnames cap at HOST_NAME_MAX = 255 on Linux; "Europe/Madrid" is 13
    /// bytes). Bumping the cap back to `readStringBinary`'s 1 GiB default would
    /// re-open the unbounded-allocation vector the helper exists to close.
    EXPECT_LE(MAX_SERVER_HELLO_STRING_SIZE, 64u * 1024u);
    EXPECT_GE(MAX_SERVER_HELLO_STRING_SIZE, 256u);
}

TEST(SanitizeUntrustedServerString, MaxPasswordComplexityRulesCapIsTight)
{
    /// `rules_size` from the server feeds directly into `reserve`; without a cap
    /// a hostile server forces an arbitrarily large allocation. Real configurations
    /// declare a handful of rules at most.
    EXPECT_LE(MAX_PASSWORD_COMPLEXITY_RULES, 4096u);
    EXPECT_GE(MAX_PASSWORD_COMPLEXITY_RULES, 16u);
}
