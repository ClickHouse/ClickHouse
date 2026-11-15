#include <gtest/gtest.h>

#include <Client/sanitizeUntrustedServerString.h>
#include <Core/ProtocolDefines.h>

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

TEST(SanitizeUntrustedServerString, ReplacesUtf8EncodedC1Controls)
{
    /// C1 controls (U+0080..U+009F) carry 8-bit equivalents of ESC-prefixed
    /// terminal introducers — U+009D is OSC, U+009C is ST. A hostile server can
    /// reach them as their UTF-8 encoding `0xC2 0x80..0x9F`, bypassing a sanitizer
    /// that only strips ESC (`0x1B`).
    String s;
    s.append("\xC2\x9D" "0;PWNED\xC2\x9C");      // 8-bit OSC ... ST
    s.append("payload");
    s.append("\xC2\x9B" "31m**INJECTED**\xC2\x9B" "0m");  // 8-bit CSI sequences

    const size_t original_size = s.size();
    sanitizeUntrustedServerString(s);

    /// Length must not change — each 2-byte C1 encoding becomes "??".
    EXPECT_EQ(s.size(), original_size);
    /// All `0xC2` bytes that were paired with C1 must be gone.
    EXPECT_EQ(s.find('\xC2'), std::string::npos);
    /// Plain ASCII surrounding the payload survives.
    EXPECT_NE(s.find("PWNED"), std::string::npos);
    EXPECT_NE(s.find("payload"), std::string::npos);
    EXPECT_NE(s.find("**INJECTED**"), std::string::npos);
}

TEST(SanitizeUntrustedServerString, PreservesUtf8WhoseContinuationByteIsInC1Range)
{
    /// Many valid UTF-8 sequences have a continuation byte in [0x80, 0x9F] —
    /// stripping that range blindly would mangle them. The Cyrillic letter "П"
    /// (U+041F) is encoded as `0xD0 0x9F`. It must survive.
    const String original = "Привет";
    String s = original;
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s, original);
}

TEST(SanitizeUntrustedServerString, PreservesUtf8WhoseLeadByteIs0xC2)
{
    /// `0xC2` is also the lead byte for legitimate Latin-1 supplement characters
    /// in the U+00A0..U+00BF range (encoded `0xC2 0xA0..0xBF`). Those must
    /// survive — only continuation bytes in [0x80, 0x9F] (the C1 range) trigger
    /// replacement.
    const String original = "non-breaking\xC2\xA0space \xC2\xBF\xC2\xA1";
    String s = original;
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s, original);
}

TEST(SanitizeUntrustedServerString, HandlesTrailing0xC2)
{
    /// A stray `0xC2` at the very end (no continuation byte) is invalid UTF-8.
    /// The sanitizer must not read past the end of the buffer; the byte is left
    /// alone (it will render as U+FFFD).
    String s;
    s.push_back('\xC2');
    sanitizeUntrustedServerString(s);
    EXPECT_EQ(s.size(), 1u);
    EXPECT_EQ(static_cast<unsigned char>(s[0]), 0xC2u);
}

TEST(SanitizeUntrustedServerString, MaxStringSizeCapIsTight)
{
    /// 4 KiB is well above any legitimate server name / time zone / display name
    /// (hostnames cap at HOST_NAME_MAX = 255 on Linux; "Europe/Madrid" is 13
    /// bytes). Bumping the cap back to `readStringBinary`'s 1 GiB default would
    /// re-open the unbounded-allocation vector the helper exists to close.
    EXPECT_LE(DBMS_MAX_HELLO_STRING_SIZE, 64u * 1024u);
    EXPECT_GE(DBMS_MAX_HELLO_STRING_SIZE, 256u);
}

TEST(SanitizeUntrustedServerString, MaxPasswordComplexityRulesCapIsTight)
{
    /// `rules_size` from the server feeds directly into `reserve`; without a cap
    /// a hostile server forces an arbitrarily large allocation. Real configurations
    /// declare a handful of rules at most. The constant is shared between server
    /// (TCPHandler::sendHello) and client (Connection::receiveHello).
    EXPECT_LE(DBMS_MAX_PASSWORD_COMPLEXITY_RULES, 4096u);
    EXPECT_GE(DBMS_MAX_PASSWORD_COMPLEXITY_RULES, 16u);
}
