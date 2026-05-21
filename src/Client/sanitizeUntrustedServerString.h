#pragma once

#include <cstddef>
#include <base/types.h>
#include <Common/StringUtils.h>


namespace DB
{

/// Cap on server-supplied display strings read during the client's Hello packet
/// (server name, time zone, display name, password-rule patterns/messages) and
/// post-handshake updates of the same fields. Legitimate values are short; a
/// high cap is purely an attack surface (`readStringBinary` defaults to 1 GiB,
/// which is a denial-of-service vector against the connecting client).
constexpr size_t MAX_SERVER_HELLO_STRING_SIZE = 4096;

/// Replace ASCII C0 control characters (0x00-0x1F), DEL (0x7F), and the UTF-8
/// encoding of C1 controls (U+0080-U+009F, encoded as `0xC2 0x80..0x9F`) with '?'
/// so a hostile server cannot inject terminal escape sequences via fields that the
/// client prints verbatim. U+009B / U+009D / U+009C are the 8-bit equivalents of
/// `ESC [` / `ESC ]` / `ESC \\` and some terminals act on them. Other UTF-8 high
/// bytes are preserved so non-ASCII display names render normally.
inline void sanitizeUntrustedServerString(String & s)
{
    for (size_t i = 0; i < s.size(); ++i)
    {
        const auto uc = static_cast<unsigned char>(s[i]);

        /// `isControlASCII` covers 0x00-0x1F only; DEL (0x7F) is not in that range
        /// but we strip it too — older terminals interpret it as backspace.
        if (isControlASCII(s[i]) || uc == 0x7F)
        {
            s[i] = '?';
            continue;
        }

        /// Catch the two-byte UTF-8 encoding of C1 controls: `0xC2 0x80..0x9F`.
        /// Stripping raw `0x80..0x9F` would mangle valid UTF-8 continuation bytes
        /// (e.g. `0x9F` in "П"), so we match the encoding pair instead.
        if (uc == 0xC2 && i + 1 < s.size())
        {
            const auto next = static_cast<unsigned char>(s[i + 1]);
            if (next >= 0x80 && next <= 0x9F)
            {
                s[i] = '?';
                s[i + 1] = '?';
                ++i;
            }
        }
    }
}

}
