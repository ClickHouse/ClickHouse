#pragma once

#include <cstddef>
#include <base/types.h>
#include <Common/StringUtils.h>


namespace DB
{

/// Cap on server-supplied display strings read during the client's Hello packet
/// (server name, time zone, display name, password-rule patterns/messages).
/// Legitimate values are short; a high cap is purely an attack surface
/// (`readStringBinary` defaults to 1 GiB, which is a denial-of-service vector).
constexpr size_t MAX_SERVER_HELLO_STRING_SIZE = 4096;

/// Replace ASCII control characters (0x00-0x1F and 0x7F DEL) with '?' so a hostile
/// server cannot inject terminal escape sequences via fields that the client prints
/// verbatim. UTF-8 high bytes (>= 0x80) are preserved so non-ASCII display names
/// render normally.
inline void sanitizeUntrustedServerString(String & s)
{
    for (auto & c : s)
    {
        /// `isControlASCII` covers 0x00-0x1F only; DEL (0x7F) is not in that range
        /// but we strip it too — older terminals interpret it as backspace.
        if (isControlASCII(c) || static_cast<unsigned char>(c) == 0x7F)
            c = '?';
    }
}

}
