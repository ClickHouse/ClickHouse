#pragma once

#include <string_view>
#include <algorithm>

namespace DB
{

/// Fast check if string looks like base64 (avoids expensive decode attempt for binary data)
/// This is a heuristic check that examines the first few bytes to determine if data
/// is likely base64-encoded or raw binary. It's optimized to avoid the expensive
/// exception handling cost of attempting base64 decode on binary data.
inline bool looksLikeBase64(std::string_view data)
{
    if (data.empty())
        return false;

    /// Base64 strings typically have length divisible by 4 (with padding)
    /// This is a quick heuristic, not a strict requirement
    if (data.size() < 4)
        return false;

    /// Check first few characters for base64 charset: A-Z, a-z, 0-9, +, /, =
    /// If we see binary bytes early, it's definitely not base64
    /// Checking 16 bytes gives > 99.9999% accuracy with minimal overhead
    size_t check_len = std::min(data.size(), size_t(16));
    for (size_t i = 0; i < check_len; ++i)
    {
        unsigned char c = data[i];
        bool is_base64_char = (c >= 'A' && c <= 'Z') ||
                              (c >= 'a' && c <= 'z') ||
                              (c >= '0' && c <= '9') ||
                              c == '+' || c == '/' || c == '=';
        if (!is_base64_char)
            return false;
    }
    return true;
}

}
