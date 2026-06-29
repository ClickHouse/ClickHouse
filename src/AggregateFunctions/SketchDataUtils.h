#pragma once

#include <string_view>
#include <string>
#include <algorithm>
#include <Common/Base64.h>

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

/// Decode base64 data if it looks like base64, otherwise return raw data
/// Returns a pair of (data_ptr, data_size) and optionally fills decoded_storage
/// Used by both aggregate functions and scalar functions for sketch deserialization
///
/// @param serialized_data The input data (may be base64 or raw binary)
/// @param decoded_storage Storage for decoded data (if base64 decoding is performed)
/// @param base64_encoded If true, data may be base64 encoded and should be checked/decoded
inline std::pair<const uint8_t*, size_t> decodeSketchData(
    std::string_view serialized_data,
    std::string& decoded_storage,
    bool base64_encoded = false)
{
    if (serialized_data.empty())
        return {nullptr, 0};

    /// Most ClickHouse-generated sketches are raw binary; skip base64 detection by default.
    if (!base64_encoded)
    {
        return {
            reinterpret_cast<const uint8_t*>(serialized_data.data()),
            serialized_data.size()
        };
    }

    /// Fast check: only attempt base64 decode if data looks like base64
    /// This avoids expensive exception handling for raw binary data (the common case)
    if (looksLikeBase64(serialized_data))
    {
        try
        {
            decoded_storage = base64Decode(std::string(serialized_data));
            return {
                reinterpret_cast<const uint8_t*>(decoded_storage.data()),
                decoded_storage.size()
            };
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Looked like base64 but wasn't valid, use raw data
        }
    }

    /// Doesn't look like base64, or decode failed - use raw data directly
    return {
        reinterpret_cast<const uint8_t*>(serialized_data.data()),
        serialized_data.size()
    };
}

}
