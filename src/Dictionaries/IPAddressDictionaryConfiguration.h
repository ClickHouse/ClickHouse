#pragma once

#include <Common/Exception.h>
#include <base/types.h>

#include <algorithm>
#include <array>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Throws if `value` is not an allowed size of the radix index, the value of the layout parameter `radix_prefix`.
/// Shared by AST conversion and XML configuration without depending on the dictionary implementation.
inline void checkIPTrieRadixPrefix(UInt64 value)
{
    static constexpr std::array<UInt64, 7> allowed_radix_bits = {8, 14, 16, 18, 20, 22, 24};

    if (std::find(allowed_radix_bits.begin(), allowed_radix_bits.end(), value) == allowed_radix_bits.end())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Parameter 'radix_prefix' of a dictionary of layout 'ip_trie' must be one of 8, 14, 16, 18, 20, 22, 24, got {}",
            value);
}

}
