#pragma once

#include <Common/SipHash.h>

#include <string_view>
#include <type_traits>

namespace DB
{

/// Folds one semantic field of an AST node into its tree hash. A string is prefixed with its
/// length: `SipHash::update` of a string appends only its bytes, so without the prefix adjacent
/// strings could exchange characters and still hash the same (e.g. the names `ab, c` and `a, bc`
/// of `CREATE SETTINGS PROFILE`). The rewrite-rule matcher treats an equal tree hash as semantic
/// equality, so every string folded by an `updateTreeHashImpl` override must keep its boundaries.
/// Any other field is folded unchanged.
template <typename T>
void updateTreeHashField(SipHash & hash_state, const T & value)
{
    if constexpr (std::is_convertible_v<const T &, std::string_view>)
    {
        const std::string_view str = value;
        hash_state.update(str.size());
        hash_state.update(str);
    }
    else
    {
        hash_state.update(value);
    }
}

}
