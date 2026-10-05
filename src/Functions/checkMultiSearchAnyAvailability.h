#pragma once

#include "config.h"

#include <limits>
#include <string_view>

#include <Core/Types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/// Volnitsky stores the matched needle index in one byte, so it cannot handle more than 255
/// needles. Above that, or when the setting forces it, the Aho-Corasick (daachorse) path runs.
/// Both `MultiSearchImpl` and the text index dispatch on this single predicate, so the index
/// never prunes granules for an argument that the function itself rejects.
inline bool multiSearchAnyNeedsAhoCorasick(size_t needles_count, bool force_daachorse)
{
    return force_daachorse || needles_count > std::numeric_limits<UInt8>::max();
}

inline void checkMultiSearchAnyAvailability(
    [[maybe_unused]] std::string_view function_name, [[maybe_unused]] size_t needles_count, [[maybe_unused]] bool force_daachorse)
{
#if !USE_AHO_CORASICK
    if (multiSearchAnyNeedsAhoCorasick(needles_count, force_daachorse))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Function {} requires Aho-Corasick support which is not available in this build "
            "(needed for more than 255 patterns, or when force_daachorse_for_multi_search is set). "
            "Either recompile with Aho-Corasick support enabled or reduce patterns to 255 or fewer.",
            function_name);
#endif
}

}
