#pragma once

#include <Core/Types.h>

namespace DB
{

/// Returns the length of a text looking like
/// -- Tags: x, y, z
/// -- Tag x: explanation of tag x
/// -- Tag y: explanation of tag y
/// -- Tag z: explanation of tag z
///
/// at the beginning of a multiline query.
/// If there are no test tags in the multiline query the function returns 0.
size_t getTestTagsLength(const String & multiline_query);

}
