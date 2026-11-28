#pragma once

#include <base/types.h>

#include <span>

namespace DB
{

/// Renames each repeated column name to `name_1`, `name_2`, ..., keeping the first occurrence unchanged.
/// A suffix that would produce a name present in `names` is skipped, so names that are unique are never
/// changed: `a,a,a_1` becomes `a,a_2,a_1`. Used for input formats whose data may repeat a column name.
void makeColumnNamesUnique(std::span<String> names);

}
