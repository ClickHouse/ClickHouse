#pragma once

#include <base/types.h>

namespace DB
{

/// A normal projection exposes the parent's `_part_offset` to its SELECT, but stores that
/// output as `_parent_part_offset`. Features which describe SELECT outputs while operating on
/// physical projection metadata must cross that name boundary explicitly.
inline String getProjectionStorageColumnName(const String & select_column_name, bool with_parent_part_offset)
{
    if (with_parent_part_offset && select_column_name == "_part_offset")
        return "_parent_part_offset";
    return select_column_name;
}

inline String getProjectionSelectColumnName(const String & storage_column_name, bool with_parent_part_offset)
{
    if (with_parent_part_offset && storage_column_name == "_parent_part_offset")
        return "_part_offset";
    return storage_column_name;
}

}
