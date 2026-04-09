#pragma once

#include <cstddef>

/// Typed index for accessing a setting field within a settings Data struct.
/// Stores the byte offset of the field, enabling type-safe access via operator[].
/// Replaces pointer-to-member for layout-independent, cache-friendly access.
/// Defined outside namespace DB so it can be included from any scope.
template <typename FieldType>
struct SettingIndex
{
    size_t offset;
};
